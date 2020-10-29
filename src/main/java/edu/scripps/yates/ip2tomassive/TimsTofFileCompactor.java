package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.files.TarZipUtils;
import edu.scripps.yates.utilities.ftp.FTPUtils;
import edu.scripps.yates.utilities.properties.PropertiesUtil;

/**
 * TIMsTOF raw files are folders. We need to compress them before uploading to
 * MassIVE. Also, we need to find the TIMsTOF folders looking into a base folder
 * and looking for the names from the ms2 files in the previously created
 * ip2FileTransfer
 * 
 * @author salvador
 *
 */
public class TimsTofFileCompactor {
	private final Logger log = Logger.getLogger(TimsTofFileCompactor.class);
	private final String[] basePaths;
	private final File ip2FileTransfer;
	private final File connectionProperties;
	private final String projectName;
	private final MySftpProgressMonitor progressMonitor;

	public TimsTofFileCompactor(String[] basePaths, File ip2FileTransfer, File connectionProperties, String projectName)
			throws IOException {
		this.basePaths = basePaths;
		this.ip2FileTransfer = ip2FileTransfer;
		this.connectionProperties = connectionProperties;
		this.projectName = projectName;
		progressMonitor = new MySftpProgressMonitor(System.out);
	}

	public static void main(String[] args) {
		final String[] basePaths = args[0].split(",");
		final File ip2FileTransfer = new File(args[1]);
		final File connectionProperties = new File(args[2]);
		final String projectName = args[3];
		TimsTofFileCompactor fpf;
		try {
			fpf = new TimsTofFileCompactor(basePaths, ip2FileTransfer, connectionProperties, projectName);

			fpf.run();
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public void run() throws IOException {
		final FileWriter fw = new FileWriter(new File(ip2FileTransfer.getParent() + File.separator
				+ FileNameUtils.getBaseName(ip2FileTransfer.getAbsolutePath()) + "_with_TIMSTof_raw.txt"));
		final FileWriter fwMissing = new FileWriter(new File(ip2FileTransfer.getParent() + File.separator
				+ FileNameUtils.getBaseName(ip2FileTransfer.getAbsolutePath()) + "_missing_raw.txt"));
		final List<String> lines = Files.readAllLines(ip2FileTransfer.getAbsoluteFile().toPath());
		boolean ms2Lines = false;
		for (final String line : lines) {

			File compressedFolder = null;
			try {
				final FileType fileType = FileType.getbyDescription(line.trim());
				if (fileType != null) {
					if (fileType == FileType.MS2) {
						ms2Lines = true;
					} else {
						ms2Lines = false;
					}
				} else {
					if (ms2Lines) {
						// check if it is in the inclusion set
						final String path = line.split("\t")[0];
						String ms2FileName = FileNameUtils.getBaseName(path);
						// remove the "_nopd" suffix
						ms2FileName = ms2FileName.replace("_nopd", "");
						File timsTofFolder = null;
						for (final String basePath : basePaths) {
							timsTofFolder = searchTIMsTofFolder(basePath, ms2FileName);
							if (timsTofFolder != null) {
								break;
							}
						}
						if (timsTofFolder == null) {
							fwMissing.write(ms2FileName + "\n");
							fwMissing.flush();
							System.out.println(ms2FileName + " not found");
							continue;
						}
						compressedFolder = compressAndSendFolder(timsTofFolder);
					}
				}
			} finally {
				fw.write(line + "\n");
				if (compressedFolder != null) {
					fw.write(FileType.RAW.getDescription() + "\n" + compressedFolder.getAbsolutePath() + "\n");
				}
				fw.flush();
			}
		}

		fwMissing.close();
		fw.close();
	}

	protected FTPClient loginToMassive() throws IOException {

		final Properties properties = getProperties(connectionProperties);
		final String hostName = properties.getProperty("massive_server_url");
		final String userName = properties.getProperty("massive_server_user_name");
		final String password = properties.getProperty("massive_server_password");
		return FTPUtils.loginFTPClient(hostName, userName, password);

	}

	protected static Properties getProperties(File propertiesFile) {
		try {
			final Properties properties = PropertiesUtil.getProperties(propertiesFile);
			return properties;
		} catch (final Exception e) {
			e.printStackTrace();

			throw new IllegalArgumentException("Error reading properties file: " + propertiesFile.getAbsolutePath());
		}
	}

	private File compressAndSendFolder(File timsTofFolder) throws IOException {
//		final FTPClient ftpMassive = null;
		try {

//			ftpMassive = loginToMassive();
//			final String remotePathInMassive = projectName + "/" + projectName + "/RAW";
			final String fileName = FilenameUtils.getName(timsTofFolder.getAbsolutePath()) + ".tar.gz";
			log.info("Transferring file " + fileName);

//			FTPUtils.makeDirectories(ftpMassive, remotePathInMassive, System.out);

//			progressMonitor.setSuffix("(" + fileName + ") ");
//			final String fullPathInMassive = "/" + remotePathInMassive + "/" + fileName;
//			final long size = FTPUtils.getSize(ftpMassive, fullPathInMassive);
//			if (size != 0) {
//				log.info("File already found in MassIVE server with  " + FileUtils.getDescriptiveSizeFromBytes(size)
//						+ ". It will be override");
//
//			}
			final File outputFile = new File("Z:\\share\\Salva\\data\\cbamberg\\SARS_Cov2\\MassIVE submission\\"
					+ FilenameUtils.getName(timsTofFolder.getAbsolutePath()) + ".tar.gz");
			if (outputFile.exists() && outputFile.length() > 0l) {
				log.info("File already created. Skipping it");
				return outputFile;
			}
//			final OutputStream outputStream = ftpMassive.storeFileStream(fullPathInMassive);
			final OutputStream outputStream = new FileOutputStream(outputFile);
			final TarZipUtils compressor = new TarZipUtils(timsTofFolder, outputStream, false);
			compressor.setLogFileCopyProgress(true);
			log.info("Compressing folder " + timsTofFolder.getAbsolutePath());
			final long sent = compressor.transfer();
			outputStream.close();
			log.info("Folder compressed at (" + FileUtils.getDescriptiveSizeFromBytes(sent) + ")");
			return outputFile;
		} finally {
//			if (ftpMassive != null) {
//				ftpMassive.logout();
//				ftpMassive.disconnect();
//			}
		}
	}

	private File searchTIMsTofFolder(String basePath2, String ms2FileName) {
		final File[] listFiles = new File(basePath2).listFiles(new FileFilter() {

			@Override
			public boolean accept(File file) {
				if (file.isDirectory()) {
					return true;
				}
				return false;
			}
		});
		for (final File file : listFiles) {
			if (file.isDirectory()) {
				final String folderName = FilenameUtils.getName(file.getAbsolutePath());
				if (folderName.endsWith(".d")) {
					if (folderName.equals(ms2FileName + ".d")) {
						return file;
					}
				}
				final File ret = searchTIMsTofFolder(file.getAbsolutePath(), ms2FileName);
				if (ret != null) {
					return ret;
				}
			}
		}
		return null;
	}

}
