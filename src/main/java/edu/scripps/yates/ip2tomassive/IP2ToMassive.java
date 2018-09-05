package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import edu.scripps.yates.dtaselectparser.DTASelectParser;
import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.ftp.FTPUtils;
import edu.scripps.yates.utilities.progresscounter.ProgressCounter;
import edu.scripps.yates.utilities.progresscounter.ProgressPrintingType;
import edu.scripps.yates.utilities.properties.PropertiesUtil;
import gnu.trove.set.hash.TIntHashSet;

public class IP2ToMassive {
	private final static Logger log = Logger.getLogger(IP2ToMassive.class);
	private final String projectName;
	protected final MySftpProgressMonitor progressMonitor;
	private final File propertiesFile;

	public final static String IP2_SERVER_PROJECT_BASE_PATH = "ip2_server_project_base_path";
	public final static String PROJECT_NAME = "project_name";
	public final static String EXPERIMENT_IDS = "experiment_ids";

	public IP2ToMassive(MySftpProgressMonitor progressMonitor, File propertiesFile) {

		this.progressMonitor = progressMonitor;
		this.propertiesFile = propertiesFile;
		this.projectName = getProperties(this.propertiesFile).getProperty(PROJECT_NAME);
		getProperties(this.propertiesFile).getProperty(IP2_SERVER_PROJECT_BASE_PATH);
	}

	public void transferFiles(List<String> pathes) {
		log.info(pathes.size() + " files to transfer");
		ProgressCounter counter = new ProgressCounter(pathes.size(), ProgressPrintingType.EVERY_STEP, 0, true);
		for (String string : pathes) {
			counter.increment();
			final String printIfNecessary = counter.printIfNecessary();
			if (!"".equals(printIfNecessary)) {
				log.info(printIfNecessary + " files transferred");
			}
			transferFile(string);
		}
	}

	public void transferFile(String fullPathInIP2) {
		OutputStream outputStream = null;
		FTPClient ftpMassive = null;
		Session sftpIP2 = null;
		try {
			sftpIP2 = loginToIP2();
			ftpMassive = loginToMassive();
			String remotePathInMassive = projectName + "/raw";
			log.info("Transferring file " + fullPathInIP2);

			FTPUtils.makeDirectories(ftpMassive, remotePathInMassive, System.out);

			String fileName = FilenameUtils.getName(fullPathInIP2);
			progressMonitor.setSuffix("(" + fileName + ") ");
			String fullPathInMassive = "/" + remotePathInMassive + "/" + fileName;
			long size = FTPUtils.getSize(ftpMassive, fullPathInMassive);
			if (size != 0) {
				log.info("File already found in MassIVE server with  " + FileUtils.getDescriptiveSizeFromBytes(size)
						+ ". It will be skipped");
				return;
			}
			outputStream = ftpMassive.storeFileStream(fullPathInMassive);
			ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sftpIP2);
			sftpChannel.get(fullPathInIP2, outputStream);
			sftpChannel.exit();
			log.info("Transfer done.");
		} catch (IOException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} catch (SftpException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} catch (JSchException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			try {
				if (outputStream != null) {
					outputStream.close();
				}
				if (ftpMassive != null) {
					ftpMassive.logout();
					ftpMassive.disconnect();
				}
				if (sftpIP2 != null) {
					sftpIP2.disconnect();
				}
			} catch (IOException e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}
		}
	}

	protected Session loginToIP2() throws IOException {
		log.info("Login into server...");
		Properties properties = getProperties(this.propertiesFile);
		String hostName = properties.getProperty("ip2_server_url");
		String userName = properties.getProperty("ip2_server_user_name");
		String password = properties.getProperty("ip2_server_password");
		int port = Integer.valueOf(properties.getProperty("ip2_server_connection_port"));
		try {
			Session ftpClient = FTPUtils.loginSSHClient(hostName, userName, password, port);
			return ftpClient;
		} catch (JSchException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Cannot connect to IP2: " + e.getMessage());
		}

	}

	protected static Properties getProperties(File propertiesFile) {
		try {
			Properties properties = PropertiesUtil.getProperties(propertiesFile);
			return properties;
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
			throw new IllegalArgumentException("Error reading properties file: " + propertiesFile.getAbsolutePath());
		}
	}

	protected FTPClient loginToMassive() throws IOException {

		Properties properties = getProperties(this.propertiesFile);
		String hostName = properties.getProperty("massive_server_url");
		String userName = properties.getProperty("massive_server_user_name");
		String password = properties.getProperty("massive_server_password");
		return FTPUtils.loginFTPClient(hostName, userName, password);

	}

	public List<String> getExperimentPathsFromIP2(String projectBasePath, TIntHashSet experimentIDs)
			throws IOException, JSchException, SftpException {
		List<String> ret = new ArrayList<String>();
		Session sftpIP2 = null;
		ChannelSftp sftpChannel = null;
		try {
			sftpIP2 = loginToIP2();
			sftpChannel = FTPUtils.openSFTPChannel(sftpIP2);
			Vector<LsEntry> ls = sftpChannel.ls(projectBasePath);
			for (LsEntry lsEntry : ls) {
				String fileName = lsEntry.getFilename();
				if (lsEntry.getAttrs().isDir() && fileName.contains("_")) {
					try {
						int experimentID = Integer.valueOf(fileName.substring(fileName.lastIndexOf("_") + 1));
						if (experimentIDs.contains(experimentID)) {
							ret.add(projectBasePath + "/" + lsEntry.getFilename());
						}
					} catch (NumberFormatException e) {

					}
				}
			}
		} finally {
			sftpChannel.disconnect();
			sftpIP2.disconnect();
		}
		return ret;
	}

	public List<String> getRawFilesPaths(String experimentPath) throws IOException, JSchException, SftpException {
		List<String> ret = new ArrayList<String>();
		Session sftpIP2 = null;
		ChannelSftp sftpChannel = null;
		try {
			sftpIP2 = loginToIP2();
			sftpChannel = FTPUtils.openSFTPChannel(sftpIP2);
			String spectraPath = experimentPath + "/spectra";
			Vector<LsEntry> ls = sftpChannel.ls(spectraPath);
			for (LsEntry lsEntry : ls) {
				String fileName = lsEntry.getFilename();
				if (!lsEntry.getAttrs().isDir() && fileName.endsWith("raw")) {
					ret.add(spectraPath + "/" + lsEntry.getFilename());
				}
			}
		} finally {
			sftpChannel.disconnect();
			sftpIP2.disconnect();
		}
		return ret;
	}

	public List<Search> getDTASelectPathsAndParameters(String experimentPath)
			throws IOException, JSchException, SftpException {
		List<Search> ret = new ArrayList<Search>();
		Session sftpIP2 = null;
		ChannelSftp sftpChannel = null;
		try {
			sftpIP2 = loginToIP2();
			sftpChannel = FTPUtils.openSFTPChannel(sftpIP2);
			String searchPath = experimentPath + "/search";
			Vector<LsEntry> ls = sftpChannel.ls(searchPath);
			for (LsEntry lsEntry : ls) {
				String fileName = lsEntry.getFilename();
				if (lsEntry.getAttrs().isDir() && fileName.contains("_")) {
					Vector<LsEntry> ls2 = sftpChannel.ls(searchPath + "/" + lsEntry.getFilename());
					for (LsEntry lsEntry2 : ls2) {
						if (lsEntry2.getFilename().equals("DTASelect-filter.txt")) {
							String parameters = readDTASelectParameters(lsEntry2, sftpIP2,
									searchPath + "/" + lsEntry.getFilename());
							int id = Integer.valueOf(fileName.substring(fileName.lastIndexOf("_") + 1));
							Search pair = new Search(id,
									searchPath + "/" + lsEntry.getFilename() + "/" + lsEntry2.getFilename(),
									parameters);
							ret.add(pair);
						}
					}
				}
			}
		} finally {
			sftpChannel.disconnect();
			sftpIP2.disconnect();
		}
		return ret;
	}

	private String readDTASelectParameters(LsEntry lsEntry2, Session remoteServerSession, String parentFolder)
			throws JSchException, SftpException, IOException {
		File tempFile = File.createTempFile("todelete", "");
		tempFile.deleteOnExit();
		FileOutputStream outputStream = new FileOutputStream(tempFile);

		long download = FTPUtils.download(remoteServerSession, parentFolder + "/" + lsEntry2.getFilename(),
				outputStream, progressMonitor);
		if (download > 0) {
			DTASelectParser parser = new DTASelectParser(tempFile);
			parser.setOnlyReadParameters(true);
			return parser.getCommandLineParameter().toString();
		}
		return projectName;
	}

}
