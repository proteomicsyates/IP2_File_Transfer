package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
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
import edu.scripps.yates.utilities.util.Pair;

/**
 * TIMsTOF raw files are folders. We need to compress them before uploading to
 * MassIVE. Here, we will provide a list of names of folders to be found in the
 * basePaths, and if found they will be compressed as tar.gz.<br>
 * Some files, such as xic files or ms2 files (stated in parameter 4 as
 * extensions) that are found inside of the .d folders, are excluded from the
 * compressed file, and they are passed to be referenced in the output file.
 * 
 * @author salvador
 *
 */
public class TimsTofFileCompactorSimple {
	private final Logger log = Logger.getLogger(TimsTofFileCompactorSimple.class);
	private final String[] basePaths;
	private final File fileWithRawFolderNames;
	private final File connectionProperties;
	private final MySftpProgressMonitor progressMonitor;
	private final List<String> extensionsOfFilesThatGoOutOfCompression;

	public TimsTofFileCompactorSimple(String[] basePaths, File fileWithRawFolderNames, File connectionProperties,
			List<String> extensionsOfFilesThatGoOutOfCompression) throws IOException {
		this.basePaths = basePaths;
		this.fileWithRawFolderNames = fileWithRawFolderNames;
		this.connectionProperties = connectionProperties;
		this.extensionsOfFilesThatGoOutOfCompression = extensionsOfFilesThatGoOutOfCompression;
		progressMonitor = new MySftpProgressMonitor(System.out);
	}

	public static void main(String[] args) {
		final String[] basePaths = args[0].split(",");
		final File fileWithRawFolderNames = new File(args[1].trim());
		final File connectionProperties = new File(args[2].trim());

		final String extensionsOfFilesThatGoOutOfCompressionRaw = args[3].trim();
		final List<String> extensionsOfFilesThatGoOutOfCompression = new ArrayList<String>();
		if (extensionsOfFilesThatGoOutOfCompressionRaw.contains(",")) {
			final String[] split = extensionsOfFilesThatGoOutOfCompressionRaw.split(",");
			for (final String string : split) {
				extensionsOfFilesThatGoOutOfCompression.add(string);
			}
		} else {
			extensionsOfFilesThatGoOutOfCompression.add(extensionsOfFilesThatGoOutOfCompressionRaw);
		}
		TimsTofFileCompactorSimple fpf;
		try {
			fpf = new TimsTofFileCompactorSimple(basePaths, fileWithRawFolderNames, connectionProperties,
					extensionsOfFilesThatGoOutOfCompression);

			fpf.run();
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public void run() throws IOException {
		final FileWriter fw = new FileWriter(new File(fileWithRawFolderNames.getParent() + File.separator
				+ FileNameUtils.getBaseName(fileWithRawFolderNames.getAbsolutePath()) + "_with_TIMSTof_raw.txt"));
		final FileWriter fwMissing = new FileWriter(new File(fileWithRawFolderNames.getParent() + File.separator
				+ FileNameUtils.getBaseName(fileWithRawFolderNames.getAbsolutePath()) + "_missing_raw.txt"));
		final List<String> rawFolderNames = Files.readAllLines(fileWithRawFolderNames.getAbsoluteFile().toPath());
		for (final String rawFolderName : rawFolderNames) {
			List<Pair<File, FileType>> entries = null;
			try {
				File timsTofFolder = null;
				for (final String basePath : basePaths) {
					timsTofFolder = searchTIMsTofFolder(basePath, rawFolderName);
					if (timsTofFolder != null) {
						break;
					}
				}
				if (timsTofFolder == null) {
					fwMissing.write(rawFolderName + "\n");
					fwMissing.flush();
					System.out.println(rawFolderName + " not found");
					continue;
				}
				entries = compressFolder(timsTofFolder, extensionsOfFilesThatGoOutOfCompression);

			} finally {
				if (entries != null) {
					for (final Pair<File, FileType> entry : entries) {
						final FileType fileType = entry.getSecondElement();
						final File file = entry.getFirstelement();
						fw.write(fileType.getDescription() + "\n" + file.getAbsolutePath() + "\n");
						fw.flush();
					}
				}
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

	private List<Pair<File, FileType>> compressFolder(File timsTofFolder,
			List<String> extensionsOfFilesThatGoOutOfCompression) throws IOException {
		final List<Pair<File, FileType>> ret = new ArrayList<Pair<File, FileType>>();

		final String fileName = FilenameUtils.getName(timsTofFolder.getAbsolutePath()) + ".tar.gz";
		log.info("Processing folder " + fileName);

		log.info("Looking for files with extensions to skip");
		final List<File> filesToSkip = lookForFilesToSkip(timsTofFolder, extensionsOfFilesThatGoOutOfCompression);
		final List<String> fileNamestoSkip = new ArrayList<String>();
		// now try to know their type
		for (final File file : filesToSkip) {
			fileNamestoSkip.add(FilenameUtils.getName(file.getAbsolutePath()));
			final String extension = FilenameUtils.getExtension(file.getAbsolutePath());
			if (extension.equalsIgnoreCase("xics")) {
				final Pair<File, FileType> pair = new Pair<File, FileType>(file, FileType.MS1);
				ret.add(pair);
			} else {
				for (final FileType fileType : FileType.values()) {
					if (extension.equalsIgnoreCase(fileType.getExtension())) {
						final Pair<File, FileType> pair = new Pair<File, FileType>(file, fileType);
						ret.add(pair);
						break;
					}
				}
			}
		}
		final File outputFile = new File(timsTofFolder.getParent() + File.separator
				+ FilenameUtils.getName(timsTofFolder.getAbsolutePath()) + ".tar.gz");
		if (outputFile.exists() && outputFile.length() > 0l) {
			log.info("File already created. Skipping it");
		} else {
			final OutputStream outputStream = new FileOutputStream(outputFile);
			final TarZipUtils compressor = new TarZipUtils(timsTofFolder, outputStream, false, fileNamestoSkip);
			compressor.setLogFileCopyProgress(true);
			log.info("Compressing folder " + timsTofFolder.getAbsolutePath());
			final long sent = compressor.transfer();
			outputStream.close();
			log.info("Folder compressed at (" + FileUtils.getDescriptiveSizeFromBytes(sent) + ")");

		}
		final Pair<File, FileType> pair = new Pair<File, FileType>(outputFile, FileType.RAW);
		ret.add(pair);
		return ret;

	}

	private List<File> lookForFilesToSkip(File timsTofFolder, List<String> extensionsOfFilesThatGoOutOfCompression2) {
		final List<File> ret = new ArrayList<File>();

		final File[] filesToSkip = timsTofFolder.listFiles(new FileFilter() {

			@Override
			public boolean accept(File file) {
				if (file.isFile()) {
					for (final String extension : extensionsOfFilesThatGoOutOfCompression2) {
						final String extension2 = FilenameUtils.getExtension(file.getAbsolutePath());
						if (extension2.equalsIgnoreCase(extension)) {
							return true;
						}
					}
				}
				return false;
			}
		});
		if (filesToSkip != null && filesToSkip.length > 0) {
			for (final File file : filesToSkip) {
				ret.add(file);
			}
		}
		// now look at directories
		final File[] directoriesToSearch = timsTofFolder.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				if (pathname.isDirectory()) {
					return true;
				}
				return false;
			}
		});

		for (final File directory : directoriesToSearch) {

			ret.addAll(lookForFilesToSkip(directory, extensionsOfFilesThatGoOutOfCompression2));

		}
		return ret;
	}

	private File searchTIMsTofFolder(String basePath2, String ms2FileName) {
		final File[] directories = new File(basePath2).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (dir.isDirectory() && name.contains(ms2FileName)) {
					return true;
				}
				return false;
			}
		});
		if (directories.length > 0) {
			return directories[0];
		}

		final File[] directoriesToSearch = new File(basePath2).listFiles(new FileFilter() {

			@Override
			public boolean accept(File file) {
				final String name = FilenameUtils.getName(file.getAbsolutePath());
				if (file.isDirectory() && !name.endsWith(".d")) {
					return true;
				}
				return false;
			}
		});
		for (final File directory : directoriesToSearch) {
			if (directory.isDirectory()) {
				final String folderName = FilenameUtils.getName(directory.getAbsolutePath());
				if (folderName.endsWith(".d")) {
					if (folderName.equals(ms2FileName + ".d")) {
						return directory;
					}
				}
				final File ret = searchTIMsTofFolder(directory.getAbsolutePath(), ms2FileName);
				if (ret != null) {
					return ret;
				}
			}
		}
		return null;
	}

}
