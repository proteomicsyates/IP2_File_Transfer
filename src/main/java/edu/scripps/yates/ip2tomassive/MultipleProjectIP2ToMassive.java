package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import edu.scripps.yates.utilities.dates.DatesUtil;
import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.ftp.FTPUtils;
import edu.scripps.yates.utilities.progresscounter.ProgressCounter;
import edu.scripps.yates.utilities.progresscounter.ProgressPrintingType;

public class MultipleProjectIP2ToMassive extends IP2ToMassive {
	private final static Logger log = Logger.getLogger(MultipleProjectIP2ToMassive.class);
	protected static final String DATASET = "DATASET";
	private final Map<String, Dataset> datasetsByName = new HashMap<String, Dataset>();
	protected final Map<String, String> keywordTranslations;

	public MultipleProjectIP2ToMassive(MySftpProgressMonitor progressMonitor, File propertiesFile,
			Map<String, String> keywordTranslations, File remotePathsFile) throws IOException {
		super(progressMonitor, propertiesFile);
		this.keywordTranslations = keywordTranslations;
		if (remotePathsFile != null) {
			processRemotePaths(remotePathsFile);
		}
	}

	public void transferDatasets() {
		final long t1 = System.currentTimeMillis();
		long totalTransferredSize = 0l;
		for (final String datasetName : datasetsByName.keySet()) {
			log.info(datasetName + " transfer started");
			final long transferDatasetSize = transferDataset(datasetName);
			totalTransferredSize += transferDatasetSize;
			log.info(datasetName + " (" + FileUtils.getDescriptiveSizeFromBytes(transferDatasetSize)
					+ ") transfer finished. Total transfered so far: "
					+ FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize));

		}
		final long spendTime = System.currentTimeMillis() - t1;
		final String speed = FileUtils.getDescriptiveSizeFromBytes(
				Double.valueOf(totalTransferredSize / (spendTime / 1000.0)).longValue()) + "/sg";
		System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred for "
				+ datasetsByName.size() + " datasets in " + DatesUtil.getDescriptiveTimeFromMillisecs(spendTime) + " ("
				+ speed + ")");

	}

	private long transferDataset(String datasetName) {
		long totalTransferredSize = 0l;
		final Dataset dataset = datasetsByName.get(datasetName);
		if (dataset == null) {
			throw new IllegalArgumentException(datasetName + " dataset not found");
		}
		final Set<FileType> fileTypes = dataset.getPathsByFileType().keySet();
		final Map<String, String> outputFileNamesByPath = dataset.getOutputFileNameByPath();

		final List<FileType> fileTypesList = new ArrayList<FileType>();
		fileTypesList.addAll(fileTypes);
		Collections.sort(fileTypesList);
		for (final FileType fileType : fileTypesList) {
			final List<String> paths = dataset.getPathsByFileType().get(fileType);
			final ProgressCounter counter = new ProgressCounter(paths.size(), ProgressPrintingType.EVERY_STEP, 1);
			for (int i = 0; i < paths.size(); i++) {
				counter.increment();
				final String path = paths.get(i);
				final String outputFileName = outputFileNamesByPath.get(path);

				long transferredSize = 0l;
				if (isLocalFile(path)) {
					transferredSize = transferLocalFile(path, outputFileName, fileType, dataset);
				} else {
					transferredSize = transferFile(path, outputFileName, fileType, dataset);
				}
				totalTransferredSize += transferredSize;
				System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred in "
						+ datasetName + " dataset so far (file " + (i + 1) + " out of " + paths.size() + ")");
				final String printIfNecessary = counter.printIfNecessary();
				if (!"".equals(printIfNecessary)) {
					System.out.println(printIfNecessary);
				}
			}
		}
		System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred in "
				+ datasetName + " dataset");
		return totalTransferredSize;

	}

	private long transferLocalFile(String fullPathLocalFile, String outputFileName, FileType fileType,
			Dataset dataset) {
		long sizeTransferred = 0l;
		try {
			if (outputFileName == null) {
				outputFileName = FilenameUtils.getName(fullPathLocalFile);
			}
			FTPClient ftpMassive = null;
			String folderPathToMassive = dataset.getName() + "/" + fileType.name();
			try {
				ftpMassive = loginToMassive();

				if (submissionName != null && !"".equals(submissionName)) {
					folderPathToMassive = "/" + submissionName + "/" + folderPathToMassive;
				}
				log.info("Transferring file " + fullPathLocalFile);
				createFullPathInRemote(ftpMassive, folderPathToMassive);
			} catch (final Exception e) {
				e.printStackTrace();
				throw e;
			} finally {
				if (ftpMassive != null) {
					ftpMassive.logout();
					ftpMassive.disconnect();
					FTPUtils.showServerReply(ftpMassive);
				}
			}

			final String fileName = FilenameUtils.getName(outputFileName);
			progressMonitor.setSuffix("(" + fileName + ") ");
			try {
				final long sizeInLocal = new File(fullPathLocalFile).length();
				long sizeInMassive = 0;
				try {
					ftpMassive = loginToMassive();
					final String fullPathToMassive = folderPathToMassive + "/" + fileName;
					sizeInMassive = FTPUtils.getSize(ftpMassive, fullPathToMassive);

					if (sizeInLocal <= sizeInMassive) {
						log.info("File '" + fullPathLocalFile + "' found in MassIVE with  "
								+ FileUtils.getDescriptiveSizeFromBytes(sizeInLocal) + ". It will be skipped");
						return sizeInMassive;
					} else if (sizeInMassive > 0) {
						log.info("File '" + fullPathLocalFile + "' found in MassIVE but sizes are different: Local:"
								+ sizeInLocal + " Massive:" + sizeInMassive + " diff: "
								+ (sizeInLocal - sizeInMassive));
					}

					final String outputFullPath = folderPathToMassive + "/" + fileName;
					log.info("Output file in Massive: " + outputFullPath);
					final OutputStream outputStreamInMassive = ftpMassive.storeFileStream(outputFullPath);
					FTPUtils.showServerReply(ftpMassive);
					if (outputStreamInMassive == null) {
						throw new IllegalArgumentException("Error trying to create output stream to Massive");
					}
					final FileInputStream inputStream = new FileInputStream(fullPathLocalFile);

					final long transferred = IOUtils.copyLarge(inputStream, outputStreamInMassive);

					outputStreamInMassive.close();
					sizeTransferred += transferred;
				} finally {
					ftpMassive.logout();
					ftpMassive.disconnect();
					FTPUtils.showServerReply(ftpMassive);
				}

			} catch (final IOException e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}

		} catch (final SocketException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		} catch (final IOException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		}
		return sizeTransferred;
	}

	private boolean isLocalFile(String path) {
		return new File(path).exists();
	}

	private void processRemotePaths(File remotePathsFile) throws IOException {
		final List<String> readAllLines = Files.readAllLines(Paths.get(remotePathsFile.toURI()));
		FileType currentFileType = null;
		String currentDataSet = null;
		for (String line : readAllLines) {
			line = line.trim();
			if (isPath(line)) {
				String fileName = currentFileType.getDefaultFileName();
				String path = line;
				if (line.contains("\t")) {
					final String[] split = line.split("\t");
					path = split[0];
					fileName = split[1];
					final String keywordToTranslate = getKeywordToTranslate(fileName);
					if (keywordToTranslate != null) {
						final String newName = fileName.replace(keywordToTranslate,
								keywordTranslations.get(keywordToTranslate));
						log.info("File named as " + fileName + " now is mapped to " + newName);
						fileName = newName;
					}
				}
				if (fileName == null) {
					// it is because it is a wild card
					// take all in the transfer
				}
				// if the name is like dtaselect-filter.txt or census-out.txt or so
				if (isGenericName(fileName)) {
					final String fileName2 = getFileNameFromIP2ServerPath(line);
					if (fileName2 != null) {
						fileName = fileName2;
					}
				}
				if (currentDataSet == null) {
					throw new IllegalArgumentException(
							"Dataset name not found. Did you forget to add a line as: 'DATASET	dataset_name'? ");
				}
				final Dataset currentDataset = datasetsByName.get(currentDataSet);
				currentDataset.addRemoteOutputFileName(path, fileName);
				currentDataset.addPath(currentFileType, path);
			} else if (FileType.getbyDescription(line) != null) {
				currentFileType = FileType.getbyDescription(line);
			} else {
				if (line.startsWith(DATASET)) {
					currentDataSet = line.substring(DATASET.length() + 1).trim();
					if (!datasetsByName.containsKey(currentDataSet)) {
						datasetsByName.put(currentDataSet, new Dataset(currentDataSet));
					}
				}
			}
		}
	}

	/**
	 * It takes the name from a IP2 server folder.<br>
	 * If the folder is like:
	 * /data/2/rpark/ip2_data/cbamberg/CPP_on_brain_tissue/B9_X5628_UZ_Sup_2019_02_11_11_242951/quant/2019_02_19_10_17605/DTASelect-filter.txt,
	 * the returned file name should be: B9_X5628_UZ_Sup, which is the name of the
	 * folder that is parent of a special folder ( quant, spectra or search ) and
	 * after removing the date from it.
	 * 
	 * @param line
	 * @return
	 */
	private String getFileNameFromIP2ServerPath(String line) {
		final String[] ip2FolderTypes = { "quant", "spectra", "search" };
		File folder = new File(line);
		String extension = "";
		while (folder != null) {
			final String extension2 = FilenameUtils.getExtension(folder.getAbsolutePath());
			if (extension2 != null && !"".equals(extension2)) {
				extension = extension2;
			}
			final String baseName = FilenameUtils.getBaseName(folder.getAbsolutePath());
			boolean specialFolderFound = false;
			for (final String string : ip2FolderTypes) {
				if (baseName.equalsIgnoreCase(string)) {
					specialFolderFound = true;
				}
			}
			folder = folder.getParentFile();
			if (specialFolderFound) {
				break;
			}
		}
		// now in folder we have the folder with the name as:
		// B9_X5628_UZ_Sup_2019_02_11_11_242951
		final String crudeFileName = FilenameUtils.getBaseName(folder.getAbsolutePath());

		final String pattern = "(.+)_\\d\\d\\d\\d_\\d\\d_.*";
		final Pattern compile = Pattern.compile(pattern);
		final Matcher matcher = compile.matcher(crudeFileName);
		if (matcher.find()) {
			String ret = matcher.group(1);
			if (extension != null && !"".equals(extension)) {
				ret = ret + "." + extension;
			}
			return ret;
		}
		return null;
	}

	private boolean isGenericName(String fileName) {
		if (fileName == null) {
			return false;
		}
		for (final FileType fileType : FileType.values()) {
			if (fileType.getDefaultFileName() != null) {
				if (fileType.getDefaultFileName().equals(fileName)) {
					return true;
				}
			}
		}
		return false;
	}

	protected String getKeywordToTranslate(String fileName) {
		for (final String keyword : keywordTranslations.keySet()) {
			if (fileName.contains(keyword)) {
				return keyword;
			}
		}
		return null;
	}

	private Map<String, String> getKeywordTranslations() {
		return keywordTranslations;
	}

	private boolean isPath(String line) {

		if (line.startsWith("/")) {
			return true;
		} else {
			// TODO add support for windows paths
			final char firstLetter = line.charAt(0);
			if ((firstLetter >= 'a' && firstLetter <= 'z') || (firstLetter >= 'A' && firstLetter <= 'Z')) {
				if (line.charAt(1) == ':') {
					return true;
				}
			}
		}
		return false;
	}

	public long transferFile(String fullPathToIP2, String outputFileName, FileType fileType, Dataset dataset) {
		Session sshIP2 = null;
		long sizeTransferred = 0l;
		try {
			sshIP2 = loginToIP2();
			FTPClient ftpMassive = null;
			String folderPathToMassive = dataset.getName() + "/" + fileType.name();
			try {
				ftpMassive = loginToMassive();

				if (submissionName != null && !"".equals(submissionName)) {
					folderPathToMassive = "/" + submissionName + "/" + folderPathToMassive;
				}
				log.info("Transferring file " + fullPathToIP2);
				createFullPathInRemote(ftpMassive, folderPathToMassive);
			} catch (final Exception e) {
				e.printStackTrace();
				throw e;
			} finally {
				if (ftpMassive != null) {
					ftpMassive.logout();
					ftpMassive.disconnect();
					FTPUtils.showServerReply(ftpMassive);
				}
			}
			if (outputFileName == null || "".equals(FilenameUtils.getExtension(fullPathToIP2))) {
				// fullPathToIP2 points to a set of files, such as:
				// path/to/files/*.raw
				String pathToFolderInIP2 = FilenameUtils.getFullPath(fullPathToIP2);
				String extension = FilenameUtils.getExtension(fullPathToIP2);
				if ("".equals(extension)) {
					pathToFolderInIP2 = fullPathToIP2;
				}
				final List<LsEntry> ftpFilesInIP2 = new ArrayList<LsEntry>();

				final boolean exist = FTPUtils.exist(sshIP2, fullPathToIP2);
				if (exist) {
					final LsEntry lsEntry = FTPUtils.getFileEntry(sshIP2, fullPathToIP2);
					ftpFilesInIP2.add(lsEntry);
				} else {

					if ("".equals(extension)) {
						extension = fileType.getExtension();
					}
					ftpFilesInIP2.addAll(FTPUtils.getFilesInFolderByExtension(sshIP2, pathToFolderInIP2, extension));
				}

				// sort ftpFilesInIP2 by name
				Collections.sort(ftpFilesInIP2, new Comparator<LsEntry>() {

					@Override
					public int compare(LsEntry o1, LsEntry o2) {
						return o1.getFilename().compareTo(o2.getFilename());
					}
				});
				int counter = 1;
				for (final LsEntry ftpFileInIP2 : ftpFilesInIP2) {

					try {
						fullPathToIP2 = pathToFolderInIP2;
						if (!fullPathToIP2.endsWith("/")) {
							fullPathToIP2 = fullPathToIP2 + "/";
						}
						fullPathToIP2 = fullPathToIP2 + ftpFileInIP2.getFilename();
						String fileName = FilenameUtils.getName(fullPathToIP2);
						if (outputFileName != null) {
							fileName = outputFileName + "_" + counter;
							if ("".equals(FilenameUtils.getExtension(fileName))) {
								fileName = fileName + "." + extension;
							}
						}
						final String keywordToTranslate = getKeywordToTranslate(fileName);
						if (keywordToTranslate != null) {
							final String newName = fileName.replace(keywordToTranslate,
									keywordTranslations.get(keywordToTranslate));
							log.info("File named as " + fileName + " now is mapped to " + newName);
							fileName = newName;
						}
						progressMonitor.setSuffix("(" + fileName + ") ");

						final long sizeInIP2 = ftpFileInIP2.getAttrs().getSize();
						final String fullPathToMassive = folderPathToMassive + "/" + fileName;
						long sizeInMassive = 0;
						try {
							ftpMassive = loginToMassive();
							ftpMassive.setFileType(FTP.BINARY_FILE_TYPE);
							sizeInMassive = FTPUtils.getSize(ftpMassive, fullPathToMassive);
							if (sizeInMassive > -1) {
								if (sizeInMassive == sizeInIP2) {
									log.info("File  '" + FilenameUtils.getName(fullPathToIP2)
											+ "' found in MassIVE as '" + fullPathToMassive + "' with  "
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2)
											+ ". It will be skipped");
									sizeTransferred += sizeInIP2;
									continue;
								} else if (sizeInMassive > sizeInIP2) {
									log.info("File '" + FilenameUtils.getName(fullPathToIP2) + "' is bigger in MassIVE:"
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive) + " than in IP2:"
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + " diff: "
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive - sizeInIP2));
								} else if (sizeInIP2 > sizeInMassive) {
									log.info("File '" + FilenameUtils.getName(fullPathToIP2) + "' is bigger in IP2:"
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + " than in Massive:"
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive) + " diff: "
											+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2 - sizeInMassive));
								}
							}
							log.info("Output file in Massive: " + fullPathToMassive);

							final OutputStream outputStreamInMassive = ftpMassive.storeFileStream(fullPathToMassive);
							FTPUtils.showServerReply(ftpMassive);
							if (outputStreamInMassive == null) {
								throw new IllegalArgumentException("Error trying to create output stream to Massive");
							}
							final ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);

							sftpChannel.get(fullPathToIP2, outputStreamInMassive, progressMonitor);
							sizeTransferred += sizeInIP2;
							sftpChannel.exit();
							outputStreamInMassive.close();
							log.info(
									"Transfer of " + FileUtils.getDescriptiveSizeFromBytes(sizeTransferred) + " done.");
							if (sizeTransferred != sizeInIP2 && sizeInIP2 > -1) {
								log.warn(FileUtils.getDescriptiveSizeFromBytes(sizeTransferred - sizeInIP2));
							}
						} finally {
							ftpMassive.logout();
							ftpMassive.disconnect();
							FTPUtils.showServerReply(ftpMassive);
							counter++;
						}

					} catch (final IOException e) {
						e.printStackTrace();
						log.warn(e.getMessage());

					} catch (final SftpException e) {
						e.printStackTrace();
						log.warn(e.getMessage());
					}

				}
			} else {
				final String fileName = FilenameUtils.getName(outputFileName);
				progressMonitor.setSuffix("(" + fileName + ") ");
				try {
					final long sizeInIP2 = FTPUtils.getSize(sshIP2, fullPathToIP2);
					long sizeInMassive = 0;
					try {
						ftpMassive = loginToMassive();
						final String fullPathToMassive = folderPathToMassive + "/" + fileName;
						sizeInMassive = FTPUtils.getSize(ftpMassive, fullPathToMassive);

						if (sizeInIP2 <= sizeInMassive) {
							log.info("File '" + fullPathToIP2 + "' found in MassIVE with  "
									+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ". It will be skipped");
							return sizeInMassive;
						} else if (sizeInMassive > 0) {
							log.info("File '" + fullPathToIP2 + "' found in MassIVE but sizes are different: IP2:"
									+ sizeInIP2 + " Massive:" + sizeInMassive + " diff: "
									+ (sizeInIP2 - sizeInMassive));
						}

						final String outputFullPath = folderPathToMassive + "/" + fileName;
						log.info("Output file in Massive: " + outputFullPath);
						final OutputStream outputStreamInMassive = ftpMassive.storeFileStream(outputFullPath);
						FTPUtils.showServerReply(ftpMassive);
						if (outputStreamInMassive == null) {
							throw new IllegalArgumentException("Error trying to create output stream to Massive");
						}

						final ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);
						sftpChannel.get(fullPathToIP2, outputStreamInMassive, progressMonitor);
						sftpChannel.exit();
						outputStreamInMassive.close();
						sizeTransferred += sizeInIP2;
					} finally {
						ftpMassive.logout();
						ftpMassive.disconnect();
						FTPUtils.showServerReply(ftpMassive);
					}

				} catch (final IOException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				} catch (final JSchException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				} catch (final SftpException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				}
			}
		} catch (final SocketException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		} catch (final IOException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		} catch (final JSchException e1) {
			e1.printStackTrace();
			log.warn(e1.getMessage());
		} catch (final SftpException e1) {
			e1.printStackTrace();
			log.warn(e1.getMessage());
		} finally {

			if (sshIP2 != null) {
				sshIP2.disconnect();
			}

		}
		return sizeTransferred;
	}

	protected boolean createFullPathInRemote(FTPClient ftpOut, String remotePath) throws IOException {

		return FTPUtils.makeDirectories(ftpOut, remotePath, System.out);

	}

}
