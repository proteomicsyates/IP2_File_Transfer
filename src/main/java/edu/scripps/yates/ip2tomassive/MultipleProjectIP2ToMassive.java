package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.ftp.FTPUtils;

public class MultipleProjectIP2ToMassive extends IP2ToMassive {
	private final static Logger log = Logger.getLogger(MultipleProjectIP2ToMassive.class);
	protected static final String DATASET = "DATASET";
	private final Map<String, Dataset> datasetsByName = new HashMap<String, Dataset>();
	private String submissionName = "";

	public MultipleProjectIP2ToMassive(MySftpProgressMonitor progressMonitor, File propertiesFile, File remotePathsFile)
			throws IOException {
		super(progressMonitor, propertiesFile);
		if (remotePathsFile != null) {
			processRemotePaths(remotePathsFile);
		}
	}

	public void transferDatasets() {
		long totalTransferredSize = 0l;
		for (String datasetName : datasetsByName.keySet()) {
			log.info(datasetName + " transfer started");
			totalTransferredSize += transferDataset(datasetName);
			log.info(datasetName + " transfer finished");

		}
		System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred in "
				+ datasetsByName.size() + " datasets");
	}

	private long transferDataset(String datasetName) {
		long totalTransferredSize = 0l;
		final Dataset dataset = datasetsByName.get(datasetName);
		if (dataset == null) {
			throw new IllegalArgumentException(datasetName + " dataset not found");
		}
		Set<FileType> fileTypes = dataset.getPathsByFileType().keySet();
		final Map<String, String> outputFileNamesByPath = dataset.getOutputFileNameByPath();
		for (FileType fileType : fileTypes) {
			final List<String> paths = dataset.getPathsByFileType().get(fileType);
			for (String path : paths) {
				final String outputFileName = outputFileNamesByPath.get(path);
				long transferredSize = transferFile(path, outputFileName, fileType, dataset);
				totalTransferredSize += transferredSize;
				System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred in "
						+ datasetName + " dataset so far");
			}
		}
		System.out.println(FileUtils.getDescriptiveSizeFromBytes(totalTransferredSize) + " transferred in "
				+ datasetName + " dataset");
		return totalTransferredSize;

	}

	private void processRemotePaths(File remotePathsFile) throws IOException {
		List<String> readAllLines = Files.readAllLines(Paths.get(remotePathsFile.toURI()));
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
				}
				if (fileName == null) {
					// it is because it is a wild card
					// take all in the transfer
				}
				final Dataset currentDataset = datasetsByName.get(currentDataSet);
				currentDataset.addRemoteOutputFileName(path, fileName);
				currentDataset.addPath(currentFileType, path);
			} else if (FileType.getbyDescription(line) != null) {
				currentFileType = FileType.getbyDescription(line);
			} else {
				if (line.startsWith(DATASET)) {
					currentDataSet = line.substring(DATASET.length() + 1).trim();
					datasetsByName.put(currentDataSet, new Dataset(currentDataSet));
				}
			}
		}
	}

	private boolean isPath(String line) {
		if (line.startsWith("/")) {
			return true;
		} else {
			// TODO add support for windows paths
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

				if (this.submissionName != null && !"".equals(submissionName)) {
					folderPathToMassive = "/" + submissionName + "/" + folderPathToMassive;
				}
				log.info("Transferring file " + fullPathToIP2);
				createFullPathInRemote(ftpMassive, folderPathToMassive);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			} finally {
				if (ftpMassive != null) {
					ftpMassive.logout();
					ftpMassive.disconnect();
					FTPUtils.showServerReply(ftpMassive);
				}
			}
			if (outputFileName == null) {
				// fullPathToIP2 points to a set of files, such as:
				// path/to/files/*.raw
				final String extension = FilenameUtils.getExtension(fullPathToIP2);

				String pathToFolderInIP2 = FilenameUtils.getFullPath(fullPathToIP2);

				List<LsEntry> ftpFilesInIP2 = FTPUtils.getFilesInFolderByExtension(sshIP2, pathToFolderInIP2,
						extension);

				for (LsEntry ftpFileInIP2 : ftpFilesInIP2) {

					try {
						fullPathToIP2 = pathToFolderInIP2 + ftpFileInIP2.getFilename();
						String fileName = FilenameUtils.getName(fullPathToIP2);

						progressMonitor.setSuffix("(" + fileName + ") ");

						long sizeInIP2 = ftpFileInIP2.getAttrs().getSize();
						String fullPathToMassive = folderPathToMassive + "/" + fileName;
						long sizeInMassive = 0;
						try {
							ftpMassive = loginToMassive();
							ftpMassive.setFileType(FTP.BINARY_FILE_TYPE);
							sizeInMassive = FTPUtils.getSize(ftpMassive, fullPathToMassive);

							if (sizeInMassive == sizeInIP2) {
								log.info("File  '" + FilenameUtils.getName(fullPathToIP2) + "' found in MassIVE as '"
										+ fullPathToMassive + "' with  "
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ". It will be skipped");
								sizeTransferred += sizeInIP2;
								continue;
							} else if (sizeInMassive > sizeInIP2) {
								log.info(
										"File '" + FilenameUtils.getName(fullPathToIP2) + "' is bigger in MassIVE: IP2:"
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + " Massive:"
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive) + " diff: "
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive - sizeInIP2));
							} else if (sizeInIP2 > sizeInMassive) {
								log.info(
										"File '" + FilenameUtils.getName(fullPathToIP2) + "' is bigger in MassIVE: IP2:"
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + " Massive:"
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInMassive) + " diff: "
												+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2 - sizeInMassive));
							}

							log.info("Output file in Massive: " + fullPathToMassive);

							OutputStream outputStreamInMassive = ftpMassive.storeFileStream(fullPathToMassive);
							FTPUtils.showServerReply(ftpMassive);
							if (outputStreamInMassive == null) {
								throw new IllegalArgumentException("Error trying to create output stream to Massive");
							}
							ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);

							sftpChannel.get(fullPathToIP2, outputStreamInMassive, progressMonitor);
							sizeTransferred += sizeInIP2;
							sftpChannel.exit();
							outputStreamInMassive.close();
							log.info(
									"Transfer of " + FileUtils.getDescriptiveSizeFromBytes(sizeTransferred) + " done.");
							if (sizeTransferred != sizeInIP2) {
								log.warn(FileUtils.getDescriptiveSizeFromBytes(sizeTransferred - sizeInIP2));
							}
						} finally {
							ftpMassive.logout();
							ftpMassive.disconnect();
							FTPUtils.showServerReply(ftpMassive);
						}

					} catch (IOException e) {
						e.printStackTrace();
						log.warn(e.getMessage());

					} catch (SftpException e) {
						e.printStackTrace();
						log.warn(e.getMessage());
					}

				}
			} else {
				String fileName = FilenameUtils.getName(outputFileName);
				progressMonitor.setSuffix("(" + fileName + ") ");
				try {
					long sizeInIP2 = FTPUtils.getSize(sshIP2, fullPathToIP2);
					long sizeInMassive = 0;
					try {
						ftpMassive = loginToMassive();
						String fullPathToMassive = folderPathToMassive + "/" + fileName;
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
						OutputStream outputStreamInMassive = ftpMassive.storeFileStream(outputFullPath);
						FTPUtils.showServerReply(ftpMassive);
						if (outputStreamInMassive == null) {
							throw new IllegalArgumentException("Error trying to create output stream to Massive");
						}

						ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);
						sftpChannel.get(fullPathToIP2, outputStreamInMassive, progressMonitor);
						sftpChannel.exit();
						outputStreamInMassive.close();
						sizeTransferred += sizeInIP2;
					} finally {
						ftpMassive.logout();
						ftpMassive.disconnect();
						FTPUtils.showServerReply(ftpMassive);
					}

				} catch (IOException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				} catch (JSchException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				} catch (SftpException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				}
			}
		} catch (SocketException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		} catch (IOException e2) {
			e2.printStackTrace();
			log.warn(e2.getMessage());
		} catch (JSchException e1) {
			e1.printStackTrace();
			log.warn(e1.getMessage());
		} catch (SftpException e1) {
			e1.printStackTrace();
			log.warn(e1.getMessage());
		} finally {

			if (sshIP2 != null) {
				sshIP2.disconnect();
			}

		}
		return sizeTransferred;
	}

	private boolean createFullPathInRemote(FTPClient ftpOut, String remotePath) throws IOException {

		return FTPUtils.makeDirectories(ftpOut, remotePath, System.out);

	}

	public String getSubmissionName() {
		return submissionName;
	}

	public void setSubmissionName(String submissionName) {
		this.submissionName = submissionName;
	}
}
