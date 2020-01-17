package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.ftp.FTPUtils;

/**
 * It uploads everything to GoogleDrive keeping the folder structure, and file
 * names, being the folder structure after the projectBasePathInIP2
 * 
 * @author salvador
 *
 */
public class MultipleProjectIP2ToGoogleDrive extends MultipleProjectIP2ToMassive {
	private final static Logger log = Logger.getLogger(MultipleProjectIP2ToGoogleDrive.class);
	private static final String contentType = GoogleDriveClient.DEFAULT_CONTENT_TYPE;
	private final GoogleDriveClient googleDriveClient;
	private boolean override;

	public MultipleProjectIP2ToGoogleDrive(MySftpProgressMonitor progressMonitor, File propertiesFile,
			Map<String, String> keywordTranslations, File remotePathsFile, GoogleDriveClient googleDriveClient,
			boolean overrideIfPresent) throws IOException {
		super(progressMonitor, propertiesFile, keywordTranslations, remotePathsFile);
		this.googleDriveClient = googleDriveClient;
		this.override = overrideIfPresent;
	}

	@Override
	public long transferFile(String fullPathToIP2, String outputFileName, FileType fileType, Dataset dataset) {
		Session sshIP2 = null;
		long sizeTransferred = 0l;
		try {
			final String projectBasePathInIP2 = getProjectBasePathInIP2();

			sshIP2 = loginToIP2();
			String remoteFolderPath = "Proteomics/data";
			if (submissionName != null && !"".equals(submissionName)) {
				remoteFolderPath = remoteFolderPath + "/" + submissionName;
			} else {
				remoteFolderPath += "/" + dataset.getName();
			}
			remoteFolderPath += "/" + fileType.name();

			// if contains null, remove it.
			if (remoteFolderPath.contains("/null")) {
				remoteFolderPath = remoteFolderPath.replace("/null", "");
			}
			// now preserve the origin folder structure
			final String originalFolderStructure = fullPathToIP2.substring(projectBasePathInIP2.length());
			remoteFolderPath += FilenameUtils.getFullPathNoEndSeparator(originalFolderStructure);
			String parentID = null;
			try {

				final List<com.google.api.services.drive.model.File> foldersWithName = googleDriveClient
						.getGoogleFoldersByName(remoteFolderPath);

				if (foldersWithName != null && !foldersWithName.isEmpty()) {
					parentID = foldersWithName.get(0).getId();
				} else {
					// create folder
					final com.google.api.services.drive.model.File folder = googleDriveClient.createGoogleFolder("root",
							remoteFolderPath);
					parentID = folder.getId();
				}

			} catch (final GeneralSecurityException e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e);
			} finally {

			}
			if (outputFileName == null) {
				// fullPathToIP2 points to a set of files, such as:
				// path/to/files/*.raw
				final String extension = FilenameUtils.getExtension(fullPathToIP2);

				final String pathToFolderInIP2 = FilenameUtils.getFullPath(fullPathToIP2);
				final List<LsEntry> ftpFilesInIP2 = new ArrayList<LsEntry>();
				if (FTPUtils.exist(sshIP2, fullPathToIP2)) {
					final LsEntry lsEntry = FTPUtils.getFileEntry(sshIP2, fullPathToIP2);
					ftpFilesInIP2.add(lsEntry);
				} else {
					ftpFilesInIP2.addAll(FTPUtils.getFilesInFolderByExtension(sshIP2, pathToFolderInIP2, extension));
				}
				for (final LsEntry ftpFileInIP2 : ftpFilesInIP2) {

					try {
						fullPathToIP2 = pathToFolderInIP2 + ftpFileInIP2.getFilename();
						String fileName = FilenameUtils.getName(fullPathToIP2);
						final List<com.google.api.services.drive.model.File> filesInGoogleDrive = googleDriveClient
								.getGoogleFilesByName(fileName, parentID);
						if (filesInGoogleDrive != null && !filesInGoogleDrive.isEmpty()) {
							final Long sizeInGoogleDrive = filesInGoogleDrive.get(0).getSize();

							final long sizeInIP2 = ftpFileInIP2.getAttrs().getSize();
							if (sizeInGoogleDrive == sizeInIP2) {
								log.info("File found at Google drive at: " + remoteFolderPath + " with the same size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ "). Skipping upload...");
								return 0l;
							}
							if (!override) {
								log.info("File found at Google drive at: " + remoteFolderPath + " . However, its size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ ") is different than the file in IP2 ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ") but override is FALSE");
								return 0l;
							} else if (override) {
								log.info("File found at Google drive at: " + remoteFolderPath + " . However, its size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ ") is different than the file in IP2 ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ") and override is TRUE");
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
						final String fullPathToMassive = remoteFolderPath + "/" + fileName;
						final long sizeInMassive = 0;
						try {

							log.info("Output file in Google Drive: " + fullPathToMassive);
							final ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);
							final InputStream inputStream = sftpChannel.get(fullPathToIP2);

							final com.google.api.services.drive.model.File uploadedFile = googleDriveClient
									.uploadFile(parentID, contentType, fileName, inputStream);

							sizeTransferred += uploadedFile.getSize();
							sftpChannel.exit();

							log.info(
									"Transfer of " + FileUtils.getDescriptiveSizeFromBytes(sizeTransferred) + " done.");
							if (sizeTransferred != sizeInIP2 && sizeInIP2 > -1) {
								log.warn(FileUtils.getDescriptiveSizeFromBytes(sizeTransferred - sizeInIP2));
							}
						} finally {

						}

					} catch (final IOException e) {
						e.printStackTrace();
						log.warn(e.getMessage());

					} catch (final SftpException e) {
						e.printStackTrace();
						log.warn(e.getMessage());
					} catch (final GeneralSecurityException e) {
						e.printStackTrace();
						log.warn(e.getMessage());
					}

				}
			} else {
				final String fileName = FilenameUtils.getName(fullPathToIP2);
				progressMonitor.setSuffix("(" + fileName + ") ");
				try {

					try {
						final List<com.google.api.services.drive.model.File> filesInGoogleDrive = googleDriveClient
								.getGoogleFilesByName(fileName, parentID);
						if (filesInGoogleDrive != null && !filesInGoogleDrive.isEmpty()) {
							final Long sizeInGoogleDrive = filesInGoogleDrive.get(0).getSize();
							final LsEntry ftpFileInIP2 = FTPUtils.getFileEntry(sshIP2, fullPathToIP2);
							final long sizeInIP2 = ftpFileInIP2.getAttrs().getSize();
							if (sizeInGoogleDrive == sizeInIP2) {
								log.info("File found at Google drive at: " + remoteFolderPath + " with the same size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ "). Skipping upload...");
								return 0l;
							}
							if (!override) {
								log.info("File found at Google drive at: " + remoteFolderPath + " . However, its size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ ") is different than the file in IP2 ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ") but override is FALSE");
								return 0l;
							} else if (override) {
								log.info("File found at Google drive at: " + remoteFolderPath + " . However, its size ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInGoogleDrive)
										+ ") is different than the file in IP2 ("
										+ FileUtils.getDescriptiveSizeFromBytes(sizeInIP2) + ") and override is TRUE");
							}
						}

						final ChannelSftp sftpChannel = FTPUtils.openSFTPChannel(sshIP2);
						final InputStream inputStream = sftpChannel.get(fullPathToIP2);

						final com.google.api.services.drive.model.File uploadedFile = googleDriveClient
								.uploadFile(parentID, contentType, fileName, inputStream);
						sizeTransferred += uploadedFile.getSize();
					} finally {

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
				} catch (final GeneralSecurityException e) {
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

	public boolean isOverride() {
		return override;
	}

	public void setOverride(boolean override) {
		this.override = override;
	}

}
