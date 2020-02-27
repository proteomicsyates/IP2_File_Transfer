package edu.scripps.yates.ip2tomassive;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import gnu.trove.map.hash.THashMap;

public class GoogleDriveClient {
	private final static Logger log = Logger.getLogger(GoogleDriveClient.class);
	private static final String APPLICATION_NAME = "Salvador Martinez Yates Lab Google API Client";
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static final String TOKENS_DIRECTORY_PATH = "tokens";

	private static GoogleDriveClient instance;
	private final String CREDENTIALS_FILE_PATH;
	private final Map<String, List<File>> filesByNameAndParentID = new THashMap<String, List<File>>();
	private final Map<String, File> filesByID = new THashMap<String, File>();
	private Drive service;

	public static GoogleDriveClient getInstance(String credentialsFilePath) {
		if (instance == null) {
			instance = new GoogleDriveClient(credentialsFilePath);
		}
		return instance;
	}

	private GoogleDriveClient(String credentialsFilePath) {
		this.CREDENTIALS_FILE_PATH = credentialsFilePath;
	}

	/**
	 * Global instance of the scopes required by this quickstart. If modifying these
	 * scopes, delete your previously saved tokens/ folder.
	 */
	private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
	public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
	public static final String TEXT_PLAIN = "text/plain";

	/**
	 * Creates an authorized Credential object.
	 * 
	 * @param HTTP_TRANSPORT The network HTTP Transport.
	 * @return An authorized Credential object.
	 * @throws IOException If the credentials.json file cannot be found.
	 */
	private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
		// Load client secrets.
		InputStream in = GoogleDriveClient.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
		if (in == null) {
			in = new FileInputStream(new java.io.File(CREDENTIALS_FILE_PATH));
			if (in == null) {
				throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
			}
		}
		final GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		final GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				clientSecrets, SCOPES)
						.setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
						.setAccessType("offline").build();
		final LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8899).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}

	private Drive getDriveService() throws GeneralSecurityException, IOException {
		if (this.service == null) {
			// Build a new authorized API client service.
			final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
					.setApplicationName(APPLICATION_NAME).build();
		}
		return service;
	}

	/**
	 * 
	 * @param folderIdParent id of the parent folder if known or null for root
	 *                       folder
	 * @param folderName     it can contain '/' and therefore in that case, several
	 *                       subfolders will be created
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public final File createGoogleFolder(String folderIdParent, String rawFolderName)
			throws IOException, GeneralSecurityException {
		log.info("Creating folder at Google drive: '" + rawFolderName + "' on parent folder with ID '" + folderIdParent
				+ "'");
		String[] folderNames = null;
		if (rawFolderName.contains("/")) {
			folderNames = rawFolderName.split("/");
		} else {
			folderNames = new String[1];
			folderNames[0] = rawFolderName;
		}
		final Drive driveService = getDriveService();
		File file = null;
		for (final String folderName : folderNames) {
			final List<File> folders = getGoogleFoldersByName(folderName, folderIdParent);
			if (folders != null && !folders.isEmpty()) {
				folderIdParent = folders.get(0).getId();
				continue;
			}
			final File fileMetadata = new File();

			fileMetadata.setName(folderName);
			fileMetadata.setMimeType("application/vnd.google-apps.folder");

			if (folderIdParent != null) {
				final List<String> parents = Arrays.asList(folderIdParent);
				fileMetadata.setParents(parents);
			}

			// Create a Folder.
			// Returns File object with id & name fields will be assigned values
			file = driveService.files().create(fileMetadata).setFields("id, name, parents").execute();
			folderIdParent = file.getId();

		}
		return file;
	}

	private File _createGoogleFile(String googleFolderIdParent, String contentType, //
			String customFileName, AbstractInputStreamContent uploadStreamContent)
			throws IOException, GeneralSecurityException {

		final File fileMetadata = new File();
		fileMetadata.setName(customFileName);

		final List<String> parents = Arrays.asList(googleFolderIdParent);
		fileMetadata.setParents(parents);
		//
		final Drive driveService = getDriveService();

		final File file = driveService.files().create(fileMetadata, uploadStreamContent)
				.setFields("id, webContentLink, webViewLink, parents, size").execute();

		return file;
	}

	private File _updateGoogleFile(String customFileName, AbstractInputStreamContent uploadStreamContent,
			String fileIDToOverride) throws IOException, GeneralSecurityException {

		final File fileMetadata = new File();
		fileMetadata.setName(customFileName);

		//
		final Drive driveService = getDriveService();

		final File file = driveService.files().update(fileIDToOverride, fileMetadata, uploadStreamContent)
				.setFields("id, webContentLink, webViewLink, parents, size").execute();

		return file;
	}

	//

	/**
	 * Create Google File from byte[]
	 * 
	 * @param googleFolderIdParent
	 * @param contentType
	 * @param customFileName
	 * @param uploadData
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File uploadFile(String googleFolderIdParent, String contentType, //
			String customFileName, byte[] uploadData) throws IOException, GeneralSecurityException {
		//
		final AbstractInputStreamContent uploadStreamContent = new ByteArrayContent(contentType, uploadData);
		//
		return _createGoogleFile(googleFolderIdParent, contentType, customFileName, uploadStreamContent);
	}

	/**
	 * Update/Override Google File from java.io.File
	 * 
	 * 
	 * @param contentType
	 * @param customFileName
	 * @param uploadFile
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File updateFile(String contentType, //
			String customFileName, java.io.File uploadFile, String fileIDToOverride)
			throws IOException, GeneralSecurityException {

		//
		final AbstractInputStreamContent uploadStreamContent = new FileContent(contentType, uploadFile);
		//
		return _updateGoogleFile(customFileName, uploadStreamContent, fileIDToOverride);
	}

	// Create Google File from InputStream
	/**
	 * Update/Override Google File from InputStream
	 * 
	 * 
	 * @param contentType
	 * @param customFileName
	 * @param inputStream
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File updateFile(String contentType, //
			String customFileName, InputStream inputStream, String fileIDToOverride)
			throws IOException, GeneralSecurityException {

		//
		final AbstractInputStreamContent uploadStreamContent = new InputStreamContent(contentType, inputStream);
		//
		return _updateGoogleFile(customFileName, uploadStreamContent, fileIDToOverride);
	}

	/**
	 * Update/Override Google File from byte[]
	 * 
	 * 
	 * @param contentType
	 * @param customFileName
	 * @param uploadData
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File updateFile(String contentType, String customFileName, byte[] uploadData, String fileIDToOverride)
			throws IOException, GeneralSecurityException {
		//
		final AbstractInputStreamContent uploadStreamContent = new ByteArrayContent(contentType, uploadData);
		//
		return _updateGoogleFile(customFileName, uploadStreamContent, fileIDToOverride);
	}

	/**
	 * Create Google File from java.io.File
	 * 
	 * @param googleFolderIdParent
	 * @param contentType
	 * @param customFileName
	 * @param uploadFile
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File uploadFile(String googleFolderIdParent, String contentType, //
			String customFileName, java.io.File uploadFile) throws IOException, GeneralSecurityException {

		//
		final AbstractInputStreamContent uploadStreamContent = new FileContent(contentType, uploadFile);
		//
		return _createGoogleFile(googleFolderIdParent, contentType, customFileName, uploadStreamContent);
	}

	// Create Google File from InputStream
	/**
	 * Create Google File from InputStream
	 * 
	 * @param googleFolderIdParent
	 * @param contentType
	 * @param customFileName
	 * @param inputStream
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public File uploadFile(String googleFolderIdParent, String contentType, //
			String customFileName, InputStream inputStream) throws IOException, GeneralSecurityException {

		//
		final AbstractInputStreamContent uploadStreamContent = new InputStreamContent(contentType, inputStream);
		//
		return _createGoogleFile(googleFolderIdParent, contentType, customFileName, uploadStreamContent);
	}

	// com.google.api.services.drive.model.File
	public final List<File> getGoogleSubFolders(String googleFolderIdParent)
			throws IOException, GeneralSecurityException {

		final Drive driveService = getDriveService();

		String pageToken = null;
		final List<File> list = new ArrayList<File>();

		String query = null;
		if (googleFolderIdParent == null) {
			query = " mimeType = 'application/vnd.google-apps.folder' " //
					+ " and 'root' in parents";
		} else {
			query = " mimeType = 'application/vnd.google-apps.folder' " //
					+ " and '" + googleFolderIdParent + "' in parents";
		}

		do {
			final FileList result = driveService.files().list().setQ(query).setSpaces("drive") //
					// Fields will be assigned values: id, name, createdTime
					.setFields("nextPageToken, files(id, name, createdTime, modifiedTime, mimeType, parents, size)")//
					.setPageToken(pageToken).execute();
			for (final File file : result.getFiles()) {
				list.add(file);
			}
			pageToken = result.getNextPageToken();
		} while (pageToken != null);
		//
		return list;
	}

	// com.google.api.services.drive.model.File
	public final List<File> getGoogleRootFolders() throws IOException, GeneralSecurityException {
		return getGoogleSubFolders(null);
	}

	// com.google.api.services.drive.model.File
	public final List<File> getGoogleFilesByName(String fileNameLike) throws IOException, GeneralSecurityException {
		return getGoogleFilesByName(fileNameLike, null);
	}

	public final List<File> getGoogleFoldersByName(String fileNameLike) throws IOException, GeneralSecurityException {
		return getGoogleFoldersByName(fileNameLike, null);
	}

	public final List<File> getGoogleFilesByName(String fileNameLike, String parentID)
			throws IOException, GeneralSecurityException {
		return getGoogleFilesOrFoldersByName(fileNameLike, false, parentID);
	}

	public final List<File> getGoogleFoldersByName(String fileNameLike, String parentID)
			throws IOException, GeneralSecurityException {
		return getGoogleFilesOrFoldersByName(fileNameLike, true, parentID);
	}

	/**
	 * 
	 * @param fileNameLike
	 * @param folder       Boolean: if true, it searches for folders, otherwise, for
	 *                     files. If null, it searches for everything
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public final List<File> getGoogleFilesOrFoldersByName(String fileNameLike, Boolean folder, String parentID)
			throws IOException, GeneralSecurityException {
		if (this.filesByNameAndParentID.containsKey(fileNameLike + parentID)) {
			return this.filesByNameAndParentID.get(fileNameLike + parentID);
		}
		final Drive driveService = getDriveService();

		String pageToken = null;

		String mimeTypeString = null;
		if (folder != null) {
			if (folder) {
				mimeTypeString = "mimeType = 'application/vnd.google-apps.folder' ";
			} else {
				mimeTypeString = "mimeType != 'application/vnd.google-apps.folder' ";
			}
		}
		// look if fileNameLike contains file separators '/'
		String[] folderNames;
		if (fileNameLike.contains("/")) {
			folderNames = fileNameLike.split("/");
		} else {
			folderNames = new String[1];
			folderNames[0] = fileNameLike;
		}

		String query = " name = '" + folderNames[folderNames.length - 1] + "' ";
		if (mimeTypeString != null) {
			query += " and " + mimeTypeString;
		}
		if (parentID != null) {
			query += " and '" + parentID + "' in parents";
		}
		final List<File> list = new ArrayList<File>();
		do {
			final FileList result = driveService.files().list().setQ(query).setSpaces("drive") //
					// Fields will be assigned values: id, name, createdTime, mimeType
					.setFields("nextPageToken, files(id, name, createdTime, modifiedTime, mimeType, parents, size)")//
					.setPageToken(pageToken).execute();
			for (final File file : result.getFiles()) {
				if (folderNames.length == 1
						|| isFileInPath(file, (String[]) ArrayUtils.subarray(folderNames, 0, folderNames.length - 1))) {
					list.add(file);
					if (!this.filesByNameAndParentID.containsKey(fileNameLike + parentID)) {
						this.filesByNameAndParentID.put(fileNameLike + parentID, new ArrayList<File>());
					}
					this.filesByNameAndParentID.get(fileNameLike + parentID).add(file);
				}
			}
			pageToken = result.getNextPageToken();
		} while (pageToken != null);

		//
		if (list.size() > 1) {
			log.info("multiple files with the same name '" + fileNameLike + "' and in the same folder are found!");
		}
		return list;
	}

	private boolean isFileInPath(File file, String[] folderNames) throws IOException, GeneralSecurityException {
		if (folderNames == null || folderNames.length == 0) {
			return true;
		}
		final List<String> parentsIDs = file.getParents();
		for (final String parentID : parentsIDs) {
			final File parentFile = getGoogleFileByID(parentID);
			if (parentFile.getName().equals(folderNames[folderNames.length - 1])) {
				if (isFileInPath(parentFile, (String[]) ArrayUtils.subarray(folderNames, 0, folderNames.length - 1))) {
					return true;
				}
			}
		}
		return false;
	}

	private void addFileToCache(File file) {
		this.filesByID.put(file.getId(), file);
	}

	public File getGoogleFileByID(String fileID) throws IOException, GeneralSecurityException {
		if (!filesByID.containsKey(fileID)) {
			final File file = getDriveService().files().get(fileID)
					.setFields("id, name, createdTime, mimeType, parents, size").execute();
			if (file != null) {
				addFileToCache(file);
			}
		}
		return filesByID.get(fileID);

	}

	/**
	 * Deletes a file in Google Drive with a certain ID
	 * 
	 * @param fileId
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public void deleteFile(String fileId) throws IOException, GeneralSecurityException {
		getDriveService().files().delete(fileId).execute();

	}

}
