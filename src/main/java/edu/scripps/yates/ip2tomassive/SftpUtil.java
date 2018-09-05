package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SftpUtil {

	private static final String SFTP = "sftp";

	/**
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public static void upload(String fileName) throws Exception {

		String method = "upload(String fileName)";
		Session session = null;
		Channel channel = null;
		ChannelSftp channelSftp = null;
		try {
			// Creating and instantiating the jsch specific instance
			JSch jsch = new JSch();
			// Fetching and setting the parameters like: user name, host and
			// port
			// from the properties file
			session = jsch.getSession("<sftp.user.name>", "<sftp.host>", Integer.valueOf("<sftp.port>"));
			// Fetching and setting the password as configured in properties
			// files
			session.setPassword("<sftp.user.password>");
			// Setting the configuration specific properties
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			// Validating if proxy is enabled to access the sftp
			isSftpProxyEnabled(session);
			// Execution start time
			long lStartTime = new Date().getTime();
			System.out.println("Connecting to the sftp...");
			// Connecting to the sftp
			session.connect();
			System.out.println("Connected to the sftp.");
			// Execution end time
			long lEndTime = new Date().getTime();
			System.out.println("---------------------------------------------");
			System.out.println("Connected to SFTP in : " + (lEndTime - lStartTime));
			// Setting the channel type as sftp
			channel = session.openChannel(SFTP);
			// Establishing the connection
			channel.connect();
			channelSftp = (ChannelSftp) channel;
			// Setting the folder location of the external system as
			// configured
			channelSftp.cd("<sftp.output.folder.url>");
			// Creating the file instance
			File file = new File(fileName);
			// Creating an fileInputStream instance
			FileInputStream fileInputStream = new FileInputStream(file);
			// Transfering the file from it source to destination location
			// via sftp
			channelSftp.put(fileInputStream, file.getName());
			// Closing the fileInputStream instance
			fileInputStream.close();
			// De-allocating the fileInputStream instance memory by
			// assigning null
			fileInputStream = null;
		} catch (Exception exception) {
			throw exception;
		} finally {
			// Validating if channel sftp is not null to exit
			if (channelSftp != null) {
				channelSftp.exit();
			}
			// Validating if channel is not null to disconnect
			if (channel != null) {
				channel.disconnect();
			}
			// Validating if session instance is not null to disconnect
			if (session != null) {
				session.disconnect();
			}
		}
	}

	/**
	 * 
	 * @param session
	 */
	private static void isSftpProxyEnabled(Session session) {
		// Fetching the sftp proxy flag set as part of the properties file
		boolean isSftpProxyEnabled = Boolean.valueOf("<sftp.proxy.enable>");
		// Validating if proxy is enabled to access the sftp
		if (isSftpProxyEnabled) {
			// Setting host and port of the proxy to access the SFTP
			session.setProxy(new ProxyHTTP("<sftp.proxy.host>", Integer.valueOf("<sftp.proxy.port>")));
		}
		System.out.println("Proxy status: " + isSftpProxyEnabled);
	}

	/**
	 * 
	 * @param folder
	 * @param event
	 * @param locale
	 */
	public static void download(String folder, String event, String locale) {

		String method = "download(String folder, String event, String locale)";
		Session session = null;
		Channel channel = null;
		ChannelSftp channelSftp = null;
		try {
			// Creating and instantiating the jsch specific instance
			JSch jsch = new JSch();
			// Fetching and setting the parameters like: user name, host and
			// port
			// from the properties file
			session = jsch.getSession("<sftp.user.name>", "<sftp.host>", Integer.valueOf("<sftp.port>"));
			// Fetching and setting the password as configured in properties
			// files
			session.setPassword("<sftp.user.password>");
			// Setting the configuration specific properties
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			// Validating if proxy is enabled to access the sftp
			isSftpProxyEnabled(session);
			// Execution start time
			long lStartTime = new Date().getTime();
			System.out.println("Connecting to the sftp...");
			// Connecting to the sftp
			session.connect();
			System.out.println("Connected to the sftp.");
			// Execution end time
			long lEndTime = new Date().getTime();
			System.out.println("---------------------------------------------");
			System.out.println("Connected to SFTP in : " + (lEndTime - lStartTime));
			// Setting the channel type as sftp
			channel = session.openChannel(SFTP);
			// Establishing the connection
			channel.connect();
			channelSftp = (ChannelSftp) channel;
			try {
				// Setting the folder location of the external system as
				// configured
				// to download the file from
				channelSftp.cd("<sftp.input.folder.url>");
			} catch (SftpException sftpException) {
				System.out.println("Failed to change the directory in sftp.");
			}
			// Listing all the .csv file(s) specific to the source system,
			// event type (download) and locale code
			Vector<ChannelSftp.LsEntry> lsEntries = channelSftp.ls(new StringBuilder("*").append("<sys.code>")
					.append("*").append(event).append("*").append(locale).append("*").append(".csv").toString());
			// Validating if files exist to process the request further
			if (lsEntries.isEmpty()) {
				System.out.println("No file exist in the specified sftp folder location.");
			}
			// Iterating the list of entries to download the file(s) from
			// the sftp
			for (ChannelSftp.LsEntry entry : lsEntries) {
				try {
					// Downloading the specified file from the sftp to the
					// specified folder path
					channelSftp.get(entry.getFilename(),
							new StringBuilder(folder).append(File.separator).append(entry.getFilename()).toString());
				} catch (SftpException sftpException) {
					System.out.println("Failed to download the file the sftp folder location.");
				}
			}
			// Iterating the list of entries to delete the file(s) from the
			// sftp
			for (ChannelSftp.LsEntry entry : lsEntries) {
				try {
					// Deleting the specified file from the sftp
					channelSftp.rm(entry.getFilename());
				} catch (SftpException sftpException) {
					System.out.println("Failed to delete the file from the sftp folder location.");
				}
			}
		} catch (Exception exception) {
			System.out.println("Failed to download the file(s) from SFTP.");
		} finally {
			// Validating if channel sftp is not null to exit
			if (channelSftp != null) {
				channelSftp.exit();
			}
			// Validating if channel is not null to disconnect
			if (channel != null) {
				channel.disconnect();
			}
			// Validating if session instance is not null to disconnect
			if (session != null) {
				session.disconnect();
			}
		}
	}

	public static void createFolder(ChannelSftp channelOut, String projectName) throws SftpException {
		channelOut.mkdir(projectName);
	}

}
