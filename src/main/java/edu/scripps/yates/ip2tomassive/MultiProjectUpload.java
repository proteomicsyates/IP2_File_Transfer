package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import gnu.trove.map.hash.THashMap;

public class MultiProjectUpload {
	public static void main(String[] args) {
		try {
			final File propertiesFile = new File(args[0]);
			File remotePathsFile = null;

			final boolean takeEverythingBaseFolder = true;
			final boolean includeDTASelectFiles = true;
			if (args.length > 1) {
				// if there is more than one file, we already have the remotePathsFile
				remotePathsFile = new File(args[1]);
			} else {
				// if there is only one file is the properties file with the info about the
				// server, the IP2 experiments, etc...
// an example is the file ip2FileTransfer.properties file 
				// create input file
				final InputFileGenerator inputFileGenerator = new InputFileGenerator(propertiesFile,
						includeDTASelectFiles, takeEverythingBaseFolder);
				remotePathsFile = inputFileGenerator.run();
			}
			boolean useGoogleDrive = false;
			if (args.length > 2) {
				useGoogleDrive = Boolean.valueOf(args[2]);
			}
			final Map<String, String> translations = new THashMap<String, String>();
			// translations.put("X09", "normal1");
			// translations.put("X5709", "normal1");
			// translations.put("X83", "normal2");
			// translations.put("X5783", "normal2");
			// translations.put("X5750", "AD1");
			// translations.put("X5743", "AD2");
			// translations.put("X5248", "control3");
			// translations.put("X4870", "control4");
			// translations.put("X5763", "AD3");
			// translations.put("X5798", "AD4");
			// translations.put("X43", "AD2");
			// translations.put("X50", "AD1");
			final MySftpProgressMonitor progressMonitor = new MySftpProgressMonitor(System.out);

			MultipleProjectIP2ToMassive m = null;
			if (useGoogleDrive) {
				final String credentialsFilePath = args[3];
				final boolean overrideIfDifferentSize = Boolean.valueOf(args[4]);
				final GoogleDriveClient client = GoogleDriveClient.getInstance(credentialsFilePath);
				m = new MultipleProjectIP2ToGoogleDrive(progressMonitor, propertiesFile, translations, remotePathsFile,
						client, overrideIfDifferentSize);
			} else {
				m = new MultipleProjectIP2ToMassive(progressMonitor, propertiesFile, translations, remotePathsFile);
			}
			// m.setSubmissionName("Surface Labeling");

			m.transferDatasets();
			System.out.println("PROGRAM FINISHED OK");
			System.exit(0);
		} catch (final IOException e) {
			e.printStackTrace();
			System.err.println(e.getMessage());
			System.exit(-1);
		} catch (final Exception e) {
			e.printStackTrace();
			System.err.println(e.getMessage());
			System.exit(-1);
		}

	}
}
