package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.IOException;

public class MultiProjectUpload {
	public static void main(String[] args) {
		File propertiesFile = new File(args[0]);
		File remotePathsFile = null;

		if (args.length > 1) {
			remotePathsFile = new File(args[1]);
		}

		MySftpProgressMonitor progressMonitor = new MySftpProgressMonitor(System.out);
		try {
			MultipleProjectIP2ToMassive m = new MultipleProjectIP2ToMassive(progressMonitor, propertiesFile,
					remotePathsFile);
			m.setSubmissionName("CFTR");
			m.transferDatasets();
			System.out.println("PROGRAM FINISHED OK");
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(e.getMessage());
			System.exit(-1);
		}

	}
}
