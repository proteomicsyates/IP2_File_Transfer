package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import gnu.trove.set.hash.TIntHashSet;

public class InputFileGenerator {
	private final File paramFile;

	public InputFileGenerator(File paramFile, boolean includeDTASelect)
			throws IOException, JSchException, SftpException {
		this.paramFile = paramFile;
		MySftpProgressMonitor progressMonitor = new MySftpProgressMonitor(System.out);
		IP2ToMassive ip2Massive = new IP2ToMassive(progressMonitor, paramFile);

		Properties properties = IP2ToMassive.getProperties(paramFile);
		String projectBasePath = properties.getProperty(IP2ToMassive.IP2_SERVER_PROJECT_BASE_PATH);
		String expIDs = properties.getProperty(IP2ToMassive.EXPERIMENT_IDS);

		TIntHashSet experimentIDs = toIntSet(expIDs);
		if (projectBasePath == null) {
			throw new IllegalArgumentException(
					IP2ToMassive.IP2_SERVER_PROJECT_BASE_PATH + " property is needed in parameters file");
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(paramFile.getParentFile().getAbsolutePath() + File.separator
					+ FilenameUtils.getBaseName(paramFile.getAbsolutePath()) + "_paths.txt");
			fw.write(MultipleProjectIP2ToMassive.DATASET + " " + properties.getProperty(IP2ToMassive.PROJECT_NAME)
					+ "\n");
			// everything is fine until here
			List<String> experimentPaths = ip2Massive.getExperimentPathsFromIP2(projectBasePath, experimentIDs);
			for (String experimentPath : experimentPaths) {
				// raws
				List<String> rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath);
				if (!rawFilesPaths.isEmpty()) {
					fw.write(FileType.RAW.getDescription() + "\n");
					for (String rawFile : rawFilesPaths) {
						fw.write(rawFile + "\n");
					}
				}
				if (includeDTASelect) {
					// dtaselect
					List<Search> dtaSelectFilesPathsAndParameterString = ip2Massive
							.getDTASelectPathsAndParameters(experimentPath);
					if (!dtaSelectFilesPathsAndParameterString.isEmpty()) {
						fw.write(FileType.DTASELECT.getDescription() + "\n");
						for (Search search : dtaSelectFilesPathsAndParameterString) {
							String parameters = search.getParameters();
							String dtaSelectPath = search.getPath();
							String newFileName = FilenameUtils.getBaseName(dtaSelectPath) + "_" + search.getId()
									+ ".txt";
							fw.write(dtaSelectPath + "\t" + newFileName + "\t" + parameters + "\n");
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fw.close();
		}
	}

	private TIntHashSet toIntSet(String expIDs) {
		try {
			TIntHashSet ret = new TIntHashSet();
			if (expIDs.contains(",")) {
				final String[] split = expIDs.split(",");
				for (String string : split) {
					ret.add(Integer.valueOf(string.trim()));
				}
			} else {
				ret.add(Integer.valueOf(expIDs.trim()));
			}
			return ret;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(IP2ToMassive.EXPERIMENT_IDS
					+ " property is malformed. Allowed format is experiment IDs as integers separated by commas.");
		}
	}

	public static void main(String[] args) {
		try {
			File paramFile = null;
			if (args.length == 1) {
				paramFile = new File(args[0]);
			} else {
				throw new IllegalArgumentException(
						"One and only one parameter is accepted, which is the input parameter file");
			}
			InputFileGenerator inputFileGenerator = new InputFileGenerator(paramFile, false);

		} catch (Exception e) {
			System.exit(-1);
		}
	}
}
