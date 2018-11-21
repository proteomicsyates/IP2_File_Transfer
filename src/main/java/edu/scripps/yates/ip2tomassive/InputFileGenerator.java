package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.mortbay.log.Log;

import gnu.trove.set.hash.TIntHashSet;

public class InputFileGenerator {
	private final File paramFile;
	private final boolean includeDTASelect;

	public InputFileGenerator(File paramFile, boolean includeDTASelect) {
		this.paramFile = paramFile;
		this.includeDTASelect = includeDTASelect;
	}

	public File run() throws IOException {
		final MySftpProgressMonitor progressMonitor = new MySftpProgressMonitor(System.out);
		final IP2ToMassive ip2Massive = new IP2ToMassive(progressMonitor, paramFile);

		final Properties properties = IP2ToMassive.getProperties(paramFile);
		final String projectBasePath = properties.getProperty(IP2ToMassive.IP2_SERVER_PROJECT_BASE_PATH);
		final String expIDs = properties.getProperty(IP2ToMassive.EXPERIMENT_IDS);

		final TIntHashSet experimentIDs = toIntSet(expIDs);
		if (projectBasePath == null) {
			throw new IllegalArgumentException(
					IP2ToMassive.IP2_SERVER_PROJECT_BASE_PATH + " property is needed in parameters file");
		}
		FileWriter fw = null;
		File ret = null;
		try {
			final String fileName = paramFile.getParentFile().getAbsolutePath() + File.separator
					+ FilenameUtils.getBaseName(paramFile.getAbsolutePath()) + "_paths.txt";
			Log.info("Creating file with remote paths at:  " + fileName);
			ret = new File(fileName);
			fw = new FileWriter(ret);
			fw.write(MultipleProjectIP2ToMassive.DATASET + " " + properties.getProperty(IP2ToMassive.PROJECT_NAME)
					+ "\n");
			// everything is fine until here
			final List<String> experimentPaths = ip2Massive.getExperimentPathsFromIP2(projectBasePath, experimentIDs);
			for (final String experimentPath : experimentPaths) {
				// raws
				final List<String> rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath);
				if (!rawFilesPaths.isEmpty()) {
					fw.write(FileType.RAW.getDescription() + "\n");
					for (final String rawFile : rawFilesPaths) {
						fw.write(rawFile + "\n");
					}
				}
				if (includeDTASelect) {
					// dtaselect
					final List<Search> dtaSelectFilesPathsAndParameterString = ip2Massive
							.getDTASelectPathsAndParameters(experimentPath);
					if (!dtaSelectFilesPathsAndParameterString.isEmpty()) {
						fw.write(FileType.DTASELECT.getDescription() + "\n");
						for (final Search search : dtaSelectFilesPathsAndParameterString) {
							final String parameters = search.getParameters();
							final String dtaSelectPath = search.getPath();
							final String newFileName = FilenameUtils.getBaseName(dtaSelectPath) + "_" + search.getId()
									+ ".txt";
							fw.write(dtaSelectPath + "\t" + newFileName + "\t" + parameters + "\n");
						}
					}
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			fw.close();
		}
		return ret;
	}

	private TIntHashSet toIntSet(String expIDs) {
		try {
			final TIntHashSet ret = new TIntHashSet();
			if (expIDs.contains(",")) {
				final String[] split = expIDs.split(",");
				for (final String string : split) {
					ret.add(Integer.valueOf(string.trim()));
				}
			} else {
				ret.add(Integer.valueOf(expIDs.trim()));
			}
			return ret;
		} catch (final NumberFormatException e) {
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
			final InputFileGenerator inputFileGenerator = new InputFileGenerator(paramFile, false);

		} catch (final Exception e) {
			System.exit(-1);
		}
	}
}
