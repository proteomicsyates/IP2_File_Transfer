package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import gnu.trove.set.hash.TIntHashSet;

public class InputFileGenerator {
	private static final Logger log = Logger.getLogger(InputFileGenerator.class);
	private final File paramFile;
	private final boolean includeDTASelect;
	private final boolean takeEverythingBaseFolder;
	private final boolean getMS1 = true;
	private final boolean getMS2 = true;
	private final boolean getMS3 = true;

	/**
	 * 
	 * @param paramFile
	 * @param includeDTASelect
	 * @param takeEverythingBaseFolder if true, it looks recursively for the files
	 *                                 from the basePath, otherwise, it uses the
	 *                                 experiment ids
	 */
	public InputFileGenerator(File paramFile, boolean includeDTASelect, boolean takeEverythingBaseFolder) {
		this.paramFile = paramFile;
		this.includeDTASelect = includeDTASelect;
		this.takeEverythingBaseFolder = takeEverythingBaseFolder;
	}

	public File run() throws IOException {
		final MySftpProgressMonitor progressMonitor = new MySftpProgressMonitor(System.out);
		final IP2ToMassive ip2Massive = new IP2ToMassive(progressMonitor, paramFile);

		final Properties properties = IP2ToMassive.getProperties(paramFile);
		final String projectBasePath = properties.getProperty(IP2ToMassive.IP2_SERVER_PROJECT_BASE_PATH);
		final String expIDs = properties.getProperty(IP2ToMassive.EXPERIMENT_IDS);

		final TIntHashSet experimentIDs = toIntSet(expIDs);
		if (!takeEverythingBaseFolder && projectBasePath == null) {
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
			log.info(experimentPaths.size() + " experiment paths retrieved");
			int numRaws = 0;
			int numMS1s = 0;
			int numMS2s = 0;
			int numMS3s = 0;
			int numDTASelects = 0;
			for (final String experimentPath : experimentPaths) {
				log.info("Looking into path: " + experimentPath);
				// raws
				List<String> rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath, "raw");
				if (!rawFilesPaths.isEmpty()) {
					fw.write(FileType.RAW.getDescription() + "\n");
					for (final String rawFile : rawFilesPaths) {
						fw.write(rawFile + "\n");
						numRaws++;
					}
				}
				// ms1
				if (getMS1) {
					rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath, "ms1");
					if (!rawFilesPaths.isEmpty()) {
						fw.write(FileType.MS1.getDescription() + "\n");
						for (final String rawFile : rawFilesPaths) {
							fw.write(rawFile + "\n");
							numMS1s++;
						}
					}
				}
				// ms2
				if (getMS2) {
					rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath, "ms2");
					if (!rawFilesPaths.isEmpty()) {
						fw.write(FileType.MS2.getDescription() + "\n");
						for (final String rawFile : rawFilesPaths) {
							fw.write(rawFile + "\n");
							numMS2s++;
						}
					}
				}
				// ms3
				if (getMS3) {
					rawFilesPaths = ip2Massive.getRawFilesPaths(experimentPath, "ms3");
					if (!rawFilesPaths.isEmpty()) {
						fw.write(FileType.MS3.getDescription() + "\n");
						for (final String rawFile : rawFilesPaths) {
							fw.write(rawFile + "\n");
							numMS3s++;
						}
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
							numDTASelects++;
						}
					}
				}
				log.info("Number of raw files so far: " + numRaws);
				log.info("Number of ms1 files so far: " + numMS1s);
				log.info("Number of ms2 files so far: " + numMS2s);
				log.info("Number of ms3 files so far: " + numMS3s);
				log.info("Number of dtaselect files so far: " + numDTASelects);
			}
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			fw.close();
		}
		return ret;
	}

	private TIntHashSet toIntSet(String expIDs) {
		if (expIDs == null || "".equals(expIDs)) {
			return null;
		}
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
			final InputFileGenerator inputFileGenerator = new InputFileGenerator(paramFile, true, true);
			inputFileGenerator.run();
			System.out.println("everything ok");
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
