package edu.scripps.yates.ip2tomassive;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gnu.trove.set.hash.THashSet;

/**
 * After creating a ip2FileTransfer_paths.txt file, we may want to discard some
 * of the files to upload by only keeping the ones in a second file
 * 
 * @author salvador
 *
 */
public class FilePathFilter {
	private final String basePath;
	private final File ip2FileTransfer;
	private final File dtaSelectInclusionFile;

	public FilePathFilter(String basePath, File ip2FileTransfer, File dtaSelectInclusionFile) {
		this.basePath = basePath;
		this.ip2FileTransfer = ip2FileTransfer;
		this.dtaSelectInclusionFile = dtaSelectInclusionFile;
	}

	public static void main(String[] args) {
		final String basePath = args[0];
		final File ip2FileTransfer = new File(args[1]);
		final File dtaSelectInclusionFile = new File(args[2]);
		final FilePathFilter fpf = new FilePathFilter(basePath, ip2FileTransfer, dtaSelectInclusionFile);
		try {
			fpf.run();
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public void run() throws IOException {
		final Set<String> dtaSelectRemotePaths = getDTASelectInclusionPaths(this.dtaSelectInclusionFile);

		final File outputFile = new File(
				this.ip2FileTransfer.getParent() + File.separator + "Filtered_ip2FileTransfer_paths.txt");
		final FileWriter fw = new FileWriter(outputFile);

		final List<String> lines = Files.readAllLines(this.ip2FileTransfer.getAbsoluteFile().toPath());
		boolean dtaSelectLines = false;
		for (final String line : lines) {
			boolean keepLine = true;
			try {
				final FileType fileType = FileType.getbyDescription(line.trim());
				if (fileType != null) {
					if (fileType == FileType.DTASELECT) {
						dtaSelectLines = true;
					} else {
						dtaSelectLines = false;
					}
				} else {
					if (dtaSelectLines) {
						// check if it is in the inclusion set
						String path = line.split("\t")[0];
						path = path.substring(path.indexOf(basePath) + basePath.length());
						if (!dtaSelectRemotePaths.contains(path)) {
							keepLine = false;
						}
					}
				}
			} finally {
				if (keepLine) {
					fw.write(line + "\n");
				}
			}

		}

		fw.close();

	}

	private Set<String> getDTASelectInclusionPaths(File dtaSelectInclusionFile2) throws IOException {
		final Set<String> lines = Files.readAllLines(this.dtaSelectInclusionFile.toPath()).stream()
				.collect(Collectors.toSet());
		final Set<String> ret = new THashSet<String>();
		for (String line : lines) {
			if (line.contains(basePath)) {
				line = line.substring(line.indexOf(basePath) + basePath.length());
				ret.add(line);
			} else {
				ret.add(line);
			}
		}
		return ret;
	}
}
