package edu.scripps.yates.ip2tomassive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dataset {
	private final String name;
	private final Map<FileType, List<String>> pathsByFileType = new HashMap<FileType, List<String>>();
	private final Map<String, String> remoteOutputFileNamesByPath = new HashMap<String, String>();

	public Dataset(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void addPath(FileType fileType, String path) {
		if (!pathsByFileType.containsKey(fileType)) {
			addFileType(fileType);
		}
		if (!pathsByFileType.get(fileType).contains(path)) {
			pathsByFileType.get(fileType).add(path);
		} else {
			System.out.println("Already there");
		}
	}

	private void addFileType(FileType fileType) {
		pathsByFileType.put(fileType, new ArrayList<String>());
	}

	public void addRemoteOutputFileName(String path, String localFileName) {
		if (remoteOutputFileNamesByPath.containsKey(path)) {
			throw new IllegalArgumentException(path + " is duplicated");
		}
		remoteOutputFileNamesByPath.put(path, localFileName);
	}

	public Map<FileType, List<String>> getPathsByFileType() {
		return pathsByFileType;
	}

	public Map<String, String> getOutputFileNameByPath() {
		return remoteOutputFileNamesByPath;
	}
}
