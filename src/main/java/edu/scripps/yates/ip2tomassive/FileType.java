package edu.scripps.yates.ip2tomassive;

public enum FileType {
	DTASELECT("DTASelect-files", "DTASelect-filter.txt", "txt"), RAW("raw-files", null, "raw"),
	CENSUS_CHRO("census_chro-files", "census_chro.xml", "xml"), OTHER("other-files", null, null),
	MS1("ms1-files", null, "ms1"), MS2("ms2-files", null, "ms2"), MS3("ms3-files", null, "ms3");
	private final String description;
	private final String defaultFileName;
	private final String extension;

	FileType(String description, String defaultFileName, String extension) {
		this.description = description;
		this.defaultFileName = defaultFileName;
		this.extension = extension;
	}

	public String getDescription() {
		return description;
	}

	public static FileType getbyDescription(String description) {
		for (final FileType fileType : values()) {
			if (fileType.getDescription().equalsIgnoreCase(description)) {
				return fileType;
			}
		}
		return null;
	}

	public String getDefaultFileName() {
		return defaultFileName;
	}

	public String getExtension() {
		return extension;
	}
}
