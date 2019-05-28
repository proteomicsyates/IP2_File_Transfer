package edu.scripps.yates.ip2tomassive;

public enum FileType {
	RAW("raw-files", null), CENSUS_CHRO("census_chro-files", "census_chro.xml"),
	DTASELECT("DTASelect-files", "DTASelect-filter.txt"), OTHER("other-files", null);
	private final String description;
	private final String defaultFileName;

	FileType(String description, String defaultFileName) {
		this.description = description;
		this.defaultFileName = defaultFileName;
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
}
