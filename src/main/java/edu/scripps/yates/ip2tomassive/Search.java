package edu.scripps.yates.ip2tomassive;

public class Search {
	private final int id;
	private final String path;
	private final String parameters;

	public Search(int id, String path, String parameters) {
		this.id = id;
		this.path = path;
		this.parameters = parameters;
	}

	public int getId() {
		return id;
	}

	public String getPath() {
		return path;
	}

	public String getParameters() {
		return parameters;
	}

}
