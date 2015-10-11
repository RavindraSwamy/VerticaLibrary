package com.vertica.app.precompiler;

class QueryFormatter {

	/**
	 * Removes unwanted characters at start and end of multi-line query
	 *
	 * @param firstLine
	 * @param lastLine
	 * @param line
	 * @return
	 */

	public static String linearizeQuery(final boolean firstLine,
			final boolean lastLine, String line) {
		if (!firstLine && line.startsWith("+") || line.startsWith("\"")) {
			line = line.substring(1, line.length());
		}
		if (!lastLine && line.endsWith("\" +")) {
			line = line.substring(0, line.lastIndexOf("\" +"));
		}
		if (!lastLine && line.endsWith("\"+")) {
			line = line.substring(0, line.lastIndexOf("\"+"));
		}
		return line;
	}

}
