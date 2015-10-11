package com.vertica.util;

import com.vertica.app.sql.engine.ConversionException;

public final class ConversionUtil {

	/**
	 * Converter used when source is a String and destination is a StringBuilder
	 *
	 * @param source
	 * @return
	 * @throws ConversionException
	 */
	public StringBuilder stringToStringBuilder(final String source)
			throws ConversionException {
		return new StringBuilder(source);
	}

	/**
	 * Converter used when source is a String and destination is a StringBuilder
	 *
	 * @param sourceObject
	 * @return
	 * @throws ConversionException
	 */
	public StringBuffer stringToStringBuffer(final String sourceObject)
			throws ConversionException {
		return new StringBuffer(sourceObject);
	}

}
