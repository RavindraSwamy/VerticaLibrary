package com.vertica.app.sql.engine;

public class ConversionException extends Exception{

	private static final long serialVersionUID = -6559189320023789199L;

	public ConversionException(String message) {
		super(message);
	}

	public ConversionException(String message, Throwable cause) {
		super(message, cause);
	}
}
