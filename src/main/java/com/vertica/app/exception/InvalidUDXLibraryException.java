package com.vertica.app.exception;

public class InvalidUDXLibraryException extends Exception {

	private static final long serialVersionUID = 7380665988457276753L;

	public InvalidUDXLibraryException(final String message) {
		super(message);
	}

	public InvalidUDXLibraryException(final String message,
			final Throwable cause) {
		super(message, cause);
	}
}
