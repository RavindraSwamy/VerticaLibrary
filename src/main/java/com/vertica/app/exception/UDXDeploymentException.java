package com.vertica.app.exception;

public class UDXDeploymentException extends Exception {

	private static final long serialVersionUID = -5508078272466994573L;

	public UDXDeploymentException(final String message) {
		super(message);
	}

	public UDXDeploymentException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
