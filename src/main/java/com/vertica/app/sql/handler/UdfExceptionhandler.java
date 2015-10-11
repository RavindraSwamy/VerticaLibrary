package com.vertica.app.sql.handler;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.sdk.UdfException;

/**
 * When a {@link SQLException} is caught, a new {@link UdfException} exception
 * is created with the original cause attached and the chain of exception is
 * thrown up to a higher level by the handler.
 *
 * @author Abhishek Chaudhary
 *
 */
public class UdfExceptionhandler {

	private static transient Logger _log;


	/**
	 * Logs the error with custom message Does not Rollback the
	 * <code> connection </connection>
	 *
	 * @param message
	 * @param e
	 * @throws UdfException
	 */
	public void handle(final String message, final SQLException e)
			throws UdfException {
		_log.error(message, e);
		throw new UdfException(e.getErrorCode(), e.getMessage());
	}

	/**
	 * Logs the error with custom message Performs a rollback and closes the
	 * connection <code> connection </connection
	 *
	 * @param message
	 * @param e
	 * @throws UdfException
	 */
	public void handle(final Connection cn, final String message,
			final SQLException e) throws UdfException {
		_log.error(message, e);
		try {
			ConnectionManager.rollbackAndClose(cn);
		} catch (SQLException ee) {
			_log.error("Error occured while closing the connection ", ee);
		}
		throw new UdfException(e.getErrorCode(), e.getMessage());
	}

	/**
	 * logs the error with {@link SQLException} Performs a rollback and closes
	 * the connection <code> connection </connection
	 *
	 * @param cn
	 * @param e
	 * @throws UdfException
	 */
	public void handle(final Connection cn, final SQLException e, final Logger log)
			throws UdfException {
		try {
			_log = log;
			_log.error("Error occured while executing UDX ...", e);
			ConnectionManager.rollbackAndClose(cn);
		} catch (SQLException ee) {
			_log.error("Error occured while closing the connection ", ee);
		}
		throw new UdfException(e.getErrorCode(), e.getMessage());
	}

}