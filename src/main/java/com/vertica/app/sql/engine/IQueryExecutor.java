package com.vertica.app.sql.engine;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.vertica.sdk.UdfException;

public interface IQueryExecutor {

	public int update(final String query, final Object... params)
			throws UdfException;

	public ResultSet query(String query, final Object... params)
			throws UdfException, SQLException;

}
