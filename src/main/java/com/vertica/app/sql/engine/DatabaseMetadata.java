package com.vertica.app.sql.engine;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Gives comprehensive information about the database as a whole.
 *
 * @author achaudhary
 *
 */
public class DatabaseMetadata {
	private static Connection conn;

	public static void main(String[] args) {
		conn = ConnectionManager.getConnection();
		try {
			getDatabaseMetadata();
			ConnectionManager.close(conn);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Retrieves a DatabaseMetaData object that contains metadata about the
	 * database to which this Connection object represents a connection. The
	 * metadata includes information about the database's tables, its supported
	 * SQL grammar, its stored procedures, the capabilities of this connection,
	 * and so on.
	 *
	 *
	 * @throws SQLException
	 */
	private static void getDatabaseMetadata() throws SQLException {
		DatabaseMetaData metaData = conn.getMetaData();
		// System.out.println(metaData.getSQLKeywords());
		System.out.println(metaData.getDriverName());
		System.out.println(metaData.getDriverVersion());
		System.out.println(metaData.getUserName());
		String[] table = { "TABLE" };
		ResultSet tables = metaData.getTables(null, null, null, table);
		while (tables.next()) {
			System.out.println(tables.getString(3));
		}
	}
}
