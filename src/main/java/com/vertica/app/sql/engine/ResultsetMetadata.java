package com.vertica.app.sql.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Generates an object that can be used to get information about the types and
 * properties of the columns in a ResultSet object.
 *
 * @author achaudhary
 *
 */
public class ResultsetMetadata {
	private static Connection conn;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*conn = ConnectionManager.getConnection();
		try {
			// Constructor removed.
			QueryExecutor qe = new QueryExecutor(conn);
			String sql = "select * from test";
			long start = System.currentTimeMillis();
			ResultSet rs = qe.query(sql);
			System.out.println("Statement "
					+ (System.currentTimeMillis() - start));
			getResultsetMetadata(rs);
			new ResultsetMetadata().printClassNames();
		} catch (SQLException e) {
			e.printStackTrace();
		}*/
	}

	public static void getResultsetMetadata(ResultSet rs) throws SQLException {
		// ResultSetMetadata is accessible from ResultSet
		ResultSetMetaData metaData = rs.getMetaData();
		System.out.println("Total number of columns "
				+ metaData.getColumnCount());
		for (int i = 0; i < metaData.getColumnCount(); i++) {
			System.out.println("Column at index " + i + " is "
					+ metaData.getColumnLabel(i + 1).toUpperCase());
			System.out.println("Column Type " + " is "
					+ metaData.getColumnTypeName((i + 1)));
		}
	}

	private void printClassNames() throws SQLException {
		printClassName(conn);
		Statement s = conn.createStatement();
		printClassName(s);
		PreparedStatement prepareStatement = conn.prepareStatement("");
		printClassName(prepareStatement);
	}

	public void printClassName(Object clazz) {
		System.out.println(clazz.toString() + "------------------------"
				+ clazz.getClass().getName());
		Method[] methods = clazz.getClass().getMethods();
		Field[] fields = clazz.getClass().getFields();
		System.out.println("Method Names");
		for (int i = 0; i < methods.length; i++) {
			// System.out.println(methods[i].getName());
		}
		System.out.println("Field Names");
		for (int i = 0; i < fields.length; i++) {
			// System.out.println(fields[i].getName());
		}
	}

}
