package com.vertica.app.sql.engine;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.vertica.app.loader.ApplicationProperties;
import com.vertica.util.StringUtil;


class VerticaConnection {


	public Connection getDBConnection() {
		Connection connection = null;
		try {
			
			long startTime = System.currentTimeMillis();
			Class.forName("com.vertica.jdbc.Driver");

			//Set properties
			final Properties myProp = new Properties();
			myProp.put("user", ApplicationProperties.getInstance().getValue("pwr_user_name"));
			// UDXLogFactory.getEDWLogger().info("User Name: " + ApplicationProperties.getInstance().getValue("user_name"));
			myProp.put("password", ApplicationProperties.getInstance().getValue("pwr_password"));
			// UDXLogFactory.getEDWLogger().info("Password: " + ApplicationProperties.getInstance().getValue("password"));
			final String URL = ApplicationProperties.getInstance().getValue("url") + ApplicationProperties.getInstance().getValue("database")  + "?characterEncoding=UTF-8";
			myProp.put("ConnectionLoadBalance", 1);
			myProp.put("characterEncoding", "UTF-8");
			connection = DriverManager.getConnection(URL, myProp);
			connection.setAutoCommit(false);
			System.out.println("Time required to create JDBC connection: " + (System.currentTimeMillis() - startTime));
			// Get node name anchoring the connection
			
			/*Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("select node_name from v_monitor.current_session;");
			rs.next();
			System.out.println("Connected to node: " + rs.getString(1).trim());*/
			
		} catch (final SQLException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}catch (final ClassNotFoundException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}
		return connection;
	}


	public Connection getDBConnectionWithSearchPath(String searchPath) {

		// set default search path
		if (!StringUtil.isToken(searchPath)){
			searchPath = "public";
		}

		Connection connection = null;
		try {
			Class.forName("com.vertica.jdbc.Driver");

			//Set properties
			final Properties myProp = new Properties();
			myProp.put("user", ApplicationProperties.getInstance().getValue("user_name"));
			myProp.put("password", ApplicationProperties.getInstance().getValue("password"));
			final String URL = ApplicationProperties.getInstance().getValue(
					"url")
					+ ApplicationProperties.getInstance().getValue("database") +   "?characterEncoding=UTF-8"
					+ "?searchpath=" + searchPath;
			myProp.put("ConnectionLoadBalance", 1);

			connection = DriverManager.getConnection(URL, myProp);
			connection.setAutoCommit(false);

		} catch (final SQLException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}catch (final ClassNotFoundException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}

		return connection;
	}


	public Connection getDBConnection(String schema_name) throws SQLException{

		Connection connection = null;
		try {
			Class.forName("com.vertica.jdbc.Driver");

			//Set properties
			final Properties myProp = new Properties();
			myProp.put("user", ApplicationProperties.getInstance().getValue(schema_name + "_user_name"));
			myProp.put("password", ApplicationProperties.getInstance().getValue(schema_name + "_password"));
			final String URL = ApplicationProperties.getInstance().getValue("url") + ApplicationProperties.getInstance().getValue("database");
			myProp.put("ConnectionLoadBalance", 1);

			connection = DriverManager.getConnection(URL, myProp);
			connection.setAutoCommit(false);

		} catch (final ClassNotFoundException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}

		return connection;
	}
}