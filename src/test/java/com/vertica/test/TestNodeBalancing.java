package com.vertica.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.vertica.app.loader.ApplicationProperties;
import com.vertica.sdk.ServerInterface;
import com.vertica.util.ExceptionUtil;

public class TestNodeBalancing {
	private static Connection conn;

	public Connection getDBConnection(final ServerInterface srvInterface) {
		try{

			for (int x=1; x <= 10; x++) {
	            try {
	                System.out.print("Connect attempt #" + x + "...");
	                Class.forName("com.vertica.jdbc.Driver");
	    			Properties myProp = new Properties();
	    			myProp.put("user", ApplicationProperties.getInstance().getValue("user_name"));
	    			myProp.put("password", ApplicationProperties.getInstance().getValue("password"));
	    			final String URL = ApplicationProperties.getInstance().getValue("url") +
	    					ApplicationProperties.getInstance().getValue("database");
	    	        myProp.put("ConnectionLoadBalance", 1);
	    			Connection connection = DriverManager.getConnection(URL, myProp);
	    			connection.setAutoCommit(false);

	                Statement stmt = connection.createStatement();

	                // Query system to table to see what node we are connected to. Assume a single row
	                // in response set.
	                ResultSet rs = stmt.executeQuery("SELECT node_name FROM v_monitor.current_session;");
	                rs.next();
	                System.out.println("Connected to node " + rs.getString(1).trim());
	                connection.close();
	            } catch (SQLException e) {
	                // Catch-all for other exceptions
	                System.out.println("Error!");
	                e.printStackTrace();
	            }
	        }


			//get database metadata
			//getDatabaseMetadata(connection);

			return null;
		}
		catch (ClassNotFoundException e) {
			srvInterface.log(ExceptionUtil.getStringFromException(e), e);
			e.printStackTrace();
		}
		return null;
	}

	public static Connection getConn() {
		return conn;
	}

	public static void setConn(Connection conn) {
		TestNodeBalancing.conn = conn;
	}
}
