package com.vertica.app.sql.engine;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.vertica.app.logging.UDXLogFactory;

public class GetSybaseObjects {

	public static void main(String[] args) throws SQLException {
		Set<String> object_list = getVerticaObjects();
		process(object_list);
	}

	public static Connection process(final Set<String> object_list) {
		Connection connection = null;
		try {
			connection = conenctToSybase();
			// Get node name anchoring the connection
			Statement stmt = connection.createStatement();
			
			ResultSet rs = stmt.executeQuery("select user_name(creator), sysprocedure.* from sysprocedure join sysobject where sysprocedure.object_id = sysobject.object_id and sysobject.creation_time > '2015-03-18' and user_name(creator) in ('edw','reconp')");
			while(rs.next()){
				writeToFile(rs.getString(1) + "." + rs.getString(5), rs.getString(6));
			}
			
			
			/*for (Iterator<String> iterator = object_list.iterator(); iterator.hasNext();) {
				String object_name = (String) iterator.next();
				// ResultSet rs = stmt.executeQuery("select user_name(creator), * from sysprocedure where user_name(CREATOR) in ('abduld','ajaym','AJAYM','amolk','AUDIT','CAMP','camp','DBA','dba','dwh_appl','EDW','edw','FIN_RAPG','IBG','janhavip','nileshm','nitinp','P2C','p2c','priyanks','priyankS','qdesk','QDESK','RECONP','reconp','saikrishnam','sandeepk','SANDEEPK','shaileshs','sudhanshus','sunilm','swapnak','sybint','the','vinayakd') and proc_name='"+object_name.substring(object_name.indexOf(".") + 1) +"'");
				ResultSet rs = stmt.executeQuery("select user_name(creator), sysprocedure.* from sysprocedure join sysobject where sysprocedure.object_id = sysobject.object_id and sysobject.creation_time > '2015-05-22' and user_name(creator) in ('edw','reconp')");
				while(rs.next()){
					writeToFile(rs.getString(1) + "." + rs.getString(5), rs.getString(6));
				}
			}*/
			System.out.println("Finished ...");
		} catch (final SQLException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}catch (final ClassNotFoundException e) {
			System.out.println("Error!");
			e.printStackTrace();
		}
		return connection;
	}

	private static Connection conenctToSybase() throws ClassNotFoundException,
			SQLException {
		Connection connection;
		long startTime = System.currentTimeMillis();
		Class.forName("com.sybase.jdbc4.jdbc.SybDriver");

		//Set properties
		final Properties myProp = new Properties();
		myProp.put("user", "mig_user");
		// UDXLogFactory.getEDWLogger().info("User Name: " + ApplicationProperties.getInstance().getValue("user_name"));
		myProp.put("password", "icici@321");
		// UDXLogFactory.getEDWLogger().info("Password: " + ApplicationProperties.getInstance().getValue("password"));
		final String URL = "jdbc:sybase:Tds:10.24.187.248:2680?ServiceName=ICICI_NODE03";

		// Sybase UAT
		// final String URL = "jdbc:sybase:Tds:10.50.56.99:3670?ServiceName=ICICI_UAT01";

		connection = DriverManager.getConnection(URL, myProp);
		connection.setAutoCommit(false);
		System.out.println("Time required to create JDBC connection: " + (System.currentTimeMillis() - startTime));
		return connection;
	}

	private static Set<String> getVerticaObjects() throws SQLException {
		QueryExecutor executor = new QueryExecutor(ConnectionManager.getConnection(), UDXLogFactory.getGenericlogger());
		ResultSet resultSet = executor.query("select function_name from functions_deployed");
		Set<String> object_list = new HashSet<String>();

		while(resultSet.next()){
			object_list.add(resultSet.getString(1));
		}
		return object_list;
	}

	private static void writeToFile(final String proc_name, final String proc_defn) {
		try {
			FileOutputStream fos = null;
			BufferedWriter bw = null;
			fos = new FileOutputStream("src/main/resources/sybase_new_procedure_prod/"+proc_name.trim() + ".txt");
			bw = new BufferedWriter(new OutputStreamWriter(fos));
			bw.write(proc_defn.replace("\"", ""));
			bw.newLine();
			bw.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
