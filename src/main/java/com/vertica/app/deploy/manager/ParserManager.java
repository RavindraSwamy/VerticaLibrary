package com.vertica.app.deploy.manager;

import static com.vertica.app.deploy.manager.UploadManager.uploadUDXParser;

import java.sql.Connection;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;

public class ParserManager {

	public static void createParser(final String userName){
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,UDXLogFactory.getGenericlogger());
			try {
				executor.update("DROP LIBRARY PUBLIC.MultiCharDelimiterParser CASCADE");
				System.out.println("Dropped previously deployed parser");
			} catch (Exception e) {
				System.err.println("No parser library found ...");
			}
			uploadUDXParser(userName);
			System.out.println("started deploying parser ...");
			String createLibrary = "create library MultiCharDelimiterParser as '/vertica_load/deploy/udx/MultiCharDelimiterParser.jar' depends '/data/" + userName + "/udx/lib/*' language 'java'";
			System.out.println(createLibrary);
			executor.update(createLibrary);
			executor.update("create parser MultiCharDelimiterParser AS LANGUAGE 'JAVA' NAME 'com.vertica.app.parser.MultiCharDelimiterParserFactory' LIBRARY MultiCharDelimiterParser FENCED;");
			System.out.println("Created requested parser ...");
			PermissionManager.grantParserPermission();
			System.out.println("Granted permissions for using parser ...");
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
	
}
