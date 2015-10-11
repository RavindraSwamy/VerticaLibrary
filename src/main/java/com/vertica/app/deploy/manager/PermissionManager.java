package com.vertica.app.deploy.manager;
import java.sql.Connection;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;

public class PermissionManager {

		public static void grantPermissions() {
			System.out.println("Started granting permissions ...");

			// 1st argument refers to schema to which a procedure belongs
			// 2nd argument refers to user who needs access on that schema

			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "FIN_RAPG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("PUBLIC", "RECONP");
			
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("RECONP", "FIN_RAPG");

			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("IBG", "FIN_RAPG");

			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("AUDIT", "FIN_RAPG");

			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("EDW", "FIN_RAPG");

			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "AUDIT");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("QDESK", "FIN_RAPG");

			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "FIN_RAPG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "QDESK");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "EDW");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "IBG");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "RECONP");
			PermissionManager.grantExecuteOnAllFunctionsInSchema("FIN_RAPG", "AUDIT");

			System.out.println("Finished granting permissions ...");

		}
		
		public static void grantLibraryPermission(final String library_name) {
			try (Connection cn = ConnectionManager.getConnection()) {
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				// GRANT USAGE ON LIBRARY PUBLIC.SAMPLE to EDW;
				// GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA zero-schema TO Bob;
				executor.update("GRANT USAGE ON LIBRARY " + library_name
						+ " TO " + "EDW");
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		public static void grantExecuteOnAllFunctionsInSchema(final String schema_name, final String user_name) {
			try (Connection cn = ConnectionManager.getConnection()) {
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				// GRANT USAGE ON LIBRARY PUBLIC.SAMPLE to EDW;
				executor.update("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA " + schema_name + " TO " + user_name);
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		public static void grantFunctionPermission(final String library_name) {
			try (Connection cn = ConnectionManager.getConnection()) {
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				// GRANT USAGE ON LIBRARY PUBLIC.SAMPLE to EDW;
				executor.update("GRANT EXECUTE ON FUNCTION " + library_name
						+ " TO " + "EDW");
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		public static void grantParserPermission() {
			try (Connection cn = ConnectionManager.getConnection()) {
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				executor.update("GRANT EXECUTE ON PARSER "
						+ "MultiCharDelimiterParser()" + " TO " + "EDW, RECONP, IBG, AUDIT, QDESK, FIN_RAPG");
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		public static void createAndGrantStorageLocation(){
			try (Connection cn = ConnectionManager.getConnection()) {
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				executor.update("CREATE LOCATION '/vertica_load' ALL NODES USAGE 'USER'");
				executor.update("GRANT READ ON LOCATION '/vertica_load' TO edw");
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}