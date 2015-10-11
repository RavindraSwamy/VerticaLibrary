package com.vertica.app.deploy;

import static com.vertica.app.deploy.manager.FunctionManager.batchCreateFunctionsInSingleConnection;
import static com.vertica.app.deploy.manager.FunctionManager.functionExists;
import static com.vertica.app.deploy.manager.UploadManager.uploadCommonUtility;
import static com.vertica.app.deploy.manager.UploadManager.uploadUDXLibrary;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import com.vertica.app.deploy.manager.PermissionManager;
import com.vertica.app.deploy.manager.UDXValidationManager;
import com.vertica.app.exception.InvalidUDXLibraryException;
import com.vertica.app.exception.UDXDeploymentException;
import com.vertica.app.loader.ApplicationProperties;
import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.builder.QueryBuilder;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.util.ExceptionUtil;

/**
 * 
 * Takes care of deployment of libraries, functions and external procedures
 * @author Abhishek Chaudhary
 *
 */

public class UDXDeploymentManager {

	public static String SCHEMA_NAME = "PUBLIC";

	public static String userName = "vertuat";

	public static String deploy_path = "/vertica_load/udx/deploy/udx/";

	// Jar path on local box
	//Documents\dev_deployment\EDW_PROCEDURES.jar
	public static String LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
	public static String LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_DEV = "C:/Users/BAN57955/Desktop/Documents/deployment/lib/dev/CommonUtility.jar";
	public static String LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_PROD = "C:/Users/BAN57955/Desktop/Documents/deployment/lib/prod/CommonUtility.jar";

	// Library Related Stuff
	public static String LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
	public static String LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";
	public static String SUPPORT_PATH = deploy_path + "lib" + "/*";

	// Function Related Stuff
	public static String FUNCTION_NAME = SCHEMA_NAME + "." + "FINACLE_ACCOUNT_TRANSACTION_AUDIT_IBG";
	public static String FACTORY = "com.vertica.sdk.edw.FinacleAccountTransactionAuditIbgFactory";
	public static boolean NO_ARG_FUNCTION = false;

	 private static String deploymentMode = "ALTER";
	//private static String deploymentMode = "FULL_DEPLOY";

	public static void main(final String[] args) throws UDXDeploymentException, InvalidUDXLibraryException {
		
		if (deploymentMode.equals("ALTER")){
			
			uploadCommonUtility(userName, LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_DEV, LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_PROD);
			
			uploadAndAlterLibrary();

			SCHEMA_NAME = "QDESK";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();

			SCHEMA_NAME = "AUDIT";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();

			SCHEMA_NAME = "IBG";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();

			SCHEMA_NAME = "RECONP";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();

			SCHEMA_NAME = "FIN_RAPG";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();

			SCHEMA_NAME = "EDW";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			uploadAndAlterLibrary();
			
		}else if (deploymentMode.equals("FULL_DEPLOY")){
			
			prepareForNewDeployment();

			massDeploy();

			SCHEMA_NAME = "QDESK";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();

			SCHEMA_NAME = "AUDIT";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();

			SCHEMA_NAME = "IBG";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();

			SCHEMA_NAME = "RECONP";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();

			SCHEMA_NAME = "FIN_RAPG";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();

			SCHEMA_NAME = "EDW";
			LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX = "C:/Users/BAN57955/Desktop/Documents/deployment/" +SCHEMA_NAME + "_PROCEDURES.jar";
			LIBRARY_NAME = SCHEMA_NAME + "." + SCHEMA_NAME + "_PROCEDURES";
			LIBRARY_PATH = deploy_path + SCHEMA_NAME + "_PROCEDURES.jar";

			massDeploy();
		}

		
		System.out.println("Mass UDX deployment successful ...");

		testRunDeployedUDX();
	}

	private static void prepareForNewDeployment() throws UDXDeploymentException {
		System.out.println("Starting deployment ...");
		System.out.println("Current Node: " + ApplicationProperties.getInstance().getValue("url"));
		System.out.println("Current User: " + ApplicationProperties.getInstance().getValue("pwr_user_name"));

		if (!userName.equals(ApplicationProperties.getInstance().getValue("server_user"))){
			throw new UDXDeploymentException("Invalid deployment configuration ...");
		}


		if (!ApplicationProperties.getInstance().getValue("pwr_user_name").equals("vertuat") &&  !ApplicationProperties.getInstance().getValue("pwr_user_name").equals("vertadm")){
			throw new UDXDeploymentException("Not an admin user, aborting deployment ...");
		}

		System.out.println("Uploading recent CommonUtility ...");
		
		uploadCommonUtility(userName, LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_DEV, LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_PROD);
		System.out.println("Uploaded recent CommonUtility successfully...");
		System.out.println("Started UDX Deployment ...");

		try(Connection cn = ConnectionManager.getConnection()) {
			QueryExecutor executor = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());
			executor.truncateTable("public.functions_deployed");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void uploadAndAlterLibrary() throws UDXDeploymentException, InvalidUDXLibraryException {
		uploadUDXLibrary(userName, LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX);
		final UDXDeploymentManager manager = new UDXDeploymentManager();
		manager.manageUDXDeployment(
				false, // create both, library and function
				true, // alter library
				false, // create library only
				false, // create function only
				false, // batch create functions
				false, // Undeploy- first function and then library
				false, // batch drop
				false, // get deployed libraries
				false, // drop function only
				false, // drop library only
				false// grant permissions
				);
	}

	private static void massDeploy() throws UDXDeploymentException,
	InvalidUDXLibraryException {
		final UDXDeploymentManager manager = new UDXDeploymentManager();

		manager.manageUDXDeployment(
				false, // create both, library and function
				false, // alter library
				true, // create library only
				false, // create function only
				false, // batch create functions
				false, // Undeploy- first function and then library
				false, // batch drop
				false, // get deployed libraries
				false, // drop function only
				false, // drop library only
				false// grant permissions
				);

		manager.manageUDXDeployment(
				false, // create both, library and function
				false, // alter library
				false, // create library only
				false, // create function only
				true, // batch create functions
				false, // Undeploy- first function and then library
				false, // batch drop
				false, // get deployed libraries
				false, // drop function only
				false, // drop library only
				false// grant permissions
				);


		manager.manageUDXDeployment(
				false, // create both, library and function
				false, // alter library
				false, // create library only
				false, // create function only
				false, // batch create functions
				false, // Undeploy- first function and then library
				false, // batch drop
				false, // get deployed libraries
				false, // drop function only
				false, // drop library only
				true// grant permissions
				);

		// ParserManager.createParser(userName);
	}



	/**
	 * Create functions in bulk. Get list of all factory classes, convert UDX
	 * name to function name. Check existence of each function before creating
	 * it. If any of the function exists, already, do not skip creation of other
	 * functions.
	 *
	 * @throws UDXDeploymentException
	 * @throws InvalidUDXLibraryException
	 */
	public void batchCreateFunctions() throws UDXDeploymentException,
	InvalidUDXLibraryException {
		// batchCreateFunctionsInMultipleConnections();
		batchCreateFunctionsInSingleConnection(SCHEMA_NAME, LIBRARY_NAME);
	}


	/**
	 * Takes care of UDX deploy-undeploy
	 *
	 * @param create
	 * @param alter
	 * @param libraryOnly
	 * @param functionOnly
	 * @param bulkFunction
	 * @param undeploy
	 * @param batchDrop
	 * @param getDeployedLibs
	 * @param dropFunctionOnly
	 * @param dropIndependentLibrary
	 * @throws UDXDeploymentException
	 * @throws InvalidUDXLibraryException
	 */
	public void manageUDXDeployment(final boolean create, final boolean alter,
			final boolean libraryOnly, final boolean functionOnly,
			final boolean bulkFunction, final boolean undeploy,
			final boolean batchDrop, final boolean getDeployedLibs,
			final boolean dropFunctionOnly, final boolean dropIndependentLibrary, final boolean grantPermissions)
					throws UDXDeploymentException, InvalidUDXLibraryException {
		if (create || alter || libraryOnly || functionOnly || bulkFunction)
			deployUDX(create, alter, libraryOnly, functionOnly, bulkFunction);
		if (undeploy)
			undeployUDX();
		if (getDeployedLibs)
			getDeployedLibraries();
		if (batchDrop)
			batchLibraryDrop();
		if (dropFunctionOnly)
			dropFunction();
		if (dropIndependentLibrary)
			dropLibrary(false);
		if (grantPermissions)
			PermissionManager.grantPermissions();
	}

	private void undeployUDX() throws UDXDeploymentException {
		dropFunction();
		dropLibrary(false);
	}

	private void batchLibraryDrop() throws UDXDeploymentException {
		final HashSet<String> librariesToBeDropped = new HashSet<>(
				Arrays.asList(LIBRARY_NAME));
		for (final Iterator<String> iterator = librariesToBeDropped.iterator(); iterator
				.hasNext();) {
			final String library_name = iterator.next();
			LIBRARY_NAME = library_name;
			dropLibrary(true);
		}
		System.out.println("All specificed libraries dropped successfully ...");
	}

	private void deployUDX(final boolean create, final boolean alter,
			final boolean libraryOnly, final boolean functionOnly,
			final boolean bulkFunction) throws InvalidUDXLibraryException,
			UDXDeploymentException {

		try {
			UDXValidationManager
			.validate(
					FACTORY,
					UDXDeploymentManager.LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX);
		} catch (final InvalidUDXLibraryException e) {
			throw new InvalidUDXLibraryException(e.getMessage());
		}

		try {
			if (alter)
				alterLibrary();
			if (create || libraryOnly)
				createLibrary();
			if (create || functionOnly)
				createFunction();
			if (bulkFunction)
				batchCreateFunctions();
		} catch (final UDXDeploymentException e) {
			throw new UDXDeploymentException(e.getMessage());
		}
	}

	public static void testRunDeployedUDX() {
		System.out.println(".");
		System.out.println(".");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			// executor.update("create table test_deployment(col1 int);");
			executor.query("select public.sp_truncate_table('test_deployment')");			

			System.out.println("Test run was successful ...");
		} catch (final Exception e) {
			System.out.println("Test run failed...");
			e.printStackTrace();
		}
	}




	/**
	 * @Permissions creates a new library Must be a superuser to create or drop
	 *              a library
	 *
	 * @throws UDXDeploymentException
	 */
	public final String createLibrary() throws UDXDeploymentException {

		if (libraryExists()) {
			dropLibrary(true);
			//throw new UDXDeploymentException(
			//		"Library with same name already exists ...");
		}

		// WARNING: DO NOT UPLOAD LIBRARIES ON PRODUCTION DIRECTLY. 
		// EITHER MOVE LIBS FROM DEV or DO A MANUAL UPLOAD

		uploadUDXLibrary(userName, LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX);

		/*if (userName.equals("vertadm") || userName.equals("vertuat")){
			System.out.println("Please create library explicitly using below statement");
			final String query = QueryBuilder.createLibrary(LIBRARY_NAME,LIBRARY_PATH, SUPPORT_PATH);
			System.out.println(query);

			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}else*/
		System.out.println("Started creating " + LIBRARY_NAME + " library ...");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String query = QueryBuilder.createLibrary(LIBRARY_NAME,
					LIBRARY_PATH, SUPPORT_PATH);
			System.out.println(query);
			executor.update(query);
			System.out.println("Library " + LIBRARY_NAME
					+ " create successfully ...");
			return LIBRARY_NAME;
		} catch (final Exception e) {
			throw new UDXDeploymentException(ExceptionUtil.getStringFromException(e));
		}
	}

	/**
	 * Replaces an existing library
	 *
	 */
	public String alterLibrary() throws UDXDeploymentException {

		if (!libraryExists()) {
			throw new UDXDeploymentException("No such library ...");
		}
		uploadUDXLibrary(userName, LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX);

		System.out.println("Started altering " + LIBRARY_NAME + " library ...");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String query = QueryBuilder.alterLibrary(LIBRARY_NAME,
					LIBRARY_PATH, SUPPORT_PATH);
			executor.update(query);
			System.out.println("Library " + LIBRARY_NAME
					+ " altered successfully ...");
			return LIBRARY_NAME;
		} catch (final Exception e) {
			throw new UDXDeploymentException("Error creating library ...");
		}
	}

	/**
	 * @Permissions To CREATE a function, the user must have CREATE privilege on
	 *              the schema to contain the function and USAGE privilege on
	 *              the library containing the function. To use a function, the
	 *              user must have USAGE privilege on the schema that contains
	 *              the function and EXECUTE privileges on the function.
	 *
	 *              Library must have been previously loaded using the CREATE
	 *              LIBRARY statement.
	 *
	 *
	 * @throws UDXDeploymentException
	 */
	public final String createFunction() throws UDXDeploymentException {
		if (functionExists(FUNCTION_NAME)) {
			throw new UDXDeploymentException(
					"Function with same already exists...");
		}
		System.out.println("Started creating " + FUNCTION_NAME
				+ " function ...");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String query = QueryBuilder.createFunction(FUNCTION_NAME,
					FACTORY, LIBRARY_NAME);
			System.out.println(query);

			try {
				executor.update(query);
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("Function " + FUNCTION_NAME
					+ " create successfully ...");


			return FUNCTION_NAME;
		} catch (final Exception e) {
			throw new UDXDeploymentException(ExceptionUtil.getStringFromException(e));
		}
	}


	/**
	 * @Permissions
	 *
	 *              To DROP a function, the user must either be a superuser, the
	 *              owner of the function, or the owner of the schema which
	 *              contains the function create a function from library
	 *
	 * @throws UDXDeploymentException
	 */
	public void dropFunction() throws UDXDeploymentException {

		if (!functionExists(FUNCTION_NAME)) {
			throw new UDXDeploymentException("No such function ...");
		}

		System.out
		.println("Warning ...\nPlease verify if function has any input arguments ...\n \nDropping function "
				+ FUNCTION_NAME + " ...");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String args = executor
					.getStringValueFromQuery(getFunctionSignature());
			final String query = QueryBuilder.dropFunction(FUNCTION_NAME, args);
			executor.update(query);
			System.out
			.println("Dropped " + FUNCTION_NAME + " successfully ...");
		} catch (final Exception e) {
			throw new UDXDeploymentException(ExceptionUtil.getStringFromException(e));
		}
	}

	/**
	 * Get signature of deployed function
	 *
	 * @return
	 */
	private String getFunctionSignature() {
		final String query = "SELECT FUNCTION_ARGUMENT_TYPE FROM USER_FUNCTIONS WHERE FUNCTION_NAME =  "
				+ "'"
				+ FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)
				+ "' AND SCHEMA_NAME="
				+ "'"
				+ FUNCTION_NAME.substring(0, FUNCTION_NAME.indexOf(".")) + "'";
		System.out.println(query);
		return query;
	}

	/**
	 * @Permissions Must be a superuser to create or drop a library
	 *
	 * @param cascade
	 * @throws UDXDeploymentException
	 */
	public void dropLibrary(final boolean cascade)
			throws UDXDeploymentException {

		if (!libraryExists()) {
			throw new UDXDeploymentException("No such library ...");
		}

		System.out.println("\n \nDropping library " + LIBRARY_NAME + " ...");
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String query = QueryBuilder
					.dropLibrary(cascade, LIBRARY_NAME);
			executor.update(query);
			System.out.println("Dropped " + LIBRARY_NAME + " successfully ...");
		} catch (final Exception e) {
			if (ExceptionUtil.getBriefException(e).contains("3855")) {
				System.err.println("Library not found ...");
			} else {
				throw new UDXDeploymentException("Error dropping library ..." + ExceptionUtil.getStringFromException(e));
			}
		}
	}

	/**
	 * Get list of all deployed libraries
	 *
	 * @throws UDXDeploymentException
	 */
	public void getDeployedLibraries() throws UDXDeploymentException {
		System.out.println("\n\n");
		System.out.println();
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final String query = "SELECT LIB_NAME, LIB_FILE_NAME, OWNER_ID FROM USER_LIBRARIES";
			final ResultSet resultSet = executor.query(query);
			while (resultSet.next()) {
				System.out.println(resultSet.getString(1) + "\t"
						+ resultSet.getString(2) + "\t"
						+ resultSet.getString(3));
			}
		} catch (final Exception e) {
			throw new UDXDeploymentException(
					"Error occured while fetching existing libraries ...");
		}
	}

	/**
	 * Verify whether a library exists already
	 *
	 * @return
	 *
	 */
	public boolean libraryExists() {
		final StringBuilder sb = new StringBuilder();
		final HashSet<String> tables = new HashSet<String>(
				Arrays.asList("USER_LIBRARIES"));
		final StringBuilder selectColumns = new StringBuilder("LIB_NAME");

		// schema name is always stored in lower case
		final StringBuilder whereConditions = new StringBuilder("LIB_NAME= "
				+ "'"
				+ LIBRARY_NAME.substring(LIBRARY_NAME.indexOf(".") + 1)
				+ "' AND SCHEMA_NAME IN ("
				+ "'"
				+ LIBRARY_NAME.substring(0, LIBRARY_NAME.indexOf("."))
				.toLowerCase() + "', '" + LIBRARY_NAME.substring(0, LIBRARY_NAME.indexOf("."))
				.toUpperCase() + "')");

		final String query = QueryBuilder.makeSelect(sb, null, tables, null,
				selectColumns, whereConditions, null);
		System.out.println(query);
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			final ResultSet rs = executor.query(query);
			if (rs.next())
				return true;
		} catch (final Exception e) {
			return false;
		}
		return false;
	}
}
