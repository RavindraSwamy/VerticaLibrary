package com.vertica.app.deploy.manager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.icici.vertica.entity.UserFunctions;
import com.icici.vertica.entity.UserProcedures;
import com.vertica.app.exception.InvalidUDXLibraryException;
import com.vertica.app.exception.UDXDeploymentException;
import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.builder.QueryBuilder;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.util.DateUtil;

public class FunctionManager {

	
	public static void batchCreateFunctionsInMultipleConnections(final String schema_name, final String library_name) throws UDXDeploymentException {
		final HashMap<String, String> udxsql = new HashMap<>();
		int countOfSuccessfulFunctions = 0;
		System.out.println("Started function creation in batch mode ...");

		// Get factory class names from UDXValidator
		// Validate method of UDXValidator must run before execution of this
		// function
		// Convert UDX name to function name
		// create a map of UDX and function name

		for (final Iterator<String> iterator = UDXValidationManager.finalClassNames
				.iterator(); iterator.hasNext();) {
			final String udxName = iterator.next();
			udxsql.put(
					udxName,
					schema_name + "."
							+ UDXValidationManager.convertUDXNameToSQLName(udxName));
		}

		// start function creation
		for (final Iterator<String> iterator = udxsql.keySet().iterator(); iterator
				.hasNext();) {

			String udx_name = iterator.next();
			try (Connection cn = ConnectionManager.getConnection()) {

				// check if function already exists
				if (functionExists(udxsql.get(udx_name))) {
					throw new UDXDeploymentException("Function "
							+ udxsql.get(udx_name) + "already exists...");
				}

				// if function doesn't exist, start creating a new function
				final QueryExecutor executor = new QueryExecutor(cn,
						UDXLogFactory.getGenericlogger());
				System.out.println("Started creating " + udxsql.get(udx_name)
						+ " function ...");
				final String query = QueryBuilder.createFunction(
						udxsql.get(udx_name), udx_name, library_name);
				System.out.println(query);
				executor.update(query);
				System.out.println("Function " + udxsql.get(udx_name)
						+ " create successfully ...");
				countOfSuccessfulFunctions++;
			} catch (final Exception e) {
				throw new UDXDeploymentException("Error creating function ..."
						+ udxsql.get(udx_name));
			}
		}


		// keep track of total number of factory classes present
		// and total function deployed successfully
		System.out.println("Total number of functions identified: "
				+ UDXValidationManager.finalClassNames.size());
		System.out.println("Total number of functions deployed successfully "
				+ countOfSuccessfulFunctions);
	}
	
	public static void batchCreateFunctionsInSingleConnection(final String schema_name, final String library_name) throws UDXDeploymentException,
	InvalidUDXLibraryException {
		final HashMap<String, String> udxsql = new HashMap<>();
		int countOfSuccessfulFunctions = 0;
		System.out.println("Started function creation in batch mode ...");

		// Get factory class names from UDXValidator
		// Validate method of UDXValidator must run before execution of this
		// function
		// Convert UDX name to function name
		// create a map of UDX and function name

		for (final Iterator<String> iterator = UDXValidationManager.finalClassNames
				.iterator(); iterator.hasNext();) {
			final String udxName = iterator.next();
			udxsql.put(
					udxName,
					schema_name + "."
							+ UDXValidationManager.convertUDXNameToSQLName(udxName));
		}

		// start function creation
		String udx_name = "";
		try (Connection cn = ConnectionManager.getConnection()) {
			final QueryExecutor executor = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());
			
			
			for (final Iterator<String> iterator = udxsql.keySet().iterator(); iterator.hasNext();){
				udx_name = iterator.next();

				// check if function already exists
				if (functionExists(udxsql.get(udx_name))) {
					throw new UDXDeploymentException("Function "
							+ udxsql.get(udx_name) + "already exists...");
				}

				// if function doesn't exist, start creating a new function
				
				System.out.println("Started creating " + udxsql.get(udx_name)
						+ " function ...");
				final String query = QueryBuilder.createFunction(
						udxsql.get(udx_name), udx_name, library_name);
				System.out.println(query);
				executor.update(query);
				System.out.println("Function " + udxsql.get(udx_name)
						+ " create successfully ...");
				
				registerFunction(cn, udxsql.get(udx_name));
				
				countOfSuccessfulFunctions++;

			}
		} catch (final Exception e) {
			throw new UDXDeploymentException("Error creating function ..."
					+ udxsql.get(udx_name));
		}

		// keep track of total number of factory classes present
		// and total function deployed successfully
		System.out.println("Total number of functions identified: "
				+ UDXValidationManager.finalClassNames.size());
		System.out.println("Total number of functions deployed successfully "
				+ countOfSuccessfulFunctions);

	}
	
	/**
	 * Verify if a function exists already
	 *
	 * @param function_name
	 * @return
	 */
	public static boolean functionExists(final String function_name) {
		final StringBuilder sb = new StringBuilder();
		final HashSet<String> tables = new HashSet<String>(
				Arrays.asList("USER_FUNCTIONS"));
		final StringBuilder selectColumns = new StringBuilder("FUNCTION_NAME");
		final StringBuilder whereConditions = new StringBuilder(
				"FUNCTION_NAME= "
						+ "'"
						+ function_name.substring(function_name.indexOf(".") + 1)
						+ "' AND SCHEMA_NAME= "
						+ "'"
						+ function_name.substring(0, function_name.indexOf("."))
						+ "'");
		final String query = QueryBuilder.makeSelect(sb, null, tables, null,
				selectColumns, whereConditions, null);
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
	
	private static void registerFunction(Connection cn, String FUNCTION_NAME) {
		try{
			final QueryExecutor executor = new QueryExecutor(cn,
					UDXLogFactory.getGenericlogger());
			
			String object_type = getObjectType(FUNCTION_NAME);
			
			final String query = "insert into public.functions_deployed(function_name,object_type, deploy_time) values (?,?, ?)";
			System.out.println(query);

			try {
				executor.update(query, FUNCTION_NAME,object_type ,DateUtil.getCurrentDate(cn));
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("Function " + FUNCTION_NAME
					+ " create successfully ...");
			cn.commit();
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	private static String getObjectType(String FUNCTION_NAME) {
		if (UserProcedures.server_files.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "MONITORING";
		else if (UserProcedures.missing_files.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "MISSING_FILES";
		else if (UserProcedures.hp_files.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "TAKEN_UP_BY_HP";
		else if (UserProcedures.newPhaseOneNestedProcs.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "NESTED PROCEDURE";
		else if (UserFunctions.newUserFunctions.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "FUNCTION";
		else if (UserFunctions.custom_user_functions.contains(FUNCTION_NAME.substring(FUNCTION_NAME.indexOf(".") + 1)))
			return "CUSTOM FUNCTION";
		else
			return "PROCEDURE";
	}
	
}
