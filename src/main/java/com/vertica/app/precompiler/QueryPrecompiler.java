package com.vertica.app.precompiler;

import static com.vertica.util.StringUtil.containsIgnoreCase;
import static com.vertica.util.StringUtil.isToken;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.sdk.UdfException;
import com.vertica.util.ExceptionUtil;

/**
 * Try to keep the logic simple and compact by creating a single class named
 * QueryPreCompiler which operates in below fashion
 *
 * (Pre-requisite: Keep all Java class received from ISpirer in a specific
 * folder)
 *
 * 1. iterate over each of the Java class 2. Parse and fetch all SQL statements
 * inside a Java class to a single place 3. Once all SQL statements are
 * available, send them to database one by one for pre-compilation 4. If
 * pre-compilation is successful, the statement is stored inside ‘passed’
 * bucket, else it is stored in ‘failed’ bucket 5. For each of the failed SQL
 * statements, the class also publishes the root cause of failure
 *
 * The result of this effort is, all ISpirer generated Vertica compliant SQL
 * statements can be validated directly against Vertica Database without having
 * any dependency on actual data and we can get a holistic efficiency of entire
 * store procedure conversion.
 *
 * We can run this utility every-time we get a new build from Ispirer
 *
 * @author Abhishek Chaudhary
 *
 */
public final class QueryPrecompiler {

	private static String searchPath;

	public static void process(final boolean isDirectory, final String dir_path, final String classToBeCompiled) throws FileNotFoundException, IOException{
		if (isDirectory)
			processFolder(dir_path);
		else
			processFile(dir_path, classToBeCompiled);
	}

	private static void processFolder(final String dir_path) throws FileNotFoundException, IOException{
		preCompileFiles(dir_path, null);
	}

	private static void processFile(final String dir_path, final String classToBeCompiled) throws FileNotFoundException, IOException{
		preCompileFiles(dir_path, classToBeCompiled);
	}
	public static void preCompileFiles(final String dir_path, final String classToBeCompiled)
			throws FileNotFoundException, IOException {

		ArrayList<String> queries = new ArrayList<String>();
		ArrayList<String> compiledQueries = new ArrayList<String>();
		ArrayList<String> failedQueries = new ArrayList<String>();

		int totalNoOfQueriesTested = 0;
		int totalNoOfQueriesPassed = 0;
		int totalNoOfQueriesFailed = 0;

		final File dir = new File(dir_path);

		final File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (final File individualProcFile : directoryListing) {
				queries = new ArrayList<>();

				if (individualProcFile.isDirectory()) {
					continue;
				}

				if (isToken(classToBeCompiled)) {
					if (!individualProcFile.getName().equals(classToBeCompiled)) {
						continue;
					}
				}

				System.out
				.println("--------------------------NEW FILE-------------"
						+ individualProcFile.getName()
						+ "----------------");

				String line = "";
				final FileInputStream fs = new FileInputStream(
						individualProcFile);
				final BufferedReader br = new BufferedReader(
						new InputStreamReader(fs));

				while ((line = br.readLine()) != null) {
					if (containsIgnoreCase(line, "prepareStatement(")) {
						String query = "";
						line = line.trim();

						int start = line.indexOf("prepareStatement(");
						start += "prepareStatement(".length();

						if (line.contains(");")) { // single line query
							query = line.substring(start, line.indexOf(");"));
							queries.add(query);
						} else { // Multi-line query
							constructFatLineQuery(queries, line, br, start);
						}
					}
				}
				fs.close();
				br.close();
				totalNoOfQueriesTested += queries.size();
				validateQuery(queries, compiledQueries = new ArrayList<>(), failedQueries = new ArrayList<>());
				totalNoOfQueriesPassed += compiledQueries.size();
				totalNoOfQueriesFailed += failedQueries.size();

			}
		}
		System.out.println("\n \n \nGathering overall validation statistics ...\n");
		System.out.println("Total No of Files Validated: " + dir.listFiles().length);
		System.out.println("Total No of SQLs validated: " + totalNoOfQueriesTested);
		System.out.println("Total No of SQLs successfully precompiled: " + totalNoOfQueriesPassed);
		System.out.println("Total No of SQLs failed precompilation: " + totalNoOfQueriesFailed);
	}

	/**
	 * Special treatment needed for queries spread across multiple lines
	 *
	 * @param queries
	 * @param line
	 * @param br
	 * @param start
	 * @throws IOException
	 */
	private static void constructFatLineQuery(final ArrayList<String> queries,
			String line, final BufferedReader br, final int start)
					throws IOException {
		String query;
		String fatline = line;
		fatline = QueryFormatter.linearizeQuery(true, false, fatline);
		while ((line = br.readLine()) != null) {
			line = line.trim();
			if (line.length() == 0) { // do nothing
				continue;
			}
			if (!line.contains(");")) { // intermediate lines
				if (line.length() != 0)
					line = QueryFormatter.linearizeQuery(false, false, line);
				fatline += " " + line;
			} else { // last line
				fatline += " " + QueryFormatter.linearizeQuery(false, true, line);
				;
				query = fatline.substring(start, fatline.indexOf(");"));
				queries.add(query);
				break;
			}
		}
	}


	/**
	 * Once the queries are extracted from Java Classes, perform necessary clean
	 * up on the query and start compiling the queries using PreparedStatement
	 *
	 * @param queries
	 * @param failedQueries2
	 * @param compiledQueries2
	 */
	private static void validateQuery(final ArrayList<String> queries,
			final ArrayList<String> compiledQueries,
			final ArrayList<String> failedQueries) {

		try (Connection cn = ConnectionManager
				.getConnectionWithSearchPath(searchPath)) {
			for (final Iterator<String> iterator = queries.iterator(); iterator
					.hasNext();) {
				String query = iterator.next();

				query = cleanupFinalQuery(query);

				if (compileQuery(query, cn)) {
					compiledQueries.add(query);
				} else {
					failedQueries.add(query);
				}
			}
		} catch (final Exception e) {
			System.err.println("Error!");
		}
		System.out.println("No of SQLs in file: " + queries.size()
				+ ", SQLs compiled successfully : "
				+ compiledQueries.size() + ", SQLs failed precompilation : "
				+ failedQueries.size());

	}

	private static String cleanupFinalQuery(String query) {
		query = query.substring(1, query.length() - 1);
		query = query.replace("\\\"", "");
		return query;
	}

	/**
	 * Responsible for sending all queries to database for pre-compilation
	 *
	 * In order to automate unit testing process, we are using out of the box
	 * capabilities of a JDBC PreparedStatement. PreparedStatement can support
	 * an important feature called ‘pre-compilation’ if a database driver
	 * supports so. (Some of the database drivers support pre-compilation and
	 * some simpler ones do not). Vertica JDBC driver does support
	 * pre-compilation, the prepareStatement method will send the statement to
	 * the database for pre-compilation. What it really means is, the SQL
	 * statement can be directly sent to the database without waiting for
	 * PreparedStatement object to be actually executed on to the database. With
	 * preparedStatement, we can validate a parametrized as well as
	 * non-parameterized query. Method Connection.prepareStatement(query) is
	 * optimized for handling parametric SQL statements that benefit from
	 * precompilation. Pre-compilation covers below aspects
	 *
	 * 1. Validating Syntactical issues in the Query 2. Identifying reference of
	 * a database object in query which doesn’t exist in the database (column,
	 * table, view etc..) 3. Identifying reference of external procedure or UDX
	 * which doesn’t exist in database
	 *
	 * @param query
	 * @param connection
	 * @return
	 * @throws UdfException
	 */
	public static boolean compileQuery(final String query,
			final Connection connection) throws UdfException {
		if (connection == null) {
			System.err.println("Error!");
		}
		if (containsIgnoreCase(query, "select ?")) {
			return true;
		}
		if (query == null || query.isEmpty()) {
			System.err.println("Error!");
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {

			if (containsIgnoreCase(query, "create local temporary table")) {
				mStmt.executeUpdate();
				connection.commit();
			}
			if (query.contains("?")) {
				charCounter('?', query);
			}
			return true;
		} catch (final SQLException se) {

			System.out.println(query);
			System.out.println(ExceptionUtil.getBriefException(se));
			return false;

		}
	}

	/**
	 * Counts the occurrence of a character in a given string
	 *
	 * @param charToBeSearched
	 * @param input
	 * @return
	 */
	public static int charCounter(final char charToBeSearched,
			final String input) {
		int charCount = 0; // resetting character count
		for (final char ch : input.toCharArray()) {
			if (ch == charToBeSearched) {
				charCount++;
			}
		}
		return charCount;
	}


	public static void setSearchPath(final String searchPath) {
		QueryPrecompiler.searchPath = searchPath;
	}

}
