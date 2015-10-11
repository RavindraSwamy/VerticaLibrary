package com.vertica.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.util.DatabaseUtil;

public class GetMemoryConsumptionDetail {

	public static void main(String[] args) {
		try (Connection cn = ConnectionManager.getConnection()) {
			QueryExecutor qe = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());

			///getJVMPoolInfo(qe);
			//getMemoryUsage(qe);
			//getMemoryConsumedInSession(qe);
			releaseJVMMemory(qe);
			releaseAllJVMMemory(qe);

			ConnectionManager.commitAndClose(cn);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error!");
		}
	}

	public static void getMemoryUsage(QueryExecutor qe) throws SQLException {
		ResultSet rs;
		rs = qe.query("SELECT * FROM MEMORY_USAGE");
		DatabaseUtil.printResultSet(rs);
	}

	/**
	 * Terminates a Java Virtual Machine (JVM), making available the memory the
	 * JVM was using.
	 *
	 * @param qe
	 * @throws SQLException
	 */
	private static void releaseJVMMemory(QueryExecutor qe) throws SQLException {
		ResultSet rs;
		rs = qe.query("SELECT RELEASE_JVM_MEMORY()");
		DatabaseUtil.printResultSet(rs);
	}

	/**
	 * Forces all sessions to release the memory consumed by their Java Virtual
	 * Machines (JVM).
	 *
	 * @Permissions Must be a superuser.
	 *
	 * @Caution: This function terminates all JVMs, including ones that are
	 *           currently executing Java UDXs. This will cause any query that
	 *           is currently executing a Java UDx to return an error.
	 *
	 * @param qe
	 * @throws SQLException
	 */
	private static void releaseAllJVMMemory(QueryExecutor qe)
			throws SQLException {
		ResultSet rs;
		rs = qe.query("SELECT RELEASE_ALL_JVM_MEMORY()");
		DatabaseUtil.printResultSet(rs);
	}

	public static void getMemoryConsumedInSession(QueryExecutor qe)
			throws SQLException {
		ResultSet rs;
		rs = qe.query("SELECT USER_NAME,JVM_MEMORY_KB FROM V_MONITOR.SESSIONS");
		DatabaseUtil.printResultSet(rs);
	}

	private static void getJVMPoolInfo(QueryExecutor qe) throws SQLException {
		ResultSet rs = qe
				.query("SELECT MAXMEMORYSIZE, PLANNEDCONCURRENCY FROM V_CATALOG.RESOURCE_POOLS WHERE NAME = 'jvm';");
		DatabaseUtil.printResultSet(rs);
	}

	public static void find() {
		/* Total number of processors or cores available to the JVM */
		System.out.println("Available processors (cores): "
				+ Runtime.getRuntime().availableProcessors());

		/* Total amount of free memory available to the JVM */
		System.out.println("Free memory (bytes): "
				+ Runtime.getRuntime().freeMemory() / 1024);
		Runtime.getRuntime().traceMethodCalls(true);
		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();
		/* Maximum amount of memory the JVM will attempt to use */
		System.out
				.println("Maximum memory (bytes): "
						+ (maxMemory == Long.MAX_VALUE ? "no limit"
								: maxMemory / 1024));

		/* Total memory currently in use by the JVM */
		System.out.println("Total memory (bytes): "
				+ Runtime.getRuntime().totalMemory() / 1024);

		/* Get a list of all filesystem roots on this system */
		// File[] roots = File.listRoots();

		/* For each filesystem root, print some info */
		/*
		 * for (File root : roots) { System.out.println("File system root: " +
		 * root.getAbsolutePath()); System.out.println("Total space (bytes): " +
		 * root.getTotalSpace()); System.out.println("Free space (bytes): " +
		 * root.getFreeSpace()); System.out .println("Usable space (bytes): " +
		 * root.getUsableSpace()); }
		 */
	}
}
