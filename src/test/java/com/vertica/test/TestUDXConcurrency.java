package com.vertica.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.sdk.UdfException;

public class TestUDXConcurrency {

	private static ExecutorService pool;

	public static void main(String[] args) {
		pool = Executors.newFixedThreadPool(15);

		for (int i = 0; i < 7; i++) {
			new Thread() {
				@Override
				public void run() {
					System.out.println(this.getName() + " started at .... "
							+ System.currentTimeMillis());
					super.run();
					Connection mConn = null;
					mConn = ConnectionManager.getConnection();
					try (PreparedStatement mStmt = mConn
							.prepareStatement("SELECT edw.proc_dpd_int_data();");) {
						long startime = System.currentTimeMillis();
						ResultSet rs = mStmt.executeQuery();
						System.out.println(System.currentTimeMillis()
								- startime + " mili seconds");
						if (rs.next()) {
							// System.out.println(rs.getString(1));
						}
					} catch (SQLException se) {
						throw new UdfException(se.getErrorCode(),
								se.getMessage());
					} finally {
						try {
							mConn.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					System.out.println(this.getName() + " finished....");

				}
			}.start();
			shutdownAndAwaitTermination(pool);
		}
	}

	public static void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

}
