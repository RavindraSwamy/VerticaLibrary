package com.vertica.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.sdk.UdfException;

public class TestTemplateForTryCatch {

	public static void main(String[] args) throws Exception {
		Connection conn = null;
		conn = ConnectionManager.getConnection();

		try (PreparedStatement mStmt = conn
				.prepareStatement("CREATE TABLE UAT_ACCT_BASE_11102012_WCC (Product_Key integer NOT NULL, Product_Description varchar(128))")) {
			mStmt.executeUpdate();
		} catch (SQLException se) {
			if (conn != null) {
				conn.close();
			}
			throw new UdfException(se.getErrorCode(), se.getMessage());
		}

		try (Connection vconn = ConnectionManager.getConnection();) {
			try (PreparedStatement mStmt = conn
					.prepareStatement("CREATE TABLE UAT_ACCT_BASE_11102012_WCC (Product_Key integer NOT NULL, "
							+ "Product_Description varchar(128))")) {
				mStmt.executeUpdate();
			}
			try (PreparedStatement mStmt = conn
					.prepareStatement("CREATE TABLE ACCT_BASE_11102012_WCC (Product_Key integer NOT NULL, "
							+ "Product_Description varchar(128))")) {
				mStmt.executeUpdate();
			}
			try {
				if (!vconn.getAutoCommit())
					vconn.commit();
			} catch (Exception e) {
				if (vconn != null) {
					vconn.rollback();
					vconn.close();
				}
			}
		} catch (SQLException se) {
			throw new UdfException(se.getErrorCode(), se.getMessage());
		}

		System.out.println("Execution Completed");

	}

}
