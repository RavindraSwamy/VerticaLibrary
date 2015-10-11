package com.vertica.app.sql.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.vertica.sdk.UdfException;
import com.vertica.util.DatabaseUtil;


/**
 * @author Abhishek Chaudhary
 *
 */
public class ConnectionManager {

	/**
	 * Create a <code>Connection</code>
	 */
	public static Connection getConnection() {
		return new VerticaConnection().getDBConnection();
	}
	
	/**
	 * Create a <code>Connection</code>
	 */
	public static Connection getConnection(final String schema_name) throws SQLException {
		return new VerticaConnection().getDBConnection(schema_name);
	}

	/**
	 * Create a <code>Connection</code>
	 */
	public static Connection getConnectionWithSearchPath(final String searchPath) {
		return new VerticaConnection().getDBConnectionWithSearchPath(searchPath);
	}

	/**
     * Close a <code>Connection</code>, avoid closing if null.
     * @param conn Connection to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Connection conn) throws SQLException {
        if (conn != null) {
            DatabaseUtil.close(conn);
        }
    }

    /**
     * Close a <code>ResultSet</code>, avoid closing if null.
     * @param rs ResultSet to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
        	DatabaseUtil.close(rs);
        }
    }

    /**
     * Close a <code>Statement</code>, avoid closing if null.
     * @param stmt Statement to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Statement stmt) throws SQLException {
        if (stmt != null) {
        	DatabaseUtil.close(stmt);
        }
    }

    /**
     * Close a <code>Connection</code>, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param conn Connection to close.
     */
    public static void closeSilently(Connection conn) {
        try {
            close(conn);
        } catch (SQLException e) {
            // Eat Exception
        }
    }

    /**
     * Close a <code>Connection</code>, <code>Statement</code> and
     * <code>ResultSet</code>.  Avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param conn Connection to close.
     * @param stmt Statement to close.
     * @param rs ResultSet to close.
     */
    public static void closeSilently(Connection conn, Statement stmt,
            ResultSet rs) {

        try {
            closeSilently(rs);
        } finally {
            try {
                closeSilently(stmt);
            } finally {
                closeSilently(conn);
            }
        }

    }

    /**
     * Close a <code>ResultSet</code>, avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param rs ResultSet to close.
     */
    public static void closeSilently(ResultSet rs) {
        try {
            close(rs);
        } catch (SQLException e) {
        	// Eat Exception
        }
    }

    /**
     * Close a <code>Statement</code>, avoid closing if null and hide
     * any SQLExceptions that occur.
     * @param stmt Statement to close.
     */
    public static void closeSilently(Statement stmt) {
        try {
            close(stmt);
        } catch (SQLException e) {
        	// Eat Exception
        }
    }

    /**
	 * Closes the {@code}connection
	 */
	public static void commit(final Connection connection) {
		try {
			if (connection != null && !connection.getAutoCommit()) {
				connection.commit();
			}
		} catch (SQLException e) {
	    	throw new UdfException(e.getErrorCode(), e.getMessage());
		}
	}

    /**
     * Commits a <code>Connection</code> then closes it, avoid closing if null.
     *
     * @param conn Connection to close.
     * @throws SQLException if a database access error occurs
     */
    public static void commitAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.commit();
            } finally {
            	DatabaseUtil.close(conn);
            }
        }
    }

    /**
     * Commits a <code>Connection</code> then closes it, avoid closing if null
     * and hide any SQLExceptions that occur.
     * @param conn Connection to close.
     */
    public static void blindCommit(Connection conn) {
        try {
            commitAndClose(conn);
        } catch (SQLException e) {
        	// Eat Exception
        }
    }

    /**
     * Performs a rollback on the <code>Connection</code> then closes it,
     * avoid closing if null.
     * @param conn Connection to rollback.  A null value is legal.
     * @throws SQLException if a database access error occurs
     */
    public static void rollbackAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.rollback();
            } finally {
            	DatabaseUtil.close(conn);
            }
        }
    }

}
