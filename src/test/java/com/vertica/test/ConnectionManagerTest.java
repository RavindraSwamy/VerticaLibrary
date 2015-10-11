package com.vertica.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.vertica.app.sql.engine.ConnectionManager;

public class ConnectionManagerTest {

	@Test
	public final void testGetConnection() {
		Connection connection = ConnectionManager.getConnection();
		assertNotNull(connection);
		try {
			ConnectionManager.close(connection);
		} catch (SQLException e) {
			fail();
		}
	}

	@Test
	public void testCommitAndCloseConnection() throws Exception {
		Connection mockCon = mock(Connection.class);
		ConnectionManager.commitAndClose(mockCon);
		verify(mockCon).commit();
		verify(mockCon).close();
	}

	@Test
	public void closeNullConnection() throws Exception {
		ConnectionManager.close((Connection) null);
	}

	@Test
	public final void testCloseConnection() throws SQLException {
		Connection mockCon = mock(Connection.class);
		ConnectionManager.commitAndClose(mockCon);
		verify(mockCon).close();
	}

	@Test
	public final void testCloseResultSet() throws SQLException {
		ResultSet rs = mock(ResultSet.class);
		ConnectionManager.close(rs);
		verify(rs).close();
	}

	@Test
	public final void testCloseStatement() throws SQLException {
		PreparedStatement ps = mock(PreparedStatement.class);
		ConnectionManager.close(ps);
		verify(ps).close();
	}

	@Test
	public final void testCloseSilentlyConnection() throws SQLException {
		Connection mockCon = mock(Connection.class);
		ConnectionManager.closeSilently(mockCon);
		verify(mockCon).close();
	}


}
