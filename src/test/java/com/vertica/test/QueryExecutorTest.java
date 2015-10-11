
package com.vertica.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.sdk.UdfException;


public class QueryExecutorTest {

	private static transient Logger _log;

	@BeforeClass
	public static void setup(){
		_log = UDXLogFactory.getEDWLogger();
	}

	@Test
	public void testCreateLocalTemporaryTable(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			ResultSet resultSet = qe.query("select * from TEST_TABLE");
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() > 0);
	}

	@Test
	public void testUpdateWithParam(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST WHERE OUTPUT > ?", 1);
			qe.commit();
			ResultSet resultSet = qe.query("select * from TEST_TABLE");
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() > 0);
	}

	@Test
	public void updateWithInvalidConnection(){
		ExpectedException ex = ExpectedException.none();
		ex.expect(UdfException.class);
		try (Connection cn = null){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST WHERE OUTPUT > ?", 1);
			qe.commit();
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			//Assert.fail();
		}
	}

	@Test
	public void updateWithInvalidQuery(){
		ExpectedException ex = ExpectedException.none();
		ex.expect(UdfException.class);
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.update("");
			qe.commit();
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			//Assert.fail();
		}
	}

	@Test
	public void testTruncateTable(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			qe.truncateTable("TEST_TABLE");
			ResultSet resultSet = qe.query("select * from TEST_TABLE");
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() == 0);
	}

	@Test
	public void testQueryWithParam(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			qe.truncateTable("TEST_TABLE");
			ResultSet resultSet = qe.query("select * from TEST_TABLE WHERE OUTPUT > ?", 0);
			getResultsetMetadata(resultSet);
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() == 0);

	}

	private static void getResultsetMetadata(ResultSet rs) throws SQLException {
		//ResultSetMetadata is accessible from ResultSet
		ResultSetMetaData metaData = rs.getMetaData();
		System.out.println("Total number of columns "+ metaData.getColumnCount());
		for (int i = 0 ; i < metaData.getColumnCount() ; i++){
			System.out.println("Column at index " + i + " is " + metaData.getColumnLabel(i+1).toUpperCase());
			System.out.println("Column Type " + " is " + metaData.getColumnTypeName((i+1)));
		}
	}

	@Test //(expected = UdfException.class)
	public void testDropTable(){
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			qe.dropTable("TEST_TABLE");
			qe.query("select * from TEST_TABLE WHERE OUTPUT > ?", 0);
			ConnectionManager.commitAndClose(cn);
		}catch (UdfException | SQLException e) {
			if (!validError(e, "4566")){
				fail();
			}
		}
	}

	private boolean validError(Exception e, final String expectedErrorCode) {
		return e.getMessage().substring(e.getMessage().indexOf("(") + 1, e.getMessage().indexOf("(") + 5).equals(expectedErrorCode);
	}

	@Test
	public void testInsertIntoTable(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			qe.truncateTable("TEST_TABLE");
			qe.insertIntoTable("TEST_TABLE", "TEST", "OUTPUT > 0");
			ResultSet resultSet = qe.query("select * from TEST_TABLE WHERE OUTPUT > ?", 0);
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() != 0);
	}

	@Test
	public void testInsertIntoTableWithGroupBy(){
		List<Object> results = new ArrayList<>();
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.createLocalTemporaryTable("TEST_TABLE", "SELECT * FROM TEST");
			qe.commit();
			qe.truncateTable("TEST_TABLE");
			qe.insertIntoTable("TEST_TABLE", "TEST", "OUTPUT > 0", "OUTPUT");
			ResultSet resultSet = qe.query("select * from TEST_TABLE WHERE OUTPUT > ?", 0);
			try {
				while(resultSet.next()){
					results.add(resultSet.getObject(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
		assertTrue(results.size() != 0);
	}

	@Test
	public void testGetValueFromQuery(){
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			qe.update("CREATE TABLE TEST_VALUE_TABLE (ACOUNT_NO integer PRIMARY KEY NOT NULL, STATUS VARCHAR(8), BALANCE FLOAT)");
			qe.commit();
			qe.update("insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE) values (1234567, 'ACTIVE', 127.23)");

			assertEquals(1234567, qe.getIntValueFromQuery("select ACOUNT_NO from TEST_VALUE_TABLE"));
			assertEquals(1234567, qe.getLongValueFromQuery("select ACOUNT_NO from TEST_VALUE_TABLE"));
			assertEquals("ACTIVE", qe.getStringValueFromQuery("select STATUS from TEST_VALUE_TABLE"));
			assertEquals(127.23, qe.getDoubleValueFromQuery("select BALANCE from TEST_VALUE_TABLE"), 0.0);

			qe.dropTable("TEST_VALUE_TABLE");
			qe.commit();
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail();
		}
	}


}
