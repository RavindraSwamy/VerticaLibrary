package com.vertica.test;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import com.vertica.app.sql.builder.QueryBuilder;

public class QueryBuilderTest {

	@Test
	public final void testCreateLocalTemporaryTable() {
		StringBuilder actualSQL = QueryBuilder.createLocalTemporaryTable(
				"TEMP_TABLE", "SELECT * FROM ANOTHER_TEMP_TABLE");
		String expectedSQL = "CREATE LOCAL TEMPORARY TABLE TEMP_TABLE ON COMMIT PRESERVE ROWS AS SELECT * FROM ANOTHER_TEMP_TABLE";
		assertEquals(expectedSQL, actualSQL.toString());
	}

	@Test
	public final void testInsertIntoTableStringStringString() {
		StringBuilder actualSQL = QueryBuilder.insertIntoTable("TEST_TARGET",
				"TEST_SOURCE", "OUTPUT < 10");
		String expectedSQL = "INSERT INTO TEST_TARGET SELECT * FROM TEST_SOURCE WHERE OUTPUT < 10";
		assertEquals(expectedSQL, actualSQL.toString());
	}

	@Test
	public final void testInsertIntoTableStringStringStringString() {
		StringBuilder actualSQL = QueryBuilder.insertIntoTable("TEST_TARGET",
				"TEST_SOURCE", "OUTPUT < 10", "NAME");
		String expectedSQL = "INSERT INTO TEST_TARGET SELECT * FROM TEST_SOURCE WHERE OUTPUT < 10 GROUP BY NAME";
		assertEquals(expectedSQL, actualSQL.toString());
	}

	@Test
	public final void testTruncate() {
		String actualSQL = QueryBuilder.truncate("TEST_TARGET");
		String expectedSQL = "TRUNCATE TABLE TEST_TARGET";
		assertEquals(expectedSQL, actualSQL.toString());
	}

	@Test
	public final void testDrop() {
		String actualSQL = QueryBuilder.drop("TEST_TARGET");
		String expectedSQL = "DROP TABLE TEST_TARGET";
		assertEquals(expectedSQL, actualSQL.toString());
	}

	@Test
	public final void testCreateLibrary() {
		try {
			final String actualQuery = QueryBuilder.createLibrary("test_add",
					"/opt/vertica/sdk/demo.jar", "/opt/vertica/sdk/lib/*");
			final String expectedQuery = "CREATE LIBRARY test_add AS '/opt/vertica/sdk/demo.jar' DEPENDS '/opt/vertica/sdk/lib/*' LANGUAGE 'JAVA'";
			assertEquals(actualQuery, expectedQuery);
		} catch (AssertionError e) {
			System.out.println(e.getMessage());
			Assert.fail();
		}
	}

	@Test
	public final void testSupportLibraryNull() {
		try {
			final String actualQuery = QueryBuilder.createLibrary("test_add",
					"/opt/vertica/sdk/demo.jar", null);
			final String expectedQuery = "CREATE LIBRARY test_add AS '/opt/vertica/sdk/demo.jar' LANGUAGE 'JAVA'";
			assertEquals(actualQuery, expectedQuery);
			Assert.assertFalse(actualQuery.contains("  "));
		} catch (AssertionError e) {
			System.out.println(e.getMessage());
			Assert.fail();
		}
	}

	@Test
	public final void testCreateFunction() {
		try {
			final String actualQuery = QueryBuilder.createFunction(
					"test_function", "com.vertica.sdk.AddTwoNumbersFactory",
					"test_add");
			final String expectedQuery = "CREATE FUNCTION test_function AS LANGUAGE 'JAVA' NAME 'com.vertica.sdk.AddTwoNumbersFactory' LIBRARY test_add FENCED";
			assertEquals(actualQuery, expectedQuery);
			Assert.assertFalse(actualQuery.contains("  "));
		} catch (AssertionError e) {
			System.out.println(e.getMessage());
			Assert.fail();
		}
	}

	@Test
	public final void testAlterLibrary() {
		try {
			final String actualQuery = QueryBuilder.alterLibrary("test_add",
					"/opt/vertica/sdk/demo.jar", "/opt/vertica/sdk/lib/*");
			final String expectedQuery = "ALTER LIBRARY test_add AS '/opt/vertica/sdk/demo.jar' DEPENDS '/opt/vertica/sdk/lib/*'";
			assertEquals(actualQuery, expectedQuery);
			Assert.assertFalse(actualQuery.contains("  "));
		} catch (AssertionError e) {
			System.out.println(e.getMessage());
			Assert.fail();
		}
	}

}
