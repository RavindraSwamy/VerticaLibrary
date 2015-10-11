package com.vertica.test.date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.ConversionException;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.util.DateUtil;

public class NegativeDatePatternTest {

	private static transient Logger _log;

	@BeforeClass
	public static void setup() {
		_log = Logger.getLogger("com.vertica.test_app");
	}

	@Test
	public void testDatePatterns() {
		try (Connection cn = ConnectionManager.getConnection()) {
			QueryExecutor qe = new QueryExecutor(cn, _log);

			qe.dropTable("TEST_VALUE_TABLE");
			qe.commit();

			qe.update("CREATE TABLE TEST_VALUE_TABLE (ACOUNT_NO integer PRIMARY KEY NOT NULL, STATUS VARCHAR(8), BALANCE FLOAT, ACNT_OPEN_DATE DATE, TRAN_DATE TIMESTAMP)");
			qe.commit();

			testdd_mm_yyyy(qe);
			testmm_dd_yyyy(qe);
			testslashed_mmddyy(qe);
			testSlashed_ddmmyy(qe);
			testTimestamp(qe);

			qe.dropTable("TEST_VALUE_TABLE");
			qe.commit();

			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
	}

	@SuppressWarnings("deprecation")
	private void testTimestamp(QueryExecutor qe) throws ParseException,
			ConversionException, SQLException {
		String date;
		date = "23-09-1998";
		qe.update(
				"insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE, ACNT_OPEN_DATE, TRAN_DATE) values (1234567, 'ACTIVE', 127.23, ?, ?)",
				DateUtil.stringToDate(date, "dd-MM-yyyy"),
				new java.sql.Timestamp(System.currentTimeMillis()));
		qe.commit();
		assertEquals(
				23,
				qe.getIntValueFromQuery("SELECT DAY(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				9,
				qe.getIntValueFromQuery("Select MONTH(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1998,
				qe.getIntValueFromQuery("SELECT YEAR(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));

		assertEquals(
				new java.sql.Timestamp(System.currentTimeMillis()).getHours(),
				qe.getIntValueFromQuery("SELECT HOUR(TRAN_DATE) FROM TEST_VALUE_TABLE;"));

	}

	private String testSlashed_ddmmyy(QueryExecutor qe) throws ParseException,
			ConversionException, SQLException {
		String date;
		date = "23/09/1998";
		qe.update(
				"insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE, ACNT_OPEN_DATE) values (1234567, 'ACTIVE', 127.23, ?)",
				DateUtil.stringToDate(date, "dd/MM/yyyy"));
		qe.commit();
		assertEquals(
				23,
				qe.getIntValueFromQuery("SELECT DAY(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				9,
				qe.getIntValueFromQuery("Select MONTH(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1998,
				qe.getIntValueFromQuery("SELECT YEAR(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		qe.truncateTable("TEST_VALUE_TABLE");
		return date;
	}

	private void testslashed_mmddyy(QueryExecutor qe) throws ParseException,
			ConversionException, SQLException {
		String date;
		Date stringToDate;
		date = "01/09/1998";
		stringToDate = DateUtil.stringToDate(date, "MM/dd/yyyy");
		qe.update(
				"insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE, ACNT_OPEN_DATE) values (1234567, 'ACTIVE', 127.23, ?)",
				stringToDate);
		qe.commit();
		assertEquals(
				9,
				qe.getIntValueFromQuery("SELECT DAY(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1,
				qe.getIntValueFromQuery("Select MONTH(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1998,
				qe.getIntValueFromQuery("SELECT YEAR(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));

		qe.truncateTable("TEST_VALUE_TABLE");
	}

	private void testmm_dd_yyyy(QueryExecutor qe) throws ParseException,
			ConversionException, SQLException {
		String date;
		Date stringToDate;
		date = "01-09-1998";
		stringToDate = DateUtil.stringToDate(date, "MM-dd-yyyy");
		qe.update(
				"insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE, ACNT_OPEN_DATE) values (1234567, 'ACTIVE', 127.23, ?)",
				stringToDate);
		qe.commit();
		assertEquals(
				9,
				qe.getIntValueFromQuery("SELECT DAY(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1,
				qe.getIntValueFromQuery("Select MONTH(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1998,
				qe.getIntValueFromQuery("SELECT YEAR(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		qe.truncateTable("TEST_VALUE_TABLE");
	}

	private void testdd_mm_yyyy(QueryExecutor qe) throws ParseException,
			ConversionException, SQLException {
		String date = "23-09-1998";
		Date stringToDate = DateUtil.stringToDate(date, "dd-MM-yyyy");
		qe.update(
				"insert into TEST_VALUE_TABLE (ACOUNT_NO, STATUS, BALANCE, ACNT_OPEN_DATE) values (1234567, 'ACTIVE', 127.23, ?)",
				stringToDate);
		qe.commit();
		assertEquals(
				23,
				qe.getIntValueFromQuery("SELECT DAY(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				9,
				qe.getIntValueFromQuery("Select MONTH(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		assertEquals(
				1998,
				qe.getIntValueFromQuery("SELECT YEAR(ACNT_OPEN_DATE) FROM TEST_VALUE_TABLE;"));
		qe.truncateTable("TEST_VALUE_TABLE");

	}

}
