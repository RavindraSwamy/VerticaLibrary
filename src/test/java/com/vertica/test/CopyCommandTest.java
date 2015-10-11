package com.vertica.test;

import static org.junit.Assert.fail;

import java.sql.Connection;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.QueryExecutor;

public class CopyCommandTest {
	private static transient Logger _log;

	@BeforeClass
	public static void setup(){
		_log = UDXLogFactory.getEDWLogger();
	}

	@Test
	public void testCopyCommand(){
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			int count = (int) qe.executeCopy("COPY PUBLIC.TEST_DEPLOYMENT (col1, col2) FROM '/vertica_load/tmp/test_copy_file.out' DELIMITER '~'");
			Assert.assertEquals(10, count);
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
	}

	@Test
	public void testCopyRejectCount(){
		try (Connection cn = ConnectionManager.getConnection()){
			QueryExecutor qe = new QueryExecutor(cn, _log);
			int count = (int) qe.executeCopy("COPY PUBLIC.TEST_DEPLOYMENT (col1, col2) FROM '/vertica_load/tmp/test_copy_reject_file.out' DELIMITER '~'");
			Assert.assertEquals(7, count);
			ConnectionManager.commitAndClose(cn);
		} catch (Exception e) {
			fail("Error !");
		}
	}
}
