package com.vertica.app.sql.engine;

import static com.vertica.app.sql.builder.QueryBuilder.drop;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.vertica.app.loader.ApplicationProperties;
import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.builder.QueryBuilder;
import com.vertica.app.sql.copy.CopyStmtGenerator;
import com.vertica.app.sql.handler.UdfExceptionhandler;
import com.vertica.sdk.UdfException;
import com.vertica.util.DatabaseUtil;

/**
 * Class to execute queries on to database. Creates queries with help of
 * {@link QueryBuilder}. The <code>Connection</code> is retrieved from the
 * <code>UDX</code> set in the constructor
 *
 * Performs the Rollback in case of exception
 *
 * @author Abhishek Chaudhary
 *
 */

public class QueryExecutor implements IQueryExecutor {

	private final UdfExceptionhandler handler = new UdfExceptionhandler();
	private static transient Logger _log;
	private final Connection connection;
	private final String current_user;


	
	public QueryExecutor(final Connection cn, final Logger _log) {
		String curr = null;
		try {
			curr = cn.getMetaData().getUserName();
			_log.info("Current user: " + cn.getMetaData().getUserName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.current_user = curr;
		this.connection = cn;
		QueryExecutor._log = _log;
	}

	/** 
	 * Truncates the table 
	 * @param tableName
	 * @throws SQLException
	 * @throws UdfException
	 */
	public void truncateTable(final String tableName) throws SQLException, UdfException {
		if (getSchemaName(tableName).isEmpty() || current_user.equalsIgnoreCase(getSchemaName(tableName))){
			update(QueryBuilder.truncate(tableName));
		}
		else
			truncateWithAdmin(QueryBuilder.truncate(tableName));
	}
	

	/**
	 * New method added to change database connection to DBA user for truncate of table.
	 * <br>The purpose of this method is to solve truncate issue
	 *  when user is not able to truncate table if not an owner of it. 
	 * @param truncate
	 * @return
	 */
	public int truncateWithAdmin(String query)  throws SQLException, UdfException {
		// while truncating a table- make it with database administrator
		Connection cn = ConnectionManager.getConnection("pwr");
		if (cn == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(cn, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = cn.prepareStatement(query);) {
			_log.info("Query: " + query);
			int count = mStmt.executeUpdate();
			_log.info("Total Records Affected: " + count);
			//cn.close();
			return count;
		} catch (SQLException se) {
			handler.handle(cn, se, _log);
		}

		return 0;
	}

	public long truncateAndAudit(String tableName) throws SQLException, UdfException{
		long count = getLongValueFromQuery("SELECT COUNT(*) FROM " + tableName);
		truncateTable(tableName);
		return count;
	}

	public int dropTable(final String tableName) throws UdfException {
		return update(drop(tableName));
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query.
	 *
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            The query replacement parameters.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	@Override
	public int update(final String query, final Object... params) throws UdfException {
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {
			fillStatement(mStmt, params);
			_log.info("Query: " + query);
			int count = mStmt.executeUpdate();
			_log.info("Total Records Affected: " + count);
			return count;
		} catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return 0;
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query without replacement
	 * parameters.
	 *
	 * @param query
	 *            The SQL to execute.
	 * @return The number of rows updated.
	 * @throws UdfException
	 *             if a database access error occurs
	 */
	public int update(final String query) throws UdfException {
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {
			_log.info("Query: " + query);
			int count = mStmt.executeUpdate();
			_log.info("Total Records Affected: " + count);
			return count;
		} catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return 0;
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query without replacement
	 * parameters.
	 *
	 * @param query
	 *            The SQL to execute.
	 * @return The number of rows updated.
	 * @throws UdfException
	 *             if a database access error occurs
	 */
	public int updateByErrorCode(final String query) throws UdfException {
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {
			_log.info("Query: " + query);
			mStmt.executeUpdate();
			return 0;
		} catch (SQLException se) {
			_log.error("Error while updating records", se);
			return se.getErrorCode();
		}
	}



	/**
	 * Inserts the data into target table from source table
	 * @param targetTable
	 * @param sourceTable
	 * @param whereClause
	 * @return
	 */
	public int insertIntoTable(final String targetTable,
			final String sourceTable, final String whereClause) {

		return update(QueryBuilder.insertIntoTable(targetTable,
				sourceTable, whereClause).toString());

	}

	/**
	 * Inserts the data into target table from source table
	 * @param targetTable
	 * @param sourceTable
	 * @param whereClause
	 * @return
	 */
	public int insertIntoTable(final String targetTable,
			final String sourceTable, final String whereClause,
			final String groupByClause)  throws UdfException {
		return update(QueryBuilder.insertIntoTable(targetTable,
				sourceTable, whereClause, groupByClause).toString());
	}

	public long getLongValueFromQuery(final String query,
			final Object... params) throws UdfException {
		long result = 0;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getLong(1);
		} catch (SQLException e) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, e, _log);

		}
		return result;
	}

	public Short getShortValueFromQuery(final String query,
			final Object... params)  throws UdfException {
		Short result = 0;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getShort(1);
		} catch (SQLException e) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, e, _log);

		}
		return result;
	}


	public int getIntValueFromQuery(final String query,
			final Object... params)  throws UdfException {
		int result = 0;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getInt(1);
		} catch (SQLException se) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, se, _log);
		}
		return result;
	}

	public double getDoubleValueFromQuery(final String query,
			final Object... params) throws UdfException {
		double result = 0;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getDouble(1);
		} catch (SQLException se) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, se, _log);
		}
		return result;
	}

	public String getStringValueFromQuery(final String query,
			final Object... params)  throws UdfException {
		String result = "";
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getString(1);
		} catch (SQLException e) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, e, _log);
		}
		return result;
	}

	public Timestamp getTimestampValueFromQuery(final String query,
			final Object... params)  throws UdfException {
		Timestamp result = null;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getTimestamp(1);
		} catch (SQLException se) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, se, _log);
		}
		return result;
	}

	public Date getDateValueFromQuery(final String query,
			final Object... params)  throws UdfException {
		Date result = null;
		try (ResultSet rs = query(query, params)) {
			if (rs.next())
				result = rs.getDate(1);
		} catch (SQLException se) {
			UdfExceptionhandler handler = new UdfExceptionhandler();
			handler.handle(connection, se, _log);
		}
		return result;
	}


	/**
	 * WARNING: Callers must close statement to avoid resource leaks
	 * @param connection
	 * @param query
	 * @param params
	 * @return
	 * @throws UdfException
	 * @throws SQLException 
	 */
	@Override
	public ResultSet query(String query, Object... params)
			throws UdfException, SQLException {
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null|| query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try{
			PreparedStatement mStmt = connection.prepareStatement(query);
			fillStatement(mStmt, params);
			_log.info("Query: " + query);
			return mStmt.executeQuery();
		}  catch (SQLException se) {
			if (se.getErrorCode()  == 4072 ) {
 				throw se;
			}
			handler.handle(connection, se, _log);
		}
		return null;
	}

	/**
	 * WARNING: Callers must close statement to avoid resource leaks
	 *
	 * Execute an SQL SELECT query with replacement parameters. The caller is
	 * responsible for closing the connection.
	 *
	 * @throws SQLException
	 *             if a database access error occurs
	 * @param query
	 *            The query to execute.
	 * @return the {@link ResultSet} object
	 * @throws UdfException
	 */
	public ResultSet query(String query) throws UdfException {
		if (connection == null)  {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null|| query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try {
			PreparedStatement mStmt = connection.prepareStatement(query);
			_log.info("Query: " + query);
			return mStmt.executeQuery();
		} catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return null;
	}

	/**
	 * Fill the <code>PreparedStatement</code> replacement parameters with the
	 * given objects.
	 *
	 * @param stmt
	 *            PreparedStatement to fill
	 * @param params
	 *            Query replacement parameters; <code>null</code> is a valid
	 *            value to pass in.
	 * @throws SQLException
	 *             if a database access error occurs
	 */

	public void fillStatement(PreparedStatement stmt, Object... params) throws UdfException {

		// nothing to do here
		if (params == null) {
			return;
		}

		boolean pmdNotSupported = false;

		ParameterMetaData pmd = null;
		if (!pmdNotSupported) {
			try {
				pmd = stmt.getParameterMetaData();
				//stmt.getMetaData().getTableName(column)
				if (pmd == null) { // can be returned by implementations that
					// don't support the method
					pmdNotSupported = true;
				} else {
					int stmtCount = pmd.getParameterCount();
					int paramsCount = params == null ? 0 : params.length;

					if (stmtCount != paramsCount) {
						throw new SQLException(
								"Wrong number of parameters: expected "
										+ stmtCount + ", was given "
										+ paramsCount);
					}
				}
			} catch (SQLException ex) {
				pmdNotSupported = true;
			}
		}

		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				try {
					stmt.setObject(i + 1, params[i]);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				int sqlType = Types.VARCHAR;
				if (!pmdNotSupported) {
					try {
						sqlType = pmd.getParameterType(i + 1);
					} catch (SQLException e) {
						pmdNotSupported = true;
					}
				}
				try {
					stmt.setNull(i + 1, sqlType);
				} catch (SQLException e) {
					e.printStackTrace();
				} // Per Java doc, you must specify
				// the parameter's SQL type.
			}
		}
	}

	@Deprecated
	public static PreparedStatement fillPreparedStatement(
			PreparedStatement stmt, Object... params) throws SQLException {
		if (params == null || params.length <= 0) {
			return stmt;
		}
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				stmt.setObject(i + 1, params[i]);
			} else {
				stmt.setNull(i + 1, Types.OTHER);
			}
		}
		return stmt;

	}

	/**
	 * Closes the {@code}connection
	 */
	public void commit() {
		try {
			if (connection != null && !connection.getAutoCommit()) {
				connection.commit();
			}
		} catch (SQLException e) {
			handler.handle(connection, e, _log);
		}
	}

	/**
	 * Creates a local temporary table. Query can have variable number of params
	 *
	 * Example:
	 *
	 * CREATE LOCAL TEMPORARY TABLE MM ON COMMIT PRESERVE ROWS AS select
	 * source_account_nbr,User_ID,Old_Net_User_ID from edw.t09315_car_accounts
	 * where SOURCE_SYSTEM_CODE = '30'"
	 *
	 * @param tableName MM
	 *
	 * @param selectClause
	 *
	 * select source_account_nbr,User_ID,Old_Net_User_ID from edw.t09315_car_accounts
	 * where SOURCE_SYSTEM_CODE = '30'"
	 *
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int createLocalTemporaryTable(final String tableName,
			final String selectClause, final Object... params)  throws UdfException{
		return update(QueryBuilder.createLocalTemporaryTable(tableName, selectClause)
				.toString(), params);
	}


	/**
	 * Rollback any changes made on the given connection. A null value of connection is legal.
	 *
	 * SQLException - if a database access error occurs
	 *
	 * @throws SQLException
	 */
	public void rollback() throws SQLException {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.rollback();
			}
		} catch (SQLException e) {
			handler.handle(connection, e, _log);
		}
	}


	/**
	 * Get all desired column names by passing desired table name
	 * @param tableName
	 * @return
	 */
	public String getColumnNames(String tableName) {
		ResultSet rs = this.query("SELECT * FROM " + tableName + " where 0 = 1");
		StringBuilder sb = new StringBuilder();
		try {
			int columnCount = rs.getMetaData().getColumnCount();
			for (int i = 1; i < columnCount + 1; i++ ) {
				sb.append(rs.getMetaData().getColumnName(i)).append( i == columnCount ? "" : ",");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return "(" + sb.toString() + ")";
	}


	/**
	 * Execute SQL statements in batch. 
	 * This batch is different from conventional batch offered by JDBC driver
	 * 
	 * @param queryString- A semicolon separated list of queries
	 * 
	 * @return
	 */
	public long[] batch(final String queryString)  throws UdfException {
		try{
			ArrayList<String> list = new ArrayList<String>(Arrays.asList(queryString.trim().split(";")));
			PreparedStatement mStmt = null;
			long result[] = new long[list.size()];
			int i=0;
			for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
				String query = (String) iterator.next();

				_log.info("query is "+ query);
				mStmt = connection.prepareStatement(query);
				if (connection == null) {
					handler.handle("Null connection", new SQLException());
				}
				if (query == null || query.isEmpty()) {
					handler.handle(connection, "Null SQL statement", new SQLException());
				}
				if(query.equalsIgnoreCase("COMMIT")){
					commit();
				}
				else{
					if(query.trim().toUpperCase().startsWith("COPY")){
						String copyTable=query.trim().replaceAll("\\s+", " ");
						copyTable=copyTable.split(" ")[1];
						_log.info("Copy statement: " + query.trim());
						result[i]=executeCopy(copyTable, query.contains(";") ? query : (query + ";"));
						_log.info("Successfully executed copy statement ...");
						continue;
					}
					else if (query.trim().toUpperCase().startsWith("TRUNCATE")){
						truncateWithAdmin(query);
					}
					else{
						result[i]=mStmt.executeUpdate();
					}
				}
				commit();
				i++;
			}
			return result;
		} catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return null;
	}

	public static void main(String[] args) throws UdfException, SQLException {
		QueryExecutor executor;
			executor = new QueryExecutor(ConnectionManager.getConnection("edw"), UDXLogFactory.getEDWLogger());
			// executor.executeCopy("copy EDW.S_NRISR_T0805_BANK_BRANCH_CHANNEL_770( BRANCH_CODE ,BRANCH_BANK_CODE ,BRANCH_NAME ,BRANCH_BANK_NAME ,BRANCH_ADDRESS1 ,BRANCH_ADDRESS2 ,BRANCH_ADDRESS3 ,BRANCH_STATE ,BRANCH_CITY ,BRANCH_COUNTRY ,BRANCH_PINCODE ,BRANCH_STATE_CODE ,BRANCH_CITY_CODE ,BRANCH_COUNTRY_CODE ,BRANCH_DD_ISSUE ,BRANCH_FINACLE_FLAG ,BRANCH_ENABLED ,ROW_START_DATE ,ROW_END_DATE ,SOURCE_SYSTEM_CODE ,DATE_CREATED  )  from '/vertica_load/LOAD_MANAGEMENT/NRISR/EVENT/YET_TO_LOAD_FILES/NRISR_T0805.out' Delimiter '~'  NO ESCAPE ");
			executor.executeCopy("copy SRCSTAGE.S_TD_ACCT_MASTER_TABLE_ADDON( ACID ,DEPOSIT_PERIOD_MTHS ,DEPOSIT_PERIOD_DAYS ,OPEN_EFFECTIVE_DATE ,MATURITY_DATE ,DEPOSIT_AMOUNT ,MATURITY_AMOUNT ,ADJUSTED_COMM_AMT ,DEPOSIT_STATUS ,ACCT_SEGMENT ,SAFE_CUSTODY_FLG ,NOMINEE_PRINT_FLG ,PRINTING_FLG ,SPL_CATG_IND ,XFER_IN_IND ,LAST_INT_PROVISION_DATE ,LAST_REPAYMENT_DATE ,CUMULATIVE_PRINCIPAL ,CUMULATIVE_INSTL_PAID ,CUMULATIVE_REPAYMENT_PAID ,CUMULATIVE_INT_PAID ,CUMULATIVE_INT_CREDITED ,INT_ACCRUAL_FLG ,RELATED_ACID ,PENAL_PCNT ,REPAYMENT_ACID ,LOAN_ACID ,PENALTY_AMOUNT ,PENALTY_RECOVERED ,PENALTY_WAIVED ,AGENT_EMP_IND ,AGENT_CODE ,MATURITY_NOTICE_DATE ,LCHG_USER_ID ,LCHG_TIME ,RCRE_USER_ID ,RCRE_TIME ,TDS_AMT ,INT_CR_RATE_CODE ,NOSTRO_VALUE_DATE ,OVERDUE_INT_AMT ,CLS_VALUE_DATE ,AUTO_RENEWAL_FLG ,PERD_MTHS_FOR_AUTO_RENEW ,PERD_DAYS_FOR_AUTO_RENEW ,MAX_AUTO_RENEWAL_ALLOWED ,AUTO_RENEWED_COUNTER ,CLOSE_ON_MATURITY_FLG ,AUTO_RENEWAL_SCHM_CODE ,AUTO_RENEWAL_INT_TBL_CODE ,AUTO_RENWL_GL_SUBHEAD_CODE ,RENEWAL_CRNCY ,RENEWAL_RATE_CODE ,RENEWAL_RATE ,TS_CNT ,SOL_ID ,ORIGINAL_DEPOSIT_AMOUNT ,NOTICE_PERIOD_MNTHS ,NOTICE_PERIOD_DAYS ,NOTICE_DATE ,ACCT_CLOSE_INTEREST_RATE ,TRAN_ID ,TXOD_REGL_OVERDRAFT ,ORIGINAL_MATURITY_AMOUNT ,REN_SRL_NUM ,DEPOSIT_TYPE ,LINK_OPER_ACCOUNT ,OUTFLOW_MULTIPLE_AMT ,AVAIL_DEPOSIT_AMT ,CUST_INST_TYPE ,TDS_TOTAL_FROM_SELF_ACCT ,INT_FLOW_FREQ_MTHS ,INT_FLOW_FREQ_DAYS ,FILLER ,PART_NAME  )  from "+
							" '/vertica_load/LOAD_MANAGEMENT/IAD/ACCOUNT/YET_TO_LOAD_FILES/IAD_TAM_PART18.out'    with parser MultiCharDelimiterParser(separator='~^')  NO ESCAPE");
	}

	public long executeCopy(final String query)  throws UdfException, SQLException {
		if(query.trim().toUpperCase().startsWith("COPY")){
			String copyTable=query.trim().replaceAll("\\s+", " ");
			copyTable=copyTable.split(" ")[1];
			copyTable =  copyTable.substring(0, copyTable.contains("(") ? copyTable.indexOf("(") : copyTable.length());
			return executeCopy(copyTable.replace("(", ""), query);
		}
		return 0;
	}

	/**
	 * We do not get row update count in case of copy statements
	 * Use below method to get row update count for copy statements
	 * @param tablename
	 * @param query
	 * @param params
	 * @return
	 * @throws UdfException
	 * @throws SQLException 
	 */
	public long executeCopy(final String tablename, String query, final Object... params) throws UdfException, SQLException{
		long initialRowCount = 0;
		try {
			query = CopyStmtGenerator.constructCopyStmt(connection, query, _log);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		initialRowCount = getLongValueFromQuery("SELECT count(*) FROM " + tablename);
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {

			_log.info("Query: " + query);
			fillStatement(mStmt, params);
			mStmt.executeQuery();
			long rowCount = getLongValueFromQuery("SELECT count(*) FROM " + tablename) - initialRowCount;

			_log.info("Total Records Affected: " + rowCount); 	

			return rowCount;
		}catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return 0;
	}

	/**
	 * Helper method to verify row count from select query
	 * @param query
	 * @param params
	 * @return
	 */
	public boolean isResultSetEmpty(final String query, final Object ... params)  throws UdfException, SQLException {
		ResultSet rs = query(query, params);
		if (rs == null || !rs.next()){
			DatabaseUtil.close(rs);
			return true;
		}
		else{
			DatabaseUtil.close(rs);
			return false;
		}
	}

	public int grant(final String query) throws UdfException {
		if (connection == null) {
			handler.handle("Null connection", new SQLException());
		}
		if (query == null || query.isEmpty()) {
			handler.handle(connection, "Null SQL statement", new SQLException());
		}
		try (PreparedStatement mStmt = connection.prepareStatement(query);) {
			_log.info("Query: " + query);
			int count = mStmt.executeUpdate();
			return count;
		} catch (SQLException se) {
			handler.handle(connection, se, _log);
		}
		return 0;
	}

	public void analyzeConstraints(final String tableName) throws UdfException {
		try {
			long startTime = System.currentTimeMillis();
			if (ApplicationProperties.getInstance().enableAnalyzeConstraint()){
				if (!this.isResultSetEmpty("select analyze_constraints(" + "'" + tableName + "')")){
					// constraints violated for table under consideration
					_log.info("Time taken to analyze constraints: " + (System.currentTimeMillis() - startTime));
					this.rollback();
					throw new UdfException(ErrorEnum.CONSTRAINTS_VIOLATED.getCode(), ErrorEnum.CONSTRAINTS_VIOLATED.getMessage());
				}
				// no constraints violated for table under consideration
				_log.info("Time taken to analyze constraints: " + (System.currentTimeMillis() - startTime));
			}
		}
		catch (SQLException ex) {
			if (ex.getErrorCode() == 4072){
				_log.info("No constraints defined on table.");
			}
		}
	}
	
	private static String getSchemaName(String table_name) {
		if (table_name.contains("."))
			return table_name.substring(0, table_name.indexOf("."));
		else
			return "";
	}
	
	public void commitAndClose(Connection conn) throws UdfException, SQLException {
		Integer memoryInKB = getIntValueFromQuery("SELECT JVM_MEMORY_KB FROM SESSIONS WHERE SESSION_ID = (SELECT SESSION_ID FROM CURRENT_SESSION)");
		_log.info("Memory before release : " + memoryInKB + " kb.");
		
		query("SELECT RELEASE_JVM_MEMORY()");
		
		memoryInKB = getIntValueFromQuery("SELECT JVM_MEMORY_KB FROM SESSIONS WHERE SESSION_ID = (SELECT SESSION_ID FROM CURRENT_SESSION)");
		_log.info("Memory after release : " + memoryInKB + " kb.");
		ConnectionManager.commitAndClose(conn);
	}
}
