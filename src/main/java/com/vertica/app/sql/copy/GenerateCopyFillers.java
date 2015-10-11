package com.vertica.app.sql.copy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.vertica.app.loader.ApplicationProperties;
import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.Constants;
import com.vertica.app.sql.engine.QueryExecutor;
import com.vertica.sdk.UdfException;
import com.vertica.util.ExceptionUtil;
import com.vertica.util.StringUtil;

public class GenerateCopyFillers {

	/**
	 * 
	 * This is a generic implementation to meet several requirements 
	 * to generate a copy statement
	 * 
	 * This function can receive copy statement with or w/o fillers
	 * 
	 * In case any of the column has a filler associated with it, do not add any other explicit filler
	 * 
	 * 
	 * @param cn
	 * @param _log
	 * @param c_owner_name
	 * @param c_i_table_name
	 * @param c_user_col_list
	 * @return
	 */
	public static String generateFillers(final Connection cn,
			final Logger _log, String c_owner_name,
			final String c_i_table_name, final String c_user_col_list) {

		try {
			QueryExecutor executor = new QueryExecutor(cn, _log);

			Long i_o_table_id = null;
			if(c_owner_name.equalsIgnoreCase("public")){
				c_owner_name = ApplicationProperties.getInstance().getValue("pwr_user_name");
			}
			i_o_table_id = executor.getLongValueFromQuery(Constants.GET_MAX_TABLE_ID, c_owner_name, c_i_table_name);
			ResultSet RsCur = null;
			RsCur = executor.query("select a.column_name, a.data_type from v_catalog.columns a where a.table_id=?", i_o_table_id);

			// Hash map with column name as key and data type & precision as value 
			LinkedHashMap<String, String> col_name_datatype = new LinkedHashMap<String, String>();

			while (true) {
				String SqlState = "00000";
				if (!RsCur.next())
					SqlState = "02000";

				Long i_state = new Long(SqlState);

				if (i_state.longValue() != Integer.valueOf("00000")) {
					try {
						RsCur.close();
					} catch (SQLException se) {
						throw new UdfException(se.getErrorCode(),
								se.getMessage());
					}
					break;
				}
				col_name_datatype.put(RsCur.getString(1), RsCur.getString(2));
			}

			String[] colArray = c_user_col_list.split(",");
			String[] copy_filler_text = new String[colArray.length];
			String colDatatype = "";
			//If user column list has FILLER columns (could be from core procedure or column named FILLER) use that column list as is
			if (c_user_col_list.contains("filler") || c_user_col_list.contains("FILLER")) {
				for (int i = 0; i < colArray.length; i++) {
					// If column is FILLER, process as is
					if (colArray[i].contains("filler") || colArray[i].contains("FILLER")) {
						copy_filler_text[i] = colArray[i];
					} 
					else {// If column is not FILLER then get data-type and append fillers accordingly
						colDatatype = col_name_datatype.get(colArray[i].trim());
						copy_filler_text[i] = appendFillers(colArray[i].trim(),	colDatatype);
					}
				}
			} else {//Take column list from DB query and create key set on column names 
				Set<String> keySet = (col_name_datatype.keySet().size() == c_user_col_list.split(",").length) ? col_name_datatype.keySet() 
						: new LinkedHashSet<String>(Arrays.asList(c_user_col_list.split(",")));

				int i = 0;
				for (Iterator<String> iterator = keySet.iterator(); iterator
						.hasNext();) {
					String next = iterator.next().trim();
					colDatatype = col_name_datatype.get(next);
					copy_filler_text[i] = appendFillers(next, colDatatype);
					i++;
				}
			}
			_log.info("Columns: " + StringUtil.joinString(copy_filler_text, ","));

			// ConnectionManager.commitAndClose(cn);
			cn.commit();
			return "(" + StringUtil.joinString(copy_filler_text, ",") + ")";
		} catch (SQLException e) {
			UDXLogFactory.getEDWLogger().error(ExceptionUtil.getStringFromException(e));
		}
		return null;
	}

	/**
	 * Add fillers for each of the desired data types.
	 * This is needed to match Sybase IQ 16 behavior
	 * 
	 * Varchar- Blank spaces in file are loaded as null in Sybase whereas Vertica retains whatever it recieved from file
	 * 
	 * @param colName
	 * @param colDatatype
	 * @return
	 */
	private static String appendFillers(String colName, String colDatatype) {
		if(colDatatype.contains("varchar") || colDatatype.contains("Varchar")){
			StringBuilder sb = new StringBuilder();
			//sb.append(copy_filler_text);
			sb.append(colName);
			sb.append("_TEMP_COL FILLER ");
			sb.append(colDatatype);
			sb.append(", ");
			sb.append("\\\""+colName +"\\\"");
			sb.append(" AS CASE WHEN ");
			sb.append("TRIM(" + colName);
			sb.append("_TEMP_COL) = '' THEN NULL ELSE ");
			sb.append(" TRIM( TRAILING FROM ");
			sb.append(colName);
			sb.append("_TEMP_COL) END");
			return sb.toString();
		}else if (colDatatype.contains("char") || colDatatype.contains("Char") ){
			StringBuilder sb = new StringBuilder();
			//sb.append(copy_filler_text);
			sb.append(colName);
			sb.append("_TEMP_COL FILLER ");
			sb.append(colDatatype);
			sb.append(", ");
			sb.append("\\\""+colName +"\\\"");
			sb.append(" AS CASE WHEN ");
			sb.append(colName);
			sb.append("_TEMP_COL IS NULL THEN '' ELSE ");
			sb.append(colName);
			sb.append("_TEMP_COL END");
			return sb.toString();
		}
		else if(colDatatype.contains("int") || colDatatype.contains("Integer")){
			StringBuilder sb = new StringBuilder();
			// sb.append(copy_filler_text);
			sb.append(colName);
			// FLOAT doesn't give correct results for higher number range :(. Need to have a decimal filler
			// sb.append("_TEMP_COL FILLER FLOAT, ");
			sb.append("_TEMP_COL FILLER DECIMAL, ");
			sb.append("\\\""+colName +"\\\"");
			sb.append(" AS CAST (");
			sb.append(colName);
			sb.append("_TEMP_COL AS INTEGER)");
			return sb.toString();
		}
		/*else if(colDatatype.contains("timestamp") || colDatatype.contains("Timestamp")) {
			StringBuilder sb = new StringBuilder();
			sb.append(colName);
			sb.append("_TEMP_COL FILLER VARCHAR(50), ");
			sb.append("\\\""+colName +"\\\"");
			sb.append(" AS CASE WHEN ");
			sb.append("REGEXP_LIKE(");
			sb.append(colName);
			sb.append("_TEMP_COL, '^[a-zA-Z]') THEN NULL ELSE CAST (");
			sb.append(colName);
			sb.append("_TEMP_COL AS TIMESTAMP) END");
			return sb.toString();
		}*/
		else{
			return "\\\""+colName +"\\\"";
		}
	}

}
