package com.vertica.app.sql.copy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.vertica.app.loader.ApplicationProperties;
import com.vertica.util.StringUtil;

public class CopyStmtGenerator {

	/**
	 * Converts old-syntax copy statement (using multi-character delimiter parser)
	 * to desired copy statement
	 * @param connection 
	 * 
	 * @param copyStmt
	 * @param _log 
	 * @return
	 * @throws SQLException 
	 */
	public static String constructCopyStmt(final Connection connection, String copyStmt, Logger _log) throws SQLException  {

		String additional_clauses = "";
		String table_name_w_schema = "";
		String multi_char_delim = "";
		String trg_delim = "\\x1E";
		String column_list = "";
		String file_location = "";
		int column_count =0; 
		String c_quote = String.valueOf((char) 39);

		// Get table name
		String pattern = "COPY\\s*\\w+\\.?\\w+\\s*\\(\\s*\\n*";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		// Now create matcher object.
		Matcher m = r.matcher(copyStmt);
		if (m.find()) {
			table_name_w_schema = m.group(0);
			table_name_w_schema = table_name_w_schema.replace("copy", "").replace("COPY", "").replace("(", "").trim().toUpperCase();
			_log.info("Found value: " + table_name_w_schema );
		} else {
			_log.info("NO MATCH");
		}

		m.reset();

		pattern = "\\(([^()]*|\\([^()]*\\))*\\)";
		// Create a Pattern object
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		// Now create matcher object.
		m = r.matcher(copyStmt);
		
		if (m.find()) {
			column_list = StringUtil.replaceLast(m.group(0).replaceFirst("\\(", ""), ")", "");
			column_count = column_list.split(",").length;
			
			column_list = GenerateCopyFillers.generateFillers(connection, _log,
							getSchemaName(table_name_w_schema),
							getTableName(table_name_w_schema), column_list) ;
			column_list = column_list.replace("'", "''");

		} else {
			_log.info("NO MATCH");
		}

		
		pattern = "\\s*SEPARATOR\\s*\\=\\s*\\W+\\'";

		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			multi_char_delim = m.group(0);
			multi_char_delim = multi_char_delim.substring(multi_char_delim.indexOf("=") + 1, multi_char_delim.length()).replace("'", "");
			_log.info("Found value: " + multi_char_delim );
		} else {
			m.reset();

			pattern = "\\s*Delimiter\\s*\\W+\\'";
			r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			m = r.matcher(copyStmt);
			if (m.find()) {
				multi_char_delim = m.group(0);
				multi_char_delim = multi_char_delim.substring(multi_char_delim.indexOf("'") + 1, multi_char_delim.length()).replace("'", "");
				_log.info("Found value: " + multi_char_delim );
			} else {
				m.reset();
				pattern = "\\s*DELIMITER\\s*AS\\W+\\'";
				r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
				m = r.matcher(copyStmt);
				if (m.find()) {
					multi_char_delim = m.group(0);
					multi_char_delim = multi_char_delim.substring(multi_char_delim.indexOf("'") + 1, multi_char_delim.length()).replace("'", "");
					_log.info("Found value: " + multi_char_delim );
				} else {
					_log.info("NO MATCH");
				}
			}
		}
		
		m.reset();

		pattern = "\\s*TRG_DELIMITER\\s*(AS)?\\W*\\'\\\\\\w*\\'";
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			trg_delim = m.group(0);
			trg_delim = trg_delim.substring(trg_delim.indexOf("'") + 1, trg_delim.length()).replace("'", "");
			_log.info("Found value: " + trg_delim );
		} else {
			_log.info("Found value: " +trg_delim);
			
		}
		
		m.reset();
		//pattern = "\\s*\\'[^']+\\'\\s";
		pattern = "\\s*\\'(.*?)\\'\\S*";
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			file_location = m.group(0);
			_log.info("Found value: " + file_location );
		} else {
			_log.info("NO MATCH");
		}

		m.reset();

		pattern = "\\s*NO ESCAPE\\s*";
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			additional_clauses = m.group(0);
		}

		m.reset();

		pattern = "\\s*ABORT ON ERROR\\s*";
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			additional_clauses = additional_clauses + " " +  m.group(0);
		}

		m.reset();

		pattern = "\\s*NO COMMIT\\s*";
		r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		m = r.matcher(copyStmt);
		if (m.find()) {
			additional_clauses = additional_clauses + " " +  m.group(0);
		}
		
		m.reset();
		
		additional_clauses = additional_clauses.replaceAll("\\s+", " ").trim();

		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("SELECT PUBLIC.COPY_MANAGER('");
		stringBuilder.append(table_name_w_schema);
		stringBuilder.append("', ");
		stringBuilder.append(file_location.replace("','", " ")  );
		stringBuilder.append(",");
		stringBuilder.append(c_quote + (!multi_char_delim.startsWith("|") ? ("\\" + multi_char_delim) : multi_char_delim.replace("|", "[|]"))  + c_quote);
		stringBuilder.append("," + c_quote +  trg_delim + c_quote);
		stringBuilder.append(",");
		stringBuilder.append(c_quote + column_list + c_quote);
		stringBuilder.append(", ");
		stringBuilder.append(c_quote+ additional_clauses + c_quote);
		stringBuilder.append(", ");
		stringBuilder.append(column_count);
		stringBuilder.append(", ");
		stringBuilder.append(c_quote + connection.getMetaData().getUserName() + c_quote); // user
		stringBuilder.append(", ");
		stringBuilder.append(c_quote + getAuthDetail(connection) + c_quote); // password
		stringBuilder.append(");");
		String copy_clause = stringBuilder.toString();

		return copy_clause;
	}


	private static String getAuthDetail(final Connection connection)
			throws SQLException {
		return ApplicationProperties.getInstance().getValue(connection.getMetaData().getUserName().toLowerCase()+ "_password");
	}
	

	private static String getTableName(String table_name) {
		return table_name.substring(table_name.indexOf(".")+1, table_name.length());
	}

	private static String getSchemaName(String table_name) {
		return table_name.substring(0, table_name.indexOf("."));
	}

	
}
