package com.vertica.app.sql.builder;

import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.vertica.util.StringUtil;

/**
 * A collection of helper methods for building Vertica SQLs. This class is
 * thread safe.
 *
 */
public class QueryBuilder {

	/**
	 * Create SQL for temporary table creation The default commit mode is
	 * "PRESERVE"
	 *
	 * @param tableName
	 * @param selectQuery
	 * @return
	 */
	public static StringBuilder createLocalTemporaryTable(
			final String tableName, final String selectQuery) {
		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE LOCAL TEMPORARY TABLE " + tableName
				+ " ON COMMIT PRESERVE ROWS AS ");
		sb.append(selectQuery);
		return sb;
	}

	/**
	 * INSERTS INTO A TARGET TABLE BY PERFORMING A SELECT * ON SOURCE TABLE THE
	 * SELECT CLAUSE MAKES USE OF WHERE CLAUSE
	 *
	 * @param target
	 * @param source
	 * @param whereClause
	 * @return
	 */
	public static StringBuilder insertIntoTable(final String target,
			final String source, final String whereClause) {
		final StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(target);
		sb.append(" SELECT * FROM ");
		sb.append(source);
		if (whereClause != null){
			sb.append(" WHERE ");
			sb.append(whereClause);
		}
		return sb;
	}

	/**
	 * INSERTS INTO A TARGET TABLE BY PERFORMING A SELECT * ON SOURCE TABLE THE
	 * SELECT CLAUSE MAKES USE OF WHERE CLAUSE AND GROUP BY CLAUSE
	 *
	 * @param target
	 * @param source
	 * @param whereClause
	 * @param groupByClause
	 * @return
	 */
	public static StringBuilder insertIntoTable(final String target,
			final String source, final String whereClause,
			final String groupByClause) {
		final StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(target);
		sb.append(" SELECT * FROM ");
		sb.append(source);
		sb.append(" WHERE ");
		sb.append(whereClause);
		sb.append(" GROUP BY ");
		sb.append(groupByClause);
		return sb;
	}

	public static String truncate(final String tableName) {
		return "TRUNCATE TABLE " + tableName;
	}

	public static String drop(final String tableName) {
		return "DROP TABLE " + tableName;
	}

	public static StringBuilder makeInClause(final Appendable sb,
			final String column, final Collection<String> items) {
		return makeInClause(sb, column, items.toArray(new String[items.size()]));
	}

	public static StringBuilder makeInClause(final Appendable sb,
			final String column, final String... items) {
		return makeInClause(sb, column, Arrays.asList(items), Types.VARCHAR);
	}

	public static StringBuilder makeInClause(final Appendable sb,
			final String column, final Collection<String> items,
			final int sqlType) {
		final Iterator<String> it = items.iterator();
		try {
			sb.append('(');
			sb.append(column);

			if (items.size() == 1) {
				sb.append('=');
				formatItem(sb, it.next(), sqlType);
			} else {
				sb.append(" IN (");
				if (!it.hasNext())
					formatItem(sb, "", sqlType);
				else {
					int cnt = 0;
					while (it.hasNext()) {

						if (cnt > 999) // IN clause is limited to 1000 items
						{
							sb.append(") OR ");
							sb.append(column);
							sb.append(" IN (");
							cnt = 0;
						} else if (cnt > 0) {
							sb.append(',');
						}
						formatItem(sb, it.next(), sqlType);
						cnt++;
					}
				}
				sb.append(')');
			}
			return (StringBuilder) sb.append(')');
		} catch (final IOException e) {
			// not happening
		}
		return null;
	}

	public static void formatItem(final Appendable sb, final Object item,
			final int sqlType) {
		try {
			if (item == null) {
				sb.append("NULL");
			} else if (sqlType == Types.CHAR || sqlType == Types.VARCHAR) {
				sb.append('\'');
				final String s = item.toString();
				for (int ii = 0; ii < s.length(); ii++) {
					sb.append(s.charAt(ii));
					if (s.charAt(ii) == '\'')
						sb.append('\'');
				}
				sb.append('\'');
			} else {
				sb.append(item.toString()); // item is not null, see first if
			}
		} catch (final IOException ex) {
			// not happening
		}
	}

	public static String makeSelect(final StringBuilder sb,
			final Set<String> hints, Set<String> tables, Set<String> joins,
			StringBuilder selectColumns, final StringBuilder whereConditions,
			final Map<String, Set<String>> orConditions) {

		/*selectColumns = new StringBuilder("Employee.Name, Department.name");
		tables = new HashSet<>(Arrays.asList("Employee", "Department"));
		joins = new HashSet<>(Arrays.asList(
				"EMPLOYEE.ID = DEPARTMENT.EMPLOYEE_ID",
				"EMPLOYEE.ID1 = DEPARTMENT.EMPLOYEE_ID"));*/

		sb.append("SELECT ");
		if (hints != null && hints.size() > 0) {
			sb.append("/*+ ");
			for (final String hint : hints) {
				sb.append(hint);
				sb.append(' ');
			}
			sb.append("*/ ");
		}
		sb.append(selectColumns);
		sb.append(" FROM ");
		for (final String table : tables) {
			sb.append(table);
			sb.append(", ");
		}
		sb.setLength(sb.length() - 2);
		if (hasWhereConditionsOrJoins(whereConditions, joins)) {
			sb.append(" WHERE ");
			// must add conditions in order in case ? was used for parameterized
			// queries
			if (whereConditions.length() > 0) {
				sb.append(whereConditions);
				sb.append(" AND ");
			}
			if (joins != null){
				for (final String join : joins) {
					sb.append(join);
					sb.append(" AND ");
				}
			}

			if (orConditions != null) {
				final Iterator<Set<String>> it = orConditions.values()
						.iterator();
				while (it.hasNext()) {
					final Set<String> s = it.next();
					sb.append('(');
					final Iterator<String> it2 = s.iterator();
					while (it2.hasNext()) {
						sb.append(it2.next());
						sb.append(" OR ");
					}
					sb.replace(sb.length() - 4, sb.length(), ")");
					sb.append(" AND ");
				}
			}
			sb.setLength(sb.length() - 5); // remove last " AND "
		}
		return sb.toString();
	}

	private static boolean hasWhereConditionsOrJoins(
			final StringBuilder whereConditions, final Set<String> joins) {
		return (whereConditions.length() > 0 || joins.size() > 0);
	}

	public static String createLibrary(final String libraryName,
			final String library_path, final String support_path) {

		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE LIBRARY ").append(libraryName).append(" AS ")
				.append("'").append(library_path).append("'");

		if (StringUtil.isToken(support_path)) {
			sb.append(" DEPENDS ").append("'").append(support_path).append("'");
		}

		sb.append(" LANGUAGE ").append("'JAVA'");
		return sb.toString();
	}

	public static String alterLibrary(final String libraryName,
			final String library_path, final String support_path) {

		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER LIBRARY ").append(libraryName);

		sb.append(" AS ").append("'").append(library_path).append("'");

		if (StringUtil.isToken(support_path)) {
			sb.append(" DEPENDS ").append("'").append(support_path).append("'");
		}

		return sb.toString();
	}

	/**
	 *
	 * Fenced mode is enabled by default. Functions written in Java and R always
	 * run in fenced mode.
	 *
	 * @param function_name
	 * @param factory
	 * @param library_name
	 * @return
	 */
	public static String createFunction(final String function_name,
			final String factory, final String library_name) {
		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE FUNCTION ").append(function_name)
				.append(" AS LANGUAGE 'JAVA' NAME ")
				.append("'" + factory + "'").append(" LIBRARY ")
				.append(library_name).append(" FENCED");
		return sb.toString();
	}

	public static String dropFunction(final String function_name, final String args) {
		return "DROP FUNCTION " + function_name + "(" + args + ")";
	}

	public static String dropLibrary(boolean cascade, String library_name) {
		return "DROP LIBRARY " + library_name + (cascade ? " CASCADE" : "");
	}

	public static void bulkInsert(final String tableName, final String delimiter, String ... columns){
		final String tempTableName = "";
		final String create_temp_table = "CREATE LOCAL TEMPORARY TABLE " + tempTableName + " ";
		//"INSERT INTO " + tableName + " " + "SELECT " + construnct(delimiter, columns.length);
	}

	public static void main(String[] args) {
		constructSplitPart("^~^", 3);
	}
	public static String constructSplitPart(final String delimiter, int count){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			sb.append("SPLIT_PART(DUMMY, '" + delimiter + "' ," + (i +1)  + ")").append(i == (count-1)? "" : ", ");
		}
		System.out.println(sb);
		return sb.toString();
	}

}
