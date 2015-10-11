package com.icici.vertica.entity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;


public class SystemFunctions {

	public static HashSet<String> sys_functionList = new HashSet<String>(	Arrays.asList("AVG", "TRUNCATE", "ABS", "ACOS", "ASCII", "ASIN",	"ATAN", "ATAN2", "AVG", "BIGINTTOHEX", "BIT_LENGTH",	"BYTE_LENGTH", "CAST", "CEIL", "CEILING", "CHAR",	"CHAR_LENGTH", "CHARINDEX", "COL_LENGTH", "COL_NAME",	"CONNECTION_PROPERTY", "CONVERT", "CORR", "COS", "COT",	"COUNT", "COUNT", "COUNT", "COVAR_POP", "COVAR_SAMP",	"CUME_DIST", "DATALENGTH", "DATE", "DATEADD",	"DATECEILING", "DATEDIFF", "DATEFLOOR", "DATEFORMAT",	"DATENAME", "DATEPART", "DATEROUND", "DATETIME", "DAY",	"DAYNAME", "DAYS", "DAYS", "DAYS", "DB_ID", "DB_NAME",	"DB_PROPERTY", "DEGREES", "DENSE_RANK", "DIFFERENCE",	"DOW", "EVENT_CONDITION", "EVENT_CONDITION_NAME",	"EVENT_PARAMETER", "EXP", "EXP_WEIGHTED_AVG",	"FIRST_VALUE", "FLOOR", "GETDATE", "GRAPHICAL_PLAN",	"GROUP_MEMBER", "GROUPING *", "HEXTOBIGINT", "HEXTOINT",	"HOUR", "HOURS", "HOURS", "HOURS", "HTML_DECODE",	"HTML_ENCODE", "HTML_PLAN", "HTTP_DECODE", "HTTP_ENCODE",	"HTTP_VARIABLE", "INDEX_COL", "INSERTSTR", "INTTOHEX",	"ISDATE", "ISNUMERIC", "LAST_VALUE", "LCASE", "LEFT",	"LEN", "LENGTH", "LN", "LOCATE", "LOG", "LOG10", "LOWER",	"LTRIM", "MAX", "MAX", "MEDIAN", "MIN", "MIN", "MINUTE",	"MINUTES", "MINUTES", "MINUTES", "MOD", "MONTH",	"MONTHNAME", "MONTHS", "MONTHS", "MONTHS",	"NEXT_CONNECTION", "NEXT_DATABASE", "NEXT_HTTP_HEADER",	"NEXT_HTTP_VARIABLE", "NOW", "NTILE", "OBJECT_ID",	"OBJECT_NAME", "OCTET_LENGTH", "PATINDEX", "PERCENT_RANK",	"PERCENTILE_CONT", "PERCENTILE_DISC", "PI", "POWER",	"PROPERTY", "PROPERTY_DESCRIPTION", "PROPERTY_NAME",	"PROPERTY_NUMBER", "QUARTER", "RADIANS", "RAND", "RANK",	"REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",	"REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY",	"REGR_SYY", "REMAINDER", "REPEAT", "REPLACE", "REPLICATE",	"REVERSE", "RIGHT", "ROUND", "RTRIM", "SECOND", "SECONDS",	"SECONDS", "SECONDS", "SIGN", "SIMILAR", "SIN", "SORTKEY",	"SOUNDEX", "SPACE", "SQRT", "SQUARE", "STDDEV",	"STDDEV_POP", "STDDEV_SAMP", "STR", "STR_REPLACE",	"STRING", "STUFF", "SUBSTRING", "SUM", "SUM", "SUSER_ID",	"SUSER_NAME", "TAN", "TODAY", "TRIM", "TRUNCNUM",	"ts_arma_ar", "ts_arma_const", "ts_arma_ma",	"ts_auto_uni_ar", "ts_autocorrelation", "ts_box_cox_XFORM",	"ts_difference", "ts_estimate_missing", "ts_lack_of_fit",	"ts_lack_of_fit_p", "ts_max_arma_ar", "ts_max_arma_const",	"ts_max_arma_likelihood", "ts_max_arma_ma",	"ts_outlier_identification", "ts_partial_autocorrelation",	"ts_vwap", "UCASE", "UPPER", "USER_ID", "USER_NAME",	"VAR_POP", "VAR_SAMP", "VARIANCE", "WEEKS", "WEEKS",	"WEEKS", "WEIGHTED_AVG", "WIDTH_BUCKET", "YEAR", "YEARS",	"YEARS", "YEARS", "YMD"));
	public static HashSet<String> noDirectWorkAround = new HashSet<String>(Arrays.asList("CONNECTION_PROPERTY", "DB_PROPERTY", "HOURS", "MINUTES", "MONTHNAME", "NEXT_CONNECTION", "NUMBER", "OBJECT_ID", "OBJECT_NAME", "REVERSE", "ROWID", "SECONDS", "SPACE", "SUSER_NAME", "USER_ID", "USER_NAME", "WEEKS", "YEARS", "CHAR", "LIST", "DATEADD", "DATETIME", "DB_NAME"));
	public static HashSet<String> sybaseIQ16Functions_fromOnlineDocumentation = new HashSet<String>(Arrays.asList("ABS", "ACOS", "ARGN", "ASCII", "ASIN", "ATAN",	"ATAN2", "AVG", "BFILE", "BIGINTTOHEX", "BIT_LENGTH",	"BYTE_LENGTH", "BYTE_LENGTH64", "BYTE_SUBSTR64", "CAST",	"CEIL", "CEILING", "CHAR", "CHAR_LENGTH", "CHAR_LENGTH64",	"CHARINDEX", "COALESCE", "COL_LENGTH", "COL_NAME",	"CONNECTION_PROPERTY", "CONVERT", "CORR", "COS", "COT",	"COUNT", "COVAR_POP", "COVAR_SAMP", "CUME_DIST",	"DATALENGTH", "DATE", "DATEADD", "DATECEILING", "DATEDIFF",	"DATEFLOOR", "DATEFORMAT", "DATENAME", "DATEPART",	"DATEROUND", "DATETIME", "DAY", "DAYNAME", "DAYS", "DB_ID",	"DB_NAME", "DB_PROPERTY", "DEGREES", "DENSE_RANK",	"DIFFERENCE", "DOW", "ERRORMSG", "EVENT_CONDITION",	"EVENT_CONDITION_NAME", "EVENT_PARAMETER", "EXP",	"EXP_WEIGHTED_AVG", "FIRST_VALUE", "FLOOR", "GETDATE",	"GRAPHICAL_PLAN", "GROUP_MEMBER", "GROUPING",	"HEXTOBIGINT", "HEXTOINT", "HOUR", "HOURS", "HTML_DECODE",	"HTML_ENCODE", "HTML_PLAN", "HTTP_DECODE", "HTTP_ENCODE",	"HTTP_HEADER", "HTTP_VARIABLE", "IFNULL", "INDEX_COL",	"INSERTSTR", "INTTOHEX", "ISDATE", "ISNULL", "ISNUMERIC",	"LAG", "LAST_VALUE", "LCASE", "LEAD", "LEFT", "LEN",	"LENGTH", "LIST", "LN", "LOCATE", "LOG", "LOG10", "LOWER",	"LTRIM", "MAX", "MEDIAN", "MIN", "MINUTE", "MINUTES",	"MOD", "MONTH", "MONTHNAME", "MONTHS", "NEWID",	"NEXT_CONNECTION", "NEXT_DATABASE", "NEXT_HTTP_HEADER",	"NEXT_HTTP_VARIABLE", "NOW", "NTILE", "NULLIF", "NUMBER",	"OBJECT_ID", "OBJECT_NAME", "OCTET_LENGTH", "PATINDEX",	"PERCENT_RANK", "PERCENTILE_CONT", "PERCENTILE_DISC", "PI",	"POWER", "PROPERTY", "PROPERTY_DESCRIPTION",	"PROPERTY_NAME", "PROPERTY_NUMBER", "QUARTER", "RADIANS",	"RAND", "RANK", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT",	"REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX",	"REGR_SXY", "REGR_SYY", "REMAINDER", "REPEAT", "REPLACE",	"REPLICATE", "REVERSE", "RIGHT", "ROUND", "ROW_NUMBER",	"ROWID", "RTRIM", "SECOND", "SECONDS", "SIGN", "SIMILAR",	"SIN", "SORTKEY", "SOUNDEX", "SP_HAS_ROLE", "SPACE",	"SQLFLAGGER", "SQRT", "SQUARE", "STDDEV", "STDDEV_POP",	"STDDEV_SAMP", "STR", "STR_REPLACE", "STRING", "STRTOUUID",	"STUFF", "SUBSTRING", "SUBSTRING64", "SUM", "SUSER_ID",	"SUSER_NAME", "TAN", "TODAY", "TRIM", "TRUNCNUM", "UCASE",	"UPPER", "USER_ID", "USER_NAME", "UUIDTOSTR", "VAR_POP",	"VAR_SAMP", "VARIANCE", "WEEKS", "WEIGHTED_AVG",	"WIDTH_BUCKET", "YEAR", "YEARS", "YMD"));

	public static void main(String[] args) {
		System.out.println(sybaseIQ16Functions_fromOnlineDocumentation.contains("TRIM"));
	}

	public static HashSet<String> getEntityNames(String mine) {
		HashSet<String> sysFunctions = new HashSet<String>();
		for (Iterator<String> iterator = SystemFunctions.sybaseIQ16Functions_fromOnlineDocumentation.iterator(); iterator.hasNext();) {
			String type = iterator.next();
			if ( mine.toUpperCase().contains(" " + type + "(")
					|| mine.toUpperCase().contains("(" + type + "(")) {
				sysFunctions.add(type);
			}
		}
		return sysFunctions;
	}

	public static HashSet<String> getNoDirectWorkAroundsEntityNames(String mine) {
		HashSet<String> noDirectWorkAroundFunctions = new HashSet<String>();
		for (Iterator<String> iterator = SystemFunctions.noDirectWorkAround.iterator(); iterator.hasNext();) {
			String type = iterator.next();
			if (mine.toUpperCase().contains(" " + type + "(") || mine.toUpperCase().contains("(" + type + "(")){
				noDirectWorkAroundFunctions.add(type);
			}
		}
		return noDirectWorkAroundFunctions;
	}



}
