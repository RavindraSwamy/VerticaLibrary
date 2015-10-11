package com.vertica.util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.commons.lang.time.DateUtils;

import com.vertica.app.logging.UDXLogFactory;
import com.vertica.app.sql.engine.ConnectionManager;
import com.vertica.app.sql.engine.ConversionException;
import com.vertica.app.sql.engine.QueryExecutor;

public class DateUtil {

	public static Date getCurrentDate(Connection cn){
		QueryExecutor executor = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());
		Date date = executor.getDateValueFromQuery("SELECT CURRENT_DATE()");
		return date;
	}

	public static Timestamp getSysDate(Connection cn){
		QueryExecutor executor = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());
		Timestamp date = executor.getTimestampValueFromQuery("SELECT SYSDATE()");
		return (date);
	}

	public static Date getDate(){
		try(Connection cn = ConnectionManager.getConnection()) {
			QueryExecutor executor = new QueryExecutor(cn, UDXLogFactory.getGenericlogger());
			Timestamp date = executor.getTimestampValueFromQuery("SELECT GETDATE()");
			return utilDateToSqlDate(date);
		} catch (Exception e) {
			/*do nothing*/
		}
		return null;
	}

	public static Date utilDateToSqlDate(final java.util.Date uDate) {
		Date sDate = new Date(uDate.getTime());
		return sDate;
	}

	/**
	 * Dateadd, a date function, adds an interval to a specified date Takes
	 * three arguments: the date part, a number, and a date. The result is a
	 * datetime value equal to the date plus the number of date parts.
	 *
	 * @param date_part
	 * @param number
	 * @param date
	 * @return
	 */
	public static Date dateadd(final String date_part,
			final int number, final Date date) {
		System.out.println(date);

		java.util.Date increment = null;

		if (DatePartEnum.DAY.toString().equalsIgnoreCase(date_part)
				|| DatePartEnum.DD.toString().equalsIgnoreCase(date_part)) {
			increment = DateUtils.addDays(date, number);
		}

		if (DatePartEnum.MONTH.toString().equalsIgnoreCase(date_part)) {
			if (number > 0 && number <= 12) {
				increment = DateUtils.addMonths(date, number);
			}
		}
		System.out.println(increment);
		return utilDateToSqlDate(increment);
	}

	/**
	 * Converts string of format MMMM d, yyyy to sql date
	 *
	 * @param string
	 * @return
	 * @throws ParseException
	 * @throws ConversionException
	 */
	public static Date stringToDate(final String sourceDate)
			throws ParseException, ConversionException {
		//DateFormat format = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		java.util.Date date;
		try {
			date = format.parse(sourceDate);
		} catch (ParseException e) {
			throw new ParseException("Cannot parse input string to date", 0);
		}

		if (date == null) {
			throw new ConversionException(MessageFormat.format(
					"Could not convert ''{0}'' to a date", sourceDate));
		}
		return utilDateToSqlDate(date);
	}

	/**
	 * Converts string of given pattern to SQL date
	 *
	 * @param sourceDate
	 * @return
	 * @throws ParseException
	 * @throws ConversionException
	 */
	public static Date stringToDate(final String sourceDate,
			final String pattern) throws ParseException, ConversionException {
		DateFormat format = new SimpleDateFormat(pattern, Locale.ENGLISH);
		java.util.Date date;
		try {
			date = format.parse(sourceDate);
		} catch (ParseException e) {
			throw new ParseException("Cannot parse input string to date", 0);
		}
		if (date == null) {
			throw new ConversionException(MessageFormat.format(
					"Could not convert ''{0}'' to a date", sourceDate));
		}
		return utilDateToSqlDate(date);
	}

}
