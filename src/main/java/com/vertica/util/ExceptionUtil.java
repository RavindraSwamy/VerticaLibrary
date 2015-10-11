package com.vertica.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;

public class ExceptionUtil {

	/**
	 * Get string representation of an entire stack trace
	 * @param e
	 * @return
	 */
	public static String getStringFromException(Exception e) {
		Writer result = new StringWriter();
		PrintWriter printWriter = new PrintWriter(result);
		e.printStackTrace(printWriter);
		return result.toString();
	}

	public static String getBriefException(Exception e){
		String result = getStringFromException(e);
		return result.substring(0, result.indexOf("\n"));
	}

	/**
     * Print the stack trace for a SQLException to STDERR.
     *
     * @param e SQLException to print stack trace of
     */
    public static void printStackTrace(SQLException e) {
        printStackTrace(e, new PrintWriter(System.err));
    }

    /**
     * Print the stack trace for a SQLException to a
     * specified PrintWriter.
     *
     * @param e SQLException to print stack trace of
     * @param pw PrintWriter to print to
     */
    public static void printStackTrace(SQLException e, PrintWriter pw) {
        SQLException next = e;
        while (next != null) {
            next.printStackTrace(pw);
            next = next.getNextException();
            if (next != null) {
                pw.println("Next SQLException:");
            }
        }
    }


}
