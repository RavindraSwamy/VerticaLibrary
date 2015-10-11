package com.vertica.util;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.vertica.sdk.UdfException;

/**
 * @author Abhishek Chaudhary
 *
 */
public class DatabaseUtil {

	/**
	 * Closes {@link Connection}s, {@link Statement}s, {@link ResultSet}s, and
	 * {@link Closeable}s. This method will close the objects in the order they
	 * are passed into this method, and will throw an
	 * {@link IllegalArgumentException} if any of the objects are not of the
	 * aforementioned type.
	 *
	 * @param r
	 *            The list of {@link Connection}s, {@link Statement}s,
	 *            {@link ResultSet}s, or {@link Closeable}s to close.
	 */
	  public static void close(Object... r)
	  {
	    for(final Object closeable : r)
	    {
	      if(closeable instanceof ResultSet)
	        closeResultSet(closeable);
	      else if(closeable instanceof Statement)
	        closeStatement(closeable);
	      else if(closeable instanceof Connection)
	        closeConnection(closeable);
	      else if(closeable instanceof Closeable)
	        try
	        {
	          ((Closeable)closeable).close();
	        }
	        catch(Throwable e)
	        { /* eat */
	        }
	      else if(closeable != null)
	        throw new IllegalArgumentException(
	            "Unable to close object of type: "
	                + closeable.getClass().toString()
	                + ".  DatabaseUtil.close(Object...) only closes objects of type: java.sql.ResultSet, java.sql.Statement, java.sql.Connection, or com.nielsen.sql.Closeable.");
	    }
	  }

	  private static void closeConnection(final Object closeable)
	  {
	    try
	    {
	      ((Connection)closeable).close();
	    }
	    catch(SQLException e)
	    {
	    	throw new UdfException(e.getErrorCode(), e.getMessage());
	    }
	  }

	  private static void closeStatement(final Object closeable)
	  {
	    try
	    {
	      ((Statement)closeable).close();
	    }
	    catch(SQLException e)
	    {
	    	throw new UdfException(e.getErrorCode(), e.getMessage());
	    }
	  }

	  private static void closeResultSet(final Object closeable)
	  {
	    try
	    {
	      ((ResultSet)closeable).close();
	    }
	    catch(SQLException e)
	    {
	    	throw new UdfException(e.getErrorCode(), e.getMessage());
	    }
	  }

	public static void printResultSet(final ResultSet rs) throws SQLException {
		final ResultSetMetaData metaData = rs.getMetaData();
	
		for (int i = 0 ; i < metaData.getColumnCount() ; i++){
			System.out.print((i != 0 ? "+" : "") + metaData.getColumnLabel(i + 1).toUpperCase());
		}
		System.out.println();
		System.out.println("----------------------------------");
		while(rs.next()){
			for (int j = 0; j < metaData.getColumnCount(); j++) {
				System.out.print( (j != 0 ? "+"  : "") + rs.getObject(j + 1));
			}
			System.out.println();
		}
		System.out.println("----------------------------------");
	
	}
}
