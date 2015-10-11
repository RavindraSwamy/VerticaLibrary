package com.vertica.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static methods for doing common stuff to java.lang.String
 */
public final class StringUtil
{
	public static final String[] EMPTY_STRING_ARRAY = new String[0];

	public static final String REGEX_CROSS_PLATFORM_CRLF = "\r\n|\r|\n";

	public static final char CRLF = '\n';

	public static boolean isParameterMissing(final Object param)
	{
		return (param==null || "null".equals(param)
				|| (param instanceof Object[] && ((Object[])param).length<=0)
				|| (param instanceof Collection && ((Collection<?>)param).isEmpty())
				|| param.toString().length()<=0);
	}


	/**
	 * ignore case version of contains method
	 *
	 * @param source
	 * @param charSeq
	 * @return
	 */
	public static boolean containsIgnoreCase(final String source, final String charSeq){
		if (isToken(source) && isToken(charSeq)){
			if (source.toLowerCase().contains(charSeq.toLowerCase())){
				return true;
			}
		}
		return false;
	}
	/**
	 * Ensure that the string contains at least one character.
	 *
	 * @param s
	 * @return true if s is not null > 0 in length
	 */
	public static boolean isToken(final String s)
	{
		return s != null && !s.isEmpty();
	}

	/**
	 * Ensure that the string is not null. Any null string becomes an empty string.
	 *
	 * @param s
	 * @return
	 */
	public static String notNull(final String s)
	{
		return s != null ? s : "";
	}

	public static String makeNullIfEmpty(final String s)
	{
		return isEmpty(s) ? null : s;
	}

	/**
	 *
	 * @param commaDelimitedString
	 * @return
	 */
	public static String[] toArray(final String commaDelimitedString)
	{
		return toArray(commaDelimitedString, "\\,");
	}

	/**
	 * @param delimitedString
	 * @return
	 */
	public static String[] toArray(final String delimitedString, final String splitOn)
	{
		String[] rslt = null;
		if (delimitedString == null)
		{
			rslt = new String[] {};
		}
		else
		{
			rslt = delimitedString.split(splitOn);
			for (int i = rslt.length - 1; i >= 0; i--)
			{
				rslt[i] = rslt[i].trim();
			}
		}
		return rslt;
	}

	public static String replaceAll(final String s, final String charsToRemove, final String replacement)
	{
		// If no characters are going to be removed, don't transform the input string.
		if (!isToken(charsToRemove))
		{
			return s;
		}

		final StringBuilder regex = new StringBuilder();
		regex.append("[");

		for (int i = 0; i < charsToRemove.length(); i++)
		{
			regex.append("\\");
			regex.append(charsToRemove.substring(i, i + 1));
		}
		regex.append("]+");

		return s.replaceAll(regex.toString(), replacement);
	}

	/**
	 * Adds a slash on the left hand side of a string, if it does not already exist.
	 *
	 * @return
	 */
	public static String addStartingSlash(final String input)
	{
		if (null != input && !input.startsWith("/"))
		{
			return "/" + input;
		}
		return input;
	}

	/**
	 * Removes the slash from the left hand side of a string, if it exists.
	 *
	 * @param input
	 * @return
	 */
	public static String removeStartingSlash(final String input)
	{
		if (null != input && input.startsWith("/"))
		{
			return input.substring(1);
		}
		return input;
	}

	/**
	 * Adds a slash on the right hand side of a string, if it does not already exist.
	 *
	 * @return
	 */
	public static String addEndingSlash(final String input)
	{
		if (null != input && !input.endsWith("/"))
		{
			return input + "/";
		}
		return input;
	}

	/**
	 * Removes the slash from the right hand side of a string, if it exists.
	 *
	 * @param input
	 * @return
	 */
	public static String removeEndingSlash(final String input)
	{
		if (null != input && input.endsWith("/"))
		{
			return input.substring(0, input.length() - 1);
		}
		return input;
	}

	/**
	 * This method gets the first slash-delimited portion of the request path. This (should) tell us what the application
	 * is trying to do.
	 *
	 * @param requestPath
	 *          The value or request.getPathInfo().
	 * @param index
	 *          The 0-based part of the path array you would like back.
	 * @return The requested slash-delimited part of the parameter requestPath.
	 */
	public static String getPartOfRequestPath(final String requestPath, final int index)
	{
		return getPartOfRequestPath(requestPath, index, false);
	}

	/**
	 * Gets the first part of the slash-delimited request path. This (should) tell us what the application is trying to
	 * do.
	 *
	 * @param requestPath
	 *          The value of request.getPathInfo().
	 * @return The first slash-delimited part of the parameter requestPath.
	 */
	public static String getFirstPartOfRequestPath(final String requestPath)
	{
		return getPartOfRequestPath(requestPath, 0, false);
	}

	/**
	 * Gets the last part of the slash-delimited request path. This (should) tell us what filename is being specified on
	 * the path.
	 *
	 * @param requestPath
	 *          The value of a slash-delimited path.
	 * @return The first slash-delimited part of the parameter requestPath.
	 */
	public static String getLastPartOfRequestPath(final String requestPath)
	{
		return getPartOfRequestPath(requestPath, 0, true);
	}

	private static String getPartOfRequestPath(String requestPath, final int index, final boolean max)
	{
		// Empty strings.
		if (isEmpty(requestPath))
		{
			return "";
		}

		// Trim off opening and closing slashes.
		requestPath = removeStartingSlash(requestPath);
		requestPath = removeEndingSlash(requestPath);

		// Split the string into parts.
		final String[] requestPathArray = requestPath.split("/");

		// Check if the maximum index has been requested.
		if (max)
		{
			return (requestPathArray[requestPathArray.length - 1]);
		}

		// If the index is out of bounds, return an empty string.
		if (requestPathArray.length <= index)
		{
			return "";
		}

		return (requestPathArray[index]);
	}

	/**
	 * Given an absolute path, a relative path, or a simple file name, this method will return the file name portion of
	 * the path.
	 *
	 * @param path
	 * @return The file name portion of this path.
	 */
	public static String getLastPartOfFilePath(final String path)
	{
		// Empty strings.
		if (isEmpty(path))
		{
			return "";
		}

		// Split the string into parts.
		if (-1 != path.indexOf('\\'))
		{
			final String[] requestPathArray = path.split("\\\\");
			return (requestPathArray[requestPathArray.length - 1]);
		}

		if (-1 != path.indexOf('/'))
		{
			final String[] requestPathArray = path.split("/");
			return (requestPathArray[requestPathArray.length - 1]);
		}
		return path;
	}

	/**
	 * <p>
	 * Removed un-sightly blemishes, and Windows carriage-returns.
	 * <p>
	 * Each "CRLF" instantly becomes a simple new line "\n"
	 *
	 * @param s
	 * @return
	 */
	public static String removeCarriageReturns(final String s)
	{
		return toSingleLine(s, Character.toString(CRLF));
	}

	/**
	 * <p>
	 * Transform lines of text into their HTML equivalents by adding line breaks.
	 * </p>
	 * <p>
	 * See accompanying method "toMultiLine" to reverse the effect.
	 * </p>
	 *
	 * @param multiLineString
	 * @return
	 */
	public static String toSingleLineHTML(final String multiLineString)
	{
		return toSingleLine(multiLineString, "<br/>");
	}

	/**
	 * <p>
	 * Transform lines of text into a single line using the separator of your choice.
	 * </p>
	 * <p>
	 * See accompanying method "toMultiLine" to reverse the effect.
	 * </p>
	 *
	 * @param multiLineString
	 * @param separator
	 * @return
	 */
	public static String toSingleLine(final String multiLineString, final String separator)
	{
		return multiLineString.replaceAll(REGEX_CROSS_PLATFORM_CRLF, separator);
	}

	/**
	 * Transform lines of text into a single line using an empty string as replacement.
	 * @param multiLineString
	 * @return
	 */
	public static String toSingleLine(final String multiLineString)
	{
		return multiLineString.replaceAll(REGEX_CROSS_PLATFORM_CRLF,"");
	}

	/**
	 * <p>
	 * Transforms HTML <br/>
	 * tags into their text equivalent.
	 * </p>
	 * See accompanying method "toSingleLine" to reverse the effect. </p>
	 *
	 * @param singleLineString
	 * @return
	 */
	public static String toMultiLine(final String singleLineString)
	{
		return toMultiLine(singleLineString, "\n");
	}

	/**
	 * <p>
	 * Transforms HTML <br/>
	 * tags into multiple lines using the line break of your choice.
	 * </p>
	 * See accompanying method "toSingleLine" to reverse the effect. </p>
	 *
	 * @param singleLineString
	 * @param separator
	 * @return
	 */
	public static String toMultiLine(final String singleLineString, final String separator)
	{
		return singleLineString.replaceAll("<br/>|<br>", separator);
	}

	/**
	 * Is this value empty?
	 */
	public static boolean isEmpty(final String s)
	{
		if(s != null)
		{
			final String trimmed = s.trim();
			return trimmed.isEmpty();
		}
		return true;
	}

	/**
	 * Is this value empty or zero?
	 */
	public static boolean isEmptyOrZero(final String s)
	{
		if(!isEmpty(s))
		{
			final String trimmed = s.trim();
			return trimmed.equals("0");
		}
		return true;
	}

	/**
	 * <p>
	 * Verifies that this is a reasonably valid E-mail address.
	 * </p>
	 * <p>
	 * Ensures the address is not empty, contains at least one "@" symbol, and at least one period.
	 * </p>
	 *
	 * @param strVal
	 * @return
	 */
	public static boolean isReasonableEmailAddress(final String strVal)
	{
		return !isEmpty(strVal) && -1 != strVal.indexOf('@') && -1 != strVal.indexOf('.');
	}

	/**
	 * <p>
	 * Pad your string up to the specified length. Uses the character you specify.
	 * </p>
	 * <p>
	 * If your string is already at least the specified length, nothing changes. Your string is NOT shortened to be
	 * exactly the specified length.
	 * </p>
	 *
	 * @param s
	 * @param length
	 * @param c
	 * @return
	 */
	public static String pad(final String s, final int length, final char c)
	{
		final StringBuilder sb = new StringBuilder(s);

		while (sb.length() < length)
		{
			sb.append(c);
		}

		return sb.toString();
	}

	/**
	 * <p>
	 * Pad the LEFT HAND side of your string up to the specified length. Uses the character you specify.
	 * </p>
	 * <p>
	 * If your string is already at least the specified length, nothing changes. Your string is NOT shortened to be
	 * exactly the specified length.
	 * </p>
	 *
	 * @param s
	 * @param length
	 * @param c
	 * @return
	 */
	public static String padLeft(final String s, final int length, final char c)
	{
		final StringBuilder sb = new StringBuilder(s);

		while (sb.length() < length)
		{
			sb.insert(0, c);
		}

		return sb.toString();
	}

	public static String padLeftOrTruncate(String s, final int length, final char pad)
	{
		int diff = length - s.length();
		if(diff > 0)
		{
			final StringBuilder sb = new StringBuilder(s);
			while(diff-- > 0)
				sb.insert(0,pad);
			s = sb.toString();
		}
		else if(diff < 0)
		{
			s = s.substring(Math.abs(diff));
		}
		return s;
	}

	/**
	 * <p>
	 * Make the specified string safe for use as a filename.
	 * </p>
	 * <p>
	 * Allows letters, numbers, spaces, periods, parenthesis, hyphens, and other selected punctuation.
	 * </p>
	 *
	 * @param name
	 * @return
	 */
	public static String makeFileSafe(final String name)
	{
		if (null == name)
		{
			return "";
		}
		return name.replaceAll("[^a-zA-Z0-9 ()+!$=,.-]+", "_").replaceAll("[_]+", "_");
	}

	/**
	 * <p>
	 * Returns the (alleged) file extension for a particular filename.
	 * <p>
	 * <p>
	 * If there is period in the file name, this will return a blank string.
	 * <p>
	 * And, even if there is a period, this will just return everything from the last period to the end of the filename.
	 *
	 * @param fileName
	 * @return
	 */
	public static String getFileExtension(final String fileName)
	{
		final String extension;
		if (null != fileName && fileName.contains("."))
		{
			final int lastPeriod = fileName.lastIndexOf('.');
			extension = fileName.substring(lastPeriod + 1);
		}
		else
		{
			extension = "";
		}
		return extension;
	}

	/**
	 * Breaks a line apart based on delim and returns the array of tokens
	 *
	 * @param line
	 * @param delim
	 * @return
	 */
	public static String[] tokenizeStringToArray(String line, final String delim)
	{
		String[] rtn = null;

		if (line != null && delim != null && line.length() > 0 && delim.length() > 0)
		{
			// Refactoring caused the behavior to change for the "line" having the "delimiter" at the start of it
			// This is due to the way StringTokenizer works vs. split; this while loop fixes that issue
			// Differences in the returned array will still be evident where delimiters occur within the "line" one after
			// another
			// Example - line = ":a:b:c" ; delim = ":"
			// Array[0] = "" Array[1] = "a" Array[2] = "b" (without the while loop)
			// Array[0] = "a" Array[1] = "b" (with the while loop)
			while (line.startsWith(delim))
				line = line.substring(delim.length());
			rtn = StringUtil.doStringToArray(line, delim);
		}
		return rtn;
	}

	/**
	 * Breaks a line apart based on delim and returns the array of tokens
	 *
	 * @param line
	 * @param delim
	 * @return
	 */
	public static String[] tokenizeStringToArrayAllowEmpty(final String line, final String delim)
	{
		if (line != null && delim != null && line.length() > 0 && delim.length() > 0)
		{
			return StringUtil.doStringToArray(line, delim);
		}
		return EMPTY_STRING_ARRAY;
	}

	private static String[] doStringToArray(final String line, final String delim)
	{
		String regEx;
		if ("^[$.|\\".contains(delim))
			regEx = "\\" + delim;
		else
			regEx = delim;
		return line.split(regEx);
	}

	public static int[] tokenizeStringToIntArray(final String line, final String delim)
	{
		return tokenizeStringToIntArray(line, delim, -1);
	}

	public static int[] tokenizeStringToIntArray(final String line, final String delim, final int itemCount)
	{
		final StringTokenizer st = new StringTokenizer(line, delim);
		int n = itemCount;
		if (n < 0)
			n = st.countTokens();
		final int rtn[] = new int[n];
		int cnt = 0;
		while (st.hasMoreElements() && cnt < n)
		{
			try
			{
				rtn[cnt] = Integer.parseInt(st.nextToken());
			}
			catch (final NumberFormatException e)
			{
				rtn[cnt] = 0;
			}
			cnt++;
		}
		return rtn;
	}

	public static long[] tokenizeStringToLongArray(final String line, final String delim)
	{
		return tokenizeStringToLongArray(line, delim, -1);
	}

	public static long[] tokenizeStringToLongArray(final String line, final String delim, final int itemCount)
	{
		final StringTokenizer st = new StringTokenizer(line, delim);
		int n = itemCount;
		if (n < 0)
			n = st.countTokens();
		final long rtn[] = new long[n];
		int cnt = 0;
		while (st.hasMoreElements() && cnt < n)
		{
			try
			{
				rtn[cnt] = Long.parseLong(st.nextToken());
			}
			catch (final NumberFormatException e)
			{
				rtn[cnt] = 0;
			}
			cnt++;
		}
		return rtn;
	}

	public static double[] tokenizeStringToDoubleArray(final String line, final String delim)
	{
		return tokenizeStringToDoubleArray(line, delim, -1);
	}

	public static double[] tokenizeStringToDoubleArray(final String line, final String delim, final int itemCount)
	{
		final StringTokenizer st = new StringTokenizer(line, delim);
		int n = itemCount;
		if (n < 0)
			n = st.countTokens();
		final double rtn[] = new double[n];
		int cnt = 0;
		while (st.hasMoreElements() && cnt < n)
		{
			try
			{
				rtn[cnt] = Double.parseDouble(st.nextToken());
			}
			catch (final NumberFormatException e)
			{
				rtn[cnt] = 0;
			}
			cnt++;
		}
		return rtn;
	}

	public static boolean[] tokenizeStringToBoolArray(final String line, final String delim)
	{
		// false is returned for any token != 'true';
		// (where true can be mixed case)
		final StringTokenizer st = new StringTokenizer(line, delim);
		final boolean rtn[] = new boolean[st.countTokens()];
		int cnt = 0;
		while (st.hasMoreElements())
		{
			rtn[cnt] = st.nextToken().equalsIgnoreCase("true");
			cnt++;
		}

		return rtn;
	}

	/**
	 * This method may seem strange but it is a good way to parse integers without making string garbage
	 *
	 * @param line
	 * @param parsePos
	 * @param results
	 * @param resultPos
	 * @return
	 */
	public static int parseInt(final String line, int parsePos, final int[] results, final int resultPos)
	{
		int nn = 0;
		while (parsePos < line.length())
		{
			final char ch = line.charAt(parsePos);
			if (ch >= '0' && ch <= '9') // only interested in ascii decimal digits
				nn = nn * 10 + ch - '0';
			else
				break; // done
			parsePos++;
		}
		results[resultPos] = nn;
		return parsePos;
	}

	/**
	 * <p>
	 * Goes through a list and runs the toString() method on each element and returns the results.
	 * </p>
	 * <p>
	 *
	 * @param objList
	 * @return
	 */
	public static Collection<String> toStringAll(final Iterable<?> objList)
	{
		final Collection<String> toStringList = new ArrayList<String>();
		for (final Object n : objList)
			toStringList.add(n.toString());
		return toStringList;
	}

	/**
	 * Escape the \ and " characters with HTML escapes &#92 and &#34 respectively.
	 *
	 * @param toEscape
	 * @return
	 */
	public static String htmlEscape(final String toEscape)
	{
		return toEscape == null ? null : toEscape.replace("&", "&amp;").replace("\\", "&#092;").replace("\"", "&#034;")
				.replace("'", "&#039;").replace("<", "&lt;").replace(">", "&gt;").trim();
	}

	/**
	 * <p>
	 * Translates special characters into their equivalent UTF-8 escape sequences.
	 * <p>
	 * You want to use this before writing out the contents to a ResourceBundle file.
	 * <p>
	 * "Æon Flux" becomes "\u00c6on Flux", "¿Leche conseguida?" becomes "\u00bfLeche conseguida?" and so on.
	 */
	public static String utf8Escape(final String containsMultiByteCharacters)
	{
		final Set<Integer> charAllowedExceptions = new HashSet<Integer>(Arrays.asList(9, 13, 10));
		final StringBuilder bundleString = new StringBuilder();

		final char[] chars = containsMultiByteCharacters.toCharArray();
		for (final char next : chars)
		{
			// Anything outside this (rather narrow) range of ASCII-like characters is escaped.
			if ((next < 32 || 126 < next) && !charAllowedExceptions.contains(Integer.valueOf(next)))
			{
				bundleString.append("\\u");
				bundleString.append(StringUtil.padLeft(Integer.toHexString(next), 4, '0'));
			}
			else
			{
				bundleString.append(next);
			}
		}
		return bundleString.toString();
	}

	/**
	 * <p>
	 * Translates UTF-8 escape sequences into their character equivalents.
	 * <p>
	 * You want to use this before displaying the contents to a user.
	 * <p>
	 * "\u00c6on Flux" becomes "Æon Flux", "\u00bfLeche conseguida?" becomes "¿Leche conseguida?" and so on.
	 */
	public static String utf8Unescape(final String containsUTFEscapes)
	{
		final StringBuilder displayString = new StringBuilder(containsUTFEscapes);

		for (int posCursor = 0; posCursor < displayString.length(); posCursor++)
		{
			final int posStart = displayString.indexOf("\\u", posCursor);
			final int posEnd = posStart + 6;
			if (0 <= posStart && posEnd <= displayString.length())
			{
				final String escapeSequence = displayString.substring(posStart + 2, posEnd);
				try
				{
					final char multiByteCharacter = (char) Integer.parseInt(escapeSequence, 16);
					displayString.replace(posStart, posEnd, Character.toString(multiByteCharacter));
				}
				catch (final NumberFormatException ex)
				{
					// Do nothing. This hex string could not be parsed. This is clearly not an escaped character.
				}
			}
		}
		return displayString.toString();
	}

	/**
	 * want to trim the String
	 * @param value
	 * @param length
	 * @return String
	 */
	public static String truncate(String value, final int length)
	{
		if (value != null && value.length() > length)
			value = value.substring(0, length);
		return value;
	}

	/**
	 * Replaces the last occurrence of a {@link String} in the specified {@link String}.
	 * @param s The {@link String} on which to perform the replacement.
	 * @param toReplace The {@link String} to be replaced.
	 * @param replacement The replacement {@link String}.
	 * @return
	 */
	public static String replaceLast(final String s, final String toReplace, final String replacement)
	{
		final StringBuilder sb = new StringBuilder(s);
		final int pos = s.lastIndexOf(toReplace);
		if(pos > 0)
			sb.replace(pos,pos+toReplace.length(),replacement);
		return sb.toString();
	}

	/**
	 * Custom replaceAll method for StringBuilder.
	 *
	 * @param source Input StringBuilder object on which replaceAll is to be called
	 * @param inputPattern Represents input regex pattern
	 * @param replacement Represents the replacement string
	 */
	public static void replaceAll(final StringBuilder source, final String inputPattern, final String replacement)
	{
		final StringBuffer tempBuffer = new StringBuffer();
		final Pattern pattern = Pattern.compile(inputPattern);
		final Matcher matcher = pattern.matcher(source);

		while (matcher.find())
			matcher.appendReplacement(tempBuffer, replacement);
		matcher.appendTail(tempBuffer);
		source.delete(0, source.length());
		source.append(tempBuffer);
	}

	/**
	 * Lower case the {@link String} if not null.
	 * @param s The {@link String} to lower case.
	 * @return The lower cased {@link String} or null if <param>s</param> was null.
	 */
	public static String lower(final String s)
	{
		return s != null ? s.toLowerCase() : null;
	}

	public static int number(final String original){
		int number = 0;
		try {
		      number = Integer.parseInt(original);
		} catch (NumberFormatException e) {
		      //No problem this time but still it is good practice to care about exceptions.
		      //Never trust user input :)
		      //do something! anything to handle the exception.
		}

		return number;
	}

	/**
	 * Reverses given string
	 * @param original
	 * @return
	 */
	public static String reverse(final String original){
		String reverse = "";
		int length = original.length();
		for ( int i = length - 1 ; i >= 0 ; i-- )
			reverse = reverse + original.charAt(i);
		return reverse;
	}


	/**
	 * String. Returns a string filled with n spaces if it succeeds and the empty string ("") if an error occurs.
	 * If n is zero, Space returns null.
	 * @param noOfSpaces
	 * @return
	 */
	public static String space(final int noOfSpaces){
		if (noOfSpaces == 0){
			return null;
		}
		StringBuilder target = new StringBuilder();
		for (int i = 0; i < noOfSpaces; i++) {
			target.append(" ");
		}
		return target.toString();
	}

	/**
	 * Repeats a given character for specified number of times
	 * @param original
	 * @param count
	 * @return
	 */
	public static String repeat(final String original, final int count){
		if (count < 0){
			throw new NumberFormatException("Invalid Count Value");
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count ; i ++){
			sb.append(original);
		}
		return sb.toString();
	}
	


	public static String substring(final String src, final int startIndex, final int endIndex){
		if (startIndex < 0 || endIndex > src.length() || endIndex == 0){
			throw new IndexOutOfBoundsException();
		}
		System.out.println(src.substring(startIndex).substring(0, endIndex));
		return src.substring(startIndex).substring(0, endIndex);
	}
	
	public static String joinString(String[] str, final String separator){
		String target = "";
		for (int i = 0; i < str.length; i++) {
			target = target + str[i] + (i == str.length - 1 ? "" : separator);
		}
		return target;
	}


}
