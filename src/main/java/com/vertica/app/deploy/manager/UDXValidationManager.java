package com.vertica.app.deploy.manager;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.vertica.app.deploy.Messages;
import com.vertica.app.exception.InvalidUDXLibraryException;
import com.vertica.util.StringUtil;

public class UDXValidationManager {

	public static HashSet<String> finalClassNames;

	public static void main(String[] args) {
		System.out.println(convertUDXNameToSQLName("com.vertica.sdk.T09522LoginMis11xMigLoadFactory"));
	}

	public static void validate(String factoryClassName,
			final String jarToBeDeployed) throws InvalidUDXLibraryException {

		System.out.println(Messages.getString("UDXValidator.0"));

		List<String> classNames = new ArrayList<String>();
		final HashSet<String> factory_names = new HashSet<String>();
		final HashSet<String> udx_names = new HashSet<String>();

		classNames = extractClassFilesFromJar(jarToBeDeployed, classNames);
		if (!classNames.contains("com.vertica.sdk.BuildInfo")) {
			throw new InvalidUDXLibraryException(
					Messages.getString("UDXValidator.5"));
		}

		if (classNames.size() < 3) {
			throw new InvalidUDXLibraryException(
					Messages.getString("UDXValidator.6"));
		}

		/*if (!classNames.contains(factoryClassName))
			throw new InvalidUDXLibraryException(
					Messages.getString("UDXValidator.6"));*/

		System.out.println(Messages.getString("UDXValidator.7"));

		getFactoryClassNames(classNames, factory_names);

		finalClassNames = new HashSet<>(factory_names);

		System.out.println(Messages.getString("UDXValidator.9"));

		getUDXNameFromFactoryName(factory_names, udx_names);
	}

	public static String printHashValues(
			HashSet<String> dependent_function_name) {
		StringBuilder sb = new StringBuilder();
		for (Iterator<String> iterator = dependent_function_name.iterator(); iterator
				.hasNext();) {
			String string = iterator.next();
			sb.append(string).append(iterator.hasNext() ? "," : "");
		}
		return sb.toString();
	}
	
	

	public static void getUDXNameFromFactoryName(
			final HashSet<String> factory_names, final HashSet<String> udx_names) {
		for (final Iterator<String> iterator = factory_names.iterator(); iterator
				.hasNext();) {
			final String factory_name = iterator.next();
			System.out.println(factory_name);
			final String convertClassNameToUDXName = convertUDXNameToSQLName(factory_name);
			udx_names.add(convertClassNameToUDXName);
			System.out.println("Recommended UDX Name: "
					+ convertClassNameToUDXName);
		}
	}

	public static void getFactoryClassNames(final List<String> classNames,
			final HashSet<String> factory_names) {
		for (Iterator<String> iterator = classNames.iterator(); iterator
				.hasNext();) {
			String factory_name = iterator.next();
			if (factory_name.endsWith("Factory")) {
				factory_names.add(factory_name);
			}
		}
	}

	@SuppressWarnings("resource")
	public static List<String> extractClassFilesFromJar(
			final String jarToBeDeployed, final List<String> classNames)
			throws InvalidUDXLibraryException {
		ZipInputStream zip = null;
		try {
			zip = new ZipInputStream(new FileInputStream(jarToBeDeployed));
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip
					.getNextEntry()) {
				if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
					// This ZipEntry represents a class
					final String className = entry.getName().replace('/', '.'); // including
					// ".class"
					classNames.add(className.substring(0, className.length()
							- ".class".length()));
				}
			}
		} catch (final IOException e) {
			throw new InvalidUDXLibraryException(
					Messages.getString("UDXValidator.3"));
		}
		return classNames;
	}

	public static String convertUDXNameToSQLName(String source) {

		if (source.equalsIgnoreCase("com.vertica.sdk.common.XPReadFileFactory")){
			return "XP_READ_FILE";
		}
		
		if (source.equalsIgnoreCase("com.vertica.sdk.common.XPWriteFileFactory")){
			return "XP_WRITE_FILE";
		}
			
		if (source.equalsIgnoreCase("com.vertica.sdk.common.DateAddFactory")){
			return "DATEADD";
		}
		
		if (source.equalsIgnoreCase("com.vertica.sdk.edw.grouptwo.procedure.SpinLoadFinacleB03301Current1Factory")){
			return "SPIN_LOAD_FINACLE_B03301_CURRENT_1";
		}
		if (source.equalsIgnoreCase("com.vertica.sdk.edw.groupone.procedure.SpinLoadUpsToDelInsert2Factory")){
			return "SPIN_LOAD_UPS_TO_DEL_INSERT_2";
		}
		
		if (source.equalsIgnoreCase("com.vertica.sdk.edw.groupone.procedure.SpinLoadFinacleDrwngPowerDpdFactory")){
			return "SPIN_LOAD_FINACLE_DRWNG_POWER_DPD123";
		}
		
		if (source.equalsIgnoreCase("com.vertica.sdk.edw.groupthree.procedure.SpinLoadHfcIcore10xToDwh1032Factory")){
			return "SPIN_LOAD_HFC_ICORE_10X_TO_DWH_1032";
		}

		if (source.equalsIgnoreCase("com.vertica.sdk.edw.groupthree.procedure.SpinLoadHfcIcore10xToDwh1033Factory")){
			return "SPIN_LOAD_HFC_ICORE_10X_TO_DWH_1033";
		}
		
		if (source.equalsIgnoreCase("com.vertica.sdk.edw.groupthree.procedure.SpinLoadHfcIcore10xToDwhFactory")){
			return "SPIN_LOAD_HFC_ICORE_10X_TO_DWH";
		}
		
		
		if (source.contains("T09522LoginMis11xMigLoadFactory")){
			return "T09522_LOGIN_MIS_11X_MIG_LOAD";
		}
		
		 source = source.substring(source.lastIndexOf(".") + 1, source.length());
		String res = "";

		for (int i = 0; i < source.length(); i++) {
			Character ch = source.charAt(i);
			if (Character.isUpperCase(ch)){
				res += " " + Character.toLowerCase(ch);
			}
			else{
				res += ch;
			}
		}
		String[] array = StringUtil.toArray(res, " ");

		String final_str = "";
		for (int i = 0; i < array.length; i++) {
			final_str = final_str + array[i]
					+ (i != 0 && i != array.length - 1 ? "_" : ""); //$NON-NLS-2$
		}

		return final_str.substring(0, final_str.length() - "_factory".length())
				.toUpperCase();

	}
	
	public static void convertSQLNameToUDXName(String sqlName){
		String finalString = sqlName;
		char[] charArray = finalString.toLowerCase().toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			if (i == 0){
				charArray[i] = Character.toUpperCase(charArray[i]);
			}
			if (charArray[i] == '_'){
				i++;
				charArray[i] = Character.toUpperCase(charArray[i]);
			}
		}
		finalString = String.valueOf(charArray).replace("_", "").concat("Factory.java");
		System.out.println(sqlName + "+" + finalString);

	}
}
