package com.vertica.app.loader;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.vertica.util.EncryptionDecryptionAES;

/**
 * Thread safe singleton class for loading the property file
 * @author achaudhary
 *
 */

public class ApplicationProperties {

	private static ApplicationProperties INSTANCE = null;
	private Properties properties;

	private ApplicationProperties(){
		properties = new Properties();
		try {
			
			/*FileInputStream in = new FileInputStream("/vertica_load/udx/auth/db.properties");
			properties.load(in);
			in.close();*/
			properties.load(getClass().getResourceAsStream("/dbdev.properties"));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static ApplicationProperties getInstance(){
		if (INSTANCE == null){
			synchronized (ApplicationProperties.class) {
				if (INSTANCE == null){
					INSTANCE = new ApplicationProperties();
				}
			}
		}
		return INSTANCE;
	}

	public String getValue(final String key){
		try {
			String value = properties.getProperty(key);
			return value;
			/*if (key.equalsIgnoreCase("current_env")){
				return value;
			}
			String decrypt =  EncryptionDecryptionAES.decrypt(value.trim());
			
			return decrypt;*/
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		// IF ONLY PASSWORD IS ENCRYPTED.
		/*if (key.contains("password")){
			try {
				String value = properties.getProperty(key);
				String decrypt =  EncryptionDecryptionAES.decrypt(value.trim());
				return decrypt;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}else
			return properties.getProperty(key);*/
	}

	public boolean setValue(final String key, final String value) {
		try {
			String encryptedValue = EncryptionDecryptionAES.encrypt(value);
			properties.setProperty(key, encryptedValue);
			System.out.println("Property set : Key - " + key + " \t Value - " + encryptedValue);
			FileOutputStream out = new FileOutputStream("/vertica_load/dbdev.properties");
			properties.store(out, "Something");
			out.close();
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) throws Exception {
		// ApplicationProperties.getInstance();
		System.out.println(EncryptionDecryptionAES.decrypt("93dJp4EoLBblMj9Z/hcZYw\\=\\="));
		System.out.println(ApplicationProperties.getInstance().enableAnalyzeConstraint());
	}

	public void encryptDecrypt(){
		try {
			//System.out.println(EncryptionDecryptionAES.decrypt("4teftgUWKD/iKqm2zoEhTA=="));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean enableAnalyzeConstraint(){
		if (getValue("enableAnalyzeConstraint") == null || getValue("enableAnalyzeConstraint").isEmpty())
			return true;
		else
			return Boolean.parseBoolean(getValue("enableAnalyzeConstraint"));
	}

}
