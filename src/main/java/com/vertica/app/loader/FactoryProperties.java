package com.vertica.app.loader;

import java.io.IOException;
import java.util.Properties;
	
/**
 * Thread safe singleton class for loading the property file
 * @author achaudhary
 *
 */

public class FactoryProperties {

	private static FactoryProperties INSTANCE = null;
	private Properties properties;

	private FactoryProperties(){
		properties = new Properties();
		try {
			 properties.load(getClass().getResourceAsStream("/factory.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static FactoryProperties getInstance(){
		if (INSTANCE == null){
			synchronized (FactoryProperties.class) {
				if (INSTANCE == null){
					INSTANCE = new FactoryProperties();
				}
			}
		}
		return INSTANCE;
	}

	public String getValue(final String key){
		return properties.getProperty(key);
	}

	public static void main(String[] args) {
		FactoryProperties.getInstance();
	}

	public void encryptDecrypt(){
		try {
			//System.out.println(EncryptionDecryptionAES.decrypt("4teftgUWKD/iKqm2zoEhTA=="));
		} catch (Exception e) {
			e.printStackTrace();
		}
		//properties.setProperty("4teftgUWKD/iKqm2zoEhTA==", "eEO0X+C9StcTARUs14ICpQ==");
		//properties.setProperty("/h9vBZcxa1AcrPa9Ozxpxw==", "P0bYWW56/cwouetDS6spdg==");
		//properties.setProperty("4j+n2YDwZONbIgqhelD0Tw==", "4teftgUWKD/iKqm2zoEhTA==");

		/*Plain Text Before Encryption: dbadmin
		Encrypted Text After Encryption: eEO0X+C9StcTARUs14ICpQ==
		Decrypted Text After Decryption: dbadmin
		Plain Text Before Encryption: icici
		Encrypted Text After Encryption: P0bYWW56/cwouetDS6spdg==
		Decrypted Text After Decryption: icici
		Plain Text Before Encryption: vertica_icici
		Encrypted Text After Encryption: qy6RX1ORAhJGDQbhAjiaTQ==
		Decrypted Text After Decryption: vertica_icici
		Plain Text Before Encryption: user_name
		Encrypted Text After Encryption: 4teftgUWKD/iKqm2zoEhTA==
		Decrypted Text After Decryption: user_name
		Plain Text Before Encryption: password
		Encrypted Text After Encryption: /h9vBZcxa1AcrPa9Ozxpxw==
		Decrypted Text After Decryption: password
		Plain Text Before Encryption: database
		Encrypted Text After Encryption: 4j+n2YDwZONbIgqhelD0Tw==
		Decrypted Text After Decryption: database*/
	}

}
