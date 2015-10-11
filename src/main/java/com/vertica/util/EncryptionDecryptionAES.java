package com.vertica.util;


import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;

import com.vertica.app.loader.ApplicationProperties;

public class EncryptionDecryptionAES {
	static Cipher cipher;
	static SecretKey secretKey = null;

	//create table serialized_auth_data(object_name varchar(20), serialized_obj LONG VARCHAR);

	static{
		generateSecretKey();
	}


	public static void generateSecretKey() {
		try {
			// read the object from file
			// save the object to file
			cipher = Cipher.getInstance("AES");

			FileInputStream fis = null;
			ObjectInputStream in = null;
			try {
				if (ApplicationProperties.getInstance().getValue("current_env").equalsIgnoreCase("local")){
					fis = new FileInputStream("src/main/resources/key.txt");	
				}else{
					fis = new FileInputStream("/vertica_load/udx/auth/key.txt");
				}
				in = new ObjectInputStream(fis);
				secretKey = (SecretKey) in.readObject();
				in.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			if (secretKey == null){
				cipher = Cipher.getInstance("AES");
				secretKey  = getSecretKey();
				System.out.println("secret key for encryprion: " + secretKey);

				/*Connection cn = ConnectionManager.getConnection();
				PreparedStatement stmt = cn.prepareStatement(insert_into_ser);
				stmt.setString(1, "ENCR_DCR_SECRET_KEY");
				// Inprogress
				// stmt.setObject(2, new ObjectOutputStream(.writeObject(secretKey).toString());
				cn.commit();*/
				// save the object to file
				FileOutputStream fos = null;
				ObjectOutputStream out = null;
				try {
					fos = new FileOutputStream("src/main/resources/key.txt");
					out = new ObjectOutputStream(fos);
					out.writeObject(secretKey);
					out.close();
					fos.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}catch (NoSuchPaddingException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		try {
			/*getEncodedDecodedStrings("icici#4_db");
			getEncodedDecodedStrings("edw12345");
			getEncodedDecodedStrings("reconp12345");
			getEncodedDecodedStrings("ibg123456");
			getEncodedDecodedStrings("audit123");
			getEncodedDecodedStrings("fin_rapg123");*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String decryptText(final String text){
		try {
			generateSecretKey();
			//return decrypt("H7tidj76yIAMnJ3ZY2kxgg==");
			return decrypt(text);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void getEncodedDecodedStrings(String plainText) throws Exception {
		System.out.println("Plain Text Before Encryption: " + plainText);
		String encryptedText = encrypt(plainText);
		System.out.println("Encrypted Text After Encryption: " + encryptedText);

		String decryptedText = decrypt(encryptedText);
		System.out.println("Decrypted Text After Decryption: " + decryptedText);
	}

	public static SecretKey getSecretKey() throws NoSuchAlgorithmException {
		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(128);
		SecretKey secretKey = keyGenerator.generateKey();
		return secretKey;
	}

	/**
	 * Encrypt the given plainText. The plainText is encrypted and the is encoded by Base64.
	 * To decypt this encyptedText, it should be first decoded by Base64 and then decryption to be done.
	 * @param plainText : The text to be encrypted.
	 * @return encryptedText : The encrypted text.
	 * @throws Exception
	 */
	public static String encrypt(String plainText) throws Exception {
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		byte[] plainTextByte = plainText.getBytes();
		byte[] encryptedByte = cipher.doFinal(plainTextByte);
		String encryptedText = Base64.encodeBase64String(encryptedByte);
		return encryptedText;
	}

	/**
	 * Decrypt the given encryptedText.
	 * To decypt this encyptedText, it should be first decoded by Base64 and then decryption to be done.
	 * @param encryptedText : Text to be decrypted.
	 * @return decryptedText : Decrypted text of given encryptedText.
	 * @throws Exception
	 */
	public static String decrypt(String encryptedText) throws Exception {
		cipher.init(Cipher.DECRYPT_MODE, secretKey);
		byte[] encryptedTextByte = Base64.decodeBase64(encryptedText);
		byte[] decryptedByte = cipher.doFinal(encryptedTextByte);
		String decryptedText = new String(decryptedByte);
		return decryptedText;
	}

	/**
	 * Deserializes the given serialized, BASE64 encoded <code>key</code>. Note that the SecretKey implmentation class
	 * must be available for this method to succeed
	 *
	 * @return the deserialized <code>SecretKey</code> implmentation or null
	 * @see genKey()
	 */
	public static synchronized SecretKey deserializeKey(String key) {
		SecretKey rslt = null;
		try {
			byte[] serializedKey = Base64.decodeBase64(key);
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serializedKey));
			rslt = (SecretKey) ois.readObject();
		}
		catch(Exception e) {
			// log.error(e);
			e.printStackTrace();
		}
		return rslt;
	}
}