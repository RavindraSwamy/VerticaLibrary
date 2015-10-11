package com.vertica.test;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.vertica.app.loader.ApplicationProperties;

public class ApplicationPropertiesTest{


	@Test
	public void testLoader(){

		ApplicationProperties instance = ApplicationProperties.getInstance();
		assertNotNull(instance.getValue("user_name"));
		assertNotNull(instance.getValue("password"));
		assertNotNull(instance.getValue("database"));
		assertNotNull(instance.getValue("url"));
	}

}
