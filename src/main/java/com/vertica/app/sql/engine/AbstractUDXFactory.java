package com.vertica.app.sql.engine;

import com.sun.media.sound.InvalidDataException;
import com.vertica.app.loader.FactoryProperties;
import com.vertica.sdk.ScalarFunctionFactory;

public abstract class AbstractUDXFactory extends ScalarFunctionFactory {

	/**
	 * Getting schema specific database connections
	 * @return
	 * @throws InvalidDataException 
	 */
	public String getSchemaName(){
		String simpleClassName = this.getClass().getSimpleName();
		String procedure_name = FactoryProperties.getInstance().getValue(simpleClassName);
		String schema_name = procedure_name.substring(0, procedure_name.indexOf(".")).toLowerCase();
		if (schema_name == null){
			return "PUBLIC";
		}
		return schema_name;
	}
	
}
