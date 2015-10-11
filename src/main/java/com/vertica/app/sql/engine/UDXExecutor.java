package com.vertica.app.sql.engine;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.vertica.sdk.BlockReader;
import com.vertica.sdk.BlockWriter;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ScalarFunction;
import com.vertica.sdk.ScalarFunctionFactory;
import com.vertica.sdk.ServerInterface;

/**
 * Executes a UDX in stand-alone fashion by creating instances of
 * {@link ScalarFunctionFactory} and {@link ScalarFunction} classes using
 * reflection API which in turn calls <code> processBlock </code> method of the
 * later class.
 *
 * @Warning- It assumes {@link ServerInterface}, {@link BlockReader} &
 *           {@link BlockWriter} to be null. You will need to comment out code
 *           snippet within UDX which are calling methods of such classes
 *
 * @author Abhishek Chaudhary
 *
 */
public final class UDXExecutor {

	@SuppressWarnings("unchecked")
	public static void execute(
			final Class<? extends AbstractUDXFactory> _class) {
		try {
			final Class<ScalarFunctionFactory> clazz = (Class<ScalarFunctionFactory>) Class
					.forName(_class.getName());
			final Constructor<ScalarFunctionFactory> c = clazz
					.getConstructor();
			final ScalarFunction sf = c.newInstance()
					.createScalarFunction(null);
			sf.processBlock(null, null, null);
		} catch (DestroyInvocation | InstantiationException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | ClassNotFoundException
				| NoSuchMethodException | SecurityException e) {
			System.err.println("Error invoking User Defined Extension ...");
		}
	}
}
