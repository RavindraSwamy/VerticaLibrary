package com.vertica.util;

import org.apache.log4j.Logger;

public class ShellUtil {


	public static void executeCommand(String command, Logger _log) {
		Process p;
		try {
			if (_log != null)
				_log.info(command.toString());
			p = Runtime.getRuntime().exec(new String[]{"bash","-c",command});
		} catch (Exception e) {
			if (_log != null)
				_log.error("Error occured while running bash command ...\n" + ExceptionUtil.getBriefException(e));
			// Do not error out UDX Execution if execution of bash command fails
			// throw new UdfException(0, e.getMessage());
		}
	}
}
