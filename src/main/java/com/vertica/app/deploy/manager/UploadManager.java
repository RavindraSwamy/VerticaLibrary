package com.vertica.app.deploy.manager;

import static com.vertica.app.loader.ApplicationProperties.getInstance;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;

public class UploadManager {


	public static void uploadUDXParser(final String userName) {
		// Initialize a SSHExec instance without referring any object.
		SSHExec ssh = null;
		// Wrap the whole execution jobs into try-catch block
		try {
			// Initialize a ConnBean object, parameter list is ip, username,
			// password
			final ConnBean cb = new ConnBean(getInstance()
					.getValue("server_ip"), getInstance().getValue(
							"server_user"), getInstance().getValue("server_password"));
			// Put the ConnBean instance as parameter for SSHExec static method
			// getInstance(ConnBean) to retrieve a real SSHExec instance
			ssh = SSHExec.getInstance(cb);
			// Create a ExecShellScript, the reference class must be CustomTask
			//final CustomTask ct2 = new ExecShellScript("/opt/vertica/sdk/",
			//		"./changepermission.sh", LIBRARY_PATH);
			// Connect to server
			ssh.connect();
			ssh.uploadSingleDataToServer("C:/Users/BAN57955/Desktop/Documents/deploymentMultiCharDelimiterParser.jar", "/data/" + userName + "/udx" +  "/");
			// Execute task and get the returned Result object
			//final Result res = ssh.exec(ct2);
			// Check result and print out messages.
			//if (res.isSuccess) {
			//	System.out.println("Return code: " + res.rc);
			//	System.out.println("sysout: " + res.sysout);
			//} else {
			//	System.out.println("Return code: " + res.rc);
			//	System.out.println("error message: " + res.error_msg);
			//}

		} catch (final TaskExecFailException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (final Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			ssh.disconnect();
		}
	}

	public static void uploadUDXLibrary(final String userName, final String LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX) {
		// Initialize a SSHExec instance without referring any object.
		SSHExec ssh = null;
		// Wrap the whole execution jobs into try-catch block
		try {
			// Initialize a ConnBean object, parameter list is ip, username,
			// password
			final ConnBean cb = new ConnBean(getInstance()
					.getValue("server_ip"), getInstance().getValue(
							"server_user"), getInstance().getValue("server_password"));
			// Put the ConnBean instance as parameter for SSHExec static method
			// getInstance(ConnBean) to retrieve a real SSHExec instance
			ssh = SSHExec.getInstance(cb);
			// Create a ExecShellScript, the reference class must be CustomTask
			//final CustomTask ct2 = new ExecShellScript("/data/"+ userName + "/udx/",
			//		"./changepermission.sh", LIBRARY_PATH);
			// Connect to server
			ssh.connect();
			ssh.uploadSingleDataToServer(
					LOCAL_LIBRARY_PATH_TO_BE_MOVED_TO_UNIX, "/vertica_load/udx/deploy/udx/");
			// Execute task and get the returned Result object
			//Result exec = ssh.exec(ct2);
			//final Result res = exec;
			// Check result and print out messages.
			/*if (res.isSuccess) {
				System.out.println("Return code: " + res.rc);
				System.out.println("sysout: " + res.sysout);
			} else {
				System.out.println("Return code: " + res.rc);
				System.out.println("error message: " + res.error_msg);
			}*/

		} catch (final TaskExecFailException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (final Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			ssh.disconnect();
		}
	}

	public static void uploadCommonUtility(final String userName, final String LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_DEV, final String LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_PROD) {
		// Initialize a SSHExec instance without referring any object.
		SSHExec ssh = null;
		// Wrap the whole execution jobs into try-catch block
		try {
			// Initialize a ConnBean object, parameter list is ip, username,
			// password
			final ConnBean cb = new ConnBean(getInstance()
					.getValue("server_ip"), getInstance().getValue(
							"server_user"), getInstance().getValue("server_password"));
			// Put the ConnBean instance as parameter for SSHExec static method
			// getInstance(ConnBean) to retrieve a real SSHExec instance
			ssh = SSHExec.getInstance(cb);
			// Create a ExecShellScript, the reference class must be CustomTask
			// Connect to server
			ssh.connect();
			ssh.uploadSingleDataToServer(userName.equals("vertadm") ? LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_PROD :
				LOCAL_COMMON_UTILITY_TO_BE_MOVED_TO_UNIX_DEV, "/vertica_load/udx/deploy/udx/lib/");

		} catch (final TaskExecFailException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (final Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			ssh.disconnect();
		}
	}
	
}
