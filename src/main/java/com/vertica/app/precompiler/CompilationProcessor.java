package com.vertica.app.precompiler;

import java.io.FileNotFoundException;
import java.io.IOException;

public class CompilationProcessor {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		QueryPrecompiler.setSearchPath("edw");
		String dir_path = "E:/Projects/Vertica/VerticaUDXDev/src/main/java/com/vertica/sdk/local";
		String file_name = "";

		if (dir_path.contains("\\")){
			dir_path = dir_path.replace("\\", "/");
		}

		// Precompile a directory
		 QueryPrecompiler.process(true, dir_path, null);

		// Precompile a file
		// QueryPrecompiler.process(false, dir_path, file_name);
	}
}
