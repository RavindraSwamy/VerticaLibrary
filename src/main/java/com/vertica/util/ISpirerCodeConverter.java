/**
 *
 */
package com.vertica.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author schaurasia
 *
 */
public class ISpirerCodeConverter {
	static int rowCount =1;
	static SimpleDateFormat dt = new SimpleDateFormat("yyyyMMdd");
	public static void main(String[] args) throws IOException{
		Map<String,Boolean> filemap = new HashMap<>();

		// List of all files to be converted
		String fileNames [] = {"T0111FinacleIbgGerFactory.java"};

		// Location of ISPirer scorce files
		String path = "D:/Anirudh/";
		for(String fileName  : fileNames){
			rowCount =1;
		Charset charset=Charset.forName("UTF-8");
		String fileContent = null;
		try {
			fileContent = readFile(path+fileName, charset);
		} catch (IOException e) {
			filemap.put(fileName, false);
		}
		if(fileContent == null)
			continue;

		String[] tokenizer =  fileContent.split("\\n");
		int count = 0;
		String delim = "";
		StringBuilder mainBuilder = new StringBuilder();
			for (int i = 0 ; i < tokenizer.length ; i++) {
				String line = tokenizer[i];
				if(line.contains("copy") || line.contains("NO ESCAPE")){
					if(line.contains("copy")){
						delim = line.substring(line.indexOf("'"),line.indexOf("'",line.indexOf("'")+1)+1);
						line = line.replace(delim, "");
						delim = delim.replace("\"", "~");
						line = line.replace("file", "from");
						line = line.replace("'\\\\x0A'", "");
						if(line.contains("executor.update"))
							line = line.replace("executor.update(", "long rowCount"+rowCount+" = executor.updateCopyToTable(");
						StringBuilder sb = new StringBuilder(line);
						//System.out.println(sb);
						if(!line.contains("NO ESCAPE"))
							mainBuilder.append(sb);
						//System.out.println("delim "+delim);
					}
					if(line.contains("NO ESCAPE")){
						StringBuilder sb = new StringBuilder(line);
						if(delim.length() > 3){
							int index = line.indexOf("NO ESCAPE");
							if(!delim.equals("'\\\\x0A'"))
								sb.insert(index-1, "with parser MultiCharDelimiterParser(separator="+delim+") ");

							//System.out.println(sb);
						}else{
							int index = line.indexOf("NO ESCAPE");
							sb.insert(index-1, "Delimiter "+delim+" ");
							//System.out.println(sb);
						}
						mainBuilder.append(sb);
					}

				}else if(line.contains("RowCount")){
					if(line.contains("',RowCount\"")){
						line = line.replaceAll("',RowCount\"", "',?\", rowCount"+rowCount++);

					}else{
						line = line.replaceAll("RowCount\\s\"", "\"+ rowCount"+rowCount++);
					}
					mainBuilder.append(line);
				}else if(line.contains("(Exception")){

					line = line.replaceAll("Exception",  "SQLException");
					mainBuilder.append(line);
				}else if(line.contains("// TODO: handle exception")){

					line = line.replaceAll("// TODO: handle exception",  "UDXLogFactory.getEDWLogger().error(ExceptionUtil.getStringFromException(e));");
					mainBuilder.append(line);
				}else if(line.contains("SqlState")){
					line = "/*"+ line;
					mainBuilder.append(line);

					while(!tokenizer[++i].trim().equals("}")){
						mainBuilder.append(tokenizer[i]);
					}
					if(tokenizer[i+1].contains("else")){
						mainBuilder.append(tokenizer[i]);
						while(!tokenizer[++i].trim().equals("}")){
							mainBuilder.append(tokenizer[i]);
						}
					}
					String str = tokenizer[i] + "*/" + "\n";
					mainBuilder.append(str);
				}else{
					mainBuilder.append(line);
				}


				count++;
			}
			//System.out.println(mainBuilder.toString().replace("ScalarFunctionFactory","AbstractUDXFactory").replace("sybase_load", "vertica_load").replace("ConnectionManager.getConnection()", "ConnectionManager.getConnection(getSchemaName())").replace("package", "//package"));

			writeFile("D://ICICI_VERTICA//UDx_"+ dt.format(new Date()),"\\"+fileName, new StringBuilder(mainBuilder.toString().replace("ScalarFunctionFactory","AbstractUDXFactory").replace("sybase_load", "vertica_load").replace("ConnectionManager.getConnection()", "ConnectionManager.getConnection(getSchemaName())")/*.replace("package", "//package")*/));
		}

		System.out.println("Files Not found "+filemap);
	}


	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	static void writeFile(String path, String fileName, StringBuilder sb) throws IOException {
		File file = new File(path);
		file.mkdirs();
		//file.createNewFile();
		Writer out = new FileWriter(file+fileName);
		BufferedWriter writer = new BufferedWriter(out);
		writer.append(sb);
		writer.flush();
		writer.close();
	}
}

