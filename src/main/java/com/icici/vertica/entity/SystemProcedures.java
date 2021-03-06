package com.icici.vertica.entity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;


public class SystemProcedures {

	public static HashSet<String> system_stored_procedure = new HashSet<String>(Arrays.asList("SA_CHAR_TERMS", "SA_DEPENDENT_VIEWS", "SA_EXTERNAL_LIBRARY_UNLOAD", "SA_LIST_EXTERNAL_LIBRARY", "SA_NCHAR_TERMS", "SA_TEXT_INDEX_VOCAB", "SA_VERIFY_PASSWORD", "SA_GET_USER_STATUS", "SP_EXPIREALLPASSWORDS", "SP_IQADDLOGIN", "SP_IQBACKUPDETAILS", "SP_IQBACKUPSUMMARY", "SP_IQCARDINALITY_ANALYSIS", "SP_IQCHECKDB", "SP_IQCHECKOPTIONS", "SP_IQCLIENT_LOOKUP", "SP_IQCOLUMN", "SP_IQCOLUMNUSE", "SP_IQCONNECTION", "SP_IQCONSTRAINT", "SP_IQCONTEXT", "SP_IQCOPYLOGINPOLICY", "SP_IQCURSORINFO", "SP_IQDATATYPE", "SP_IQDBSIZE", "SP_IQDBSPACE", "SP_IQDBSPACEINFO", "SP_IQDBSPACEOBJECTINFO", "SP_IQDBSTATISTICS", "SP_IQDROPLOGIN", "SP_IQEMPTYFILE", "SP_IQESTJOIN", "SP_IQESTDBSPACES", "SP_IQESTSPACE", "SP_IQEVENT", "SP_IQFILE", "SP_IQHELP", "SP_IQINDEX AND SP_IQINDEX_ALTS", "SP_IQINDEXADVICE", "SP_IQINDEXFRAGMENTATION", "SP_IQINDEXINFO", "SP_IQINDEXMETADATA", "SP_IQINDEXSIZE", "SP_IQINDEXUSE", "SP_IQJOININDEX", "SP_IQJOININDEXSIZE", "SP_IQLMCONFIG", "SP_IQLOCKS", "SP_IQMODIFYADMIN", "SP_IQMODIFYLOGIN", "SP_IQMPXINCCONNPOOLINFO", "SP_IQMPXINCHEARTBEATINFO", "SP_IQMPXINFO", "SP_IQMPXVALIDATE", "SP_IQMPXVERSIONINFO", "SP_IQOBJECTINFO", "SP_IQPASSWORD", "SP_IQPKEYS", "SP_IQPROCEDURE", "SP_IQPROCPARM", "SP_IQREBUILDINDEX", "SP_IQRENAME", "SP_IQ_RESET_IDENTITY", "SP_IQRESTOREACTION", "SP_IQROWDENSITY", "SP_IQSHOWPSEXE", "SP_IQSPACEINFO", "SP_IQSPACEUSED", "SP_IQSTATISTICS", "SP_IQSTATUS", "SP_IQSYSMON", "SP_IQTABLE", "SP_IQTABLESIZE", "SP_IQTABLEUSE", "SP_IQTRANSACTION", "SP_IQUNUSEDCOLUMN", "SP_IQUNUSEDINDEX", "SP_IQUNUSEDTABLE", "SP_IQVERSIONUSE", "SP_IQVIEW", "SP_IQWHO", "SP_IQWORKMON"));

	// Nested procedure calls could be in ALL procedures,
		//not just in phase 1 procedures
		public static String getEntityName(String mine) {
			for (Iterator<String> iterator = SystemProcedures.system_stored_procedure.iterator(); iterator
					.hasNext();) {
				String type = iterator.next();
				if (mine.contains(type + "("))
					return type;
			}
			return "NA";
		}


}
