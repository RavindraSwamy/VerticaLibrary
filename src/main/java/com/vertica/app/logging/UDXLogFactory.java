package com.vertica.app.logging;

import org.apache.log4j.Logger;

public class UDXLogFactory  {

	private static final Logger EDWLogger = Logger.getLogger("com.vertica.sdk.edw");
	private static final Logger CoreProcedureLogger = Logger.getLogger("com.vertica.sdk.edw.groupone.procedure");
	private static final Logger RECONPLogger = Logger.getLogger("com.vertica.sdk.reconp");
	private static final Logger CAMPLogger = Logger.getLogger("com.vertica.sdk.reconp");
	private static final Logger QDESKLogger = Logger.getLogger("com.vertica.sdk.qdesk");
	private static final Logger FIN_RAPGLogger = Logger.getLogger("com.vertica.sdk.fin_rapg");
	private static final Logger IBGLogger = Logger.getLogger("com.vertica.sdk.ibg");
	private static final Logger AUDITLogger = Logger.getLogger("com.vertica.sdk.audit");
	private static final Logger AJAYMLogger = Logger.getLogger("com.vertica.sdk.ajaym");
	private static final Logger AMOLKLogger = Logger.getLogger("com.vertica.sdk.amolk");
	private static final Logger DBALogger = Logger.getLogger("com.vertica.sdk.dba");
	private static final Logger DWH_APPLLogger = Logger.getLogger("com.vertica.sdk.dwh_app");
	private static final Logger JANHAVIPLogger = Logger.getLogger("com.vertica.sdk.janhavip");
	private static final Logger NILESHMLogger = Logger.getLogger("com.vertica.sdk.nileshm");
	private static final Logger NITINPLogger = Logger.getLogger("com.vertica.sdk.nitinp");
	private static final Logger P2CLogger = Logger.getLogger("com.vertica.sdk.p2c");
	private static final Logger PRIYANKSLogger = Logger.getLogger("com.vertica.sdk.priyanks");
	private static final Logger SAIKRISHNAMLogger = Logger.getLogger("com.vertica.sdk.saikrishnam");
	private static final Logger SHAILESHSLogger = Logger.getLogger("com.vertica.sdk.shaileshs");
	private static final Logger SUDHANSHUSLogger = Logger.getLogger("com.vertica.sdk.sudhanshus");
	private static final Logger SUNILMLogger = Logger.getLogger("com.vertica.sdk.sunilm");
	private static final Logger SWAPNAKLogger = Logger.getLogger("com.vertica.sdk.swapnak");
	private static final Logger SYBINTLogger = Logger.getLogger("com.vertica.sdk.sybint");
	private static final Logger VINAYAKDLogger = Logger.getLogger("com.vertica.sdk.vinayakd");
	private static final Logger SANDEEPKLogger = Logger.getLogger("com.vertica.sdk.sandeepk");
	private static final Logger ABDULDLogger = Logger.getLogger("com.vertica.sdk.abduld");

	private static final Logger patchLogger = Logger.getLogger("com.vertica.app.util");
	private static final Logger genericLogger = Logger.getLogger("com.vertica.sdk.common");


	public static Logger getABDULDLogger() {
		return ABDULDLogger;
	}

	public static Logger getAJAYMLogger() {
		return AJAYMLogger;
	}

	public static Logger getAMOLKLogger() {
		return AMOLKLogger;
	}

	public static Logger getDBALogger() {
		return DBALogger;
	}

	public static Logger getDWH_APPLLogger() {
		return DWH_APPLLogger;
	}

	public static Logger getJANHAVIPLogger() {
		return JANHAVIPLogger;
	}

	public static Logger getNILESHMLogger() {
		return NILESHMLogger;
	}

	public static Logger getNITINPLogger() {
		return NITINPLogger;
	}

	public static Logger getP2CLogger() {
		return P2CLogger;
	}

	public static Logger getPRIYANKSLogger() {
		return PRIYANKSLogger;
	}

	public static Logger getSAIKRISHNAMLogger() {
		return SAIKRISHNAMLogger;
	}

	public static Logger getSHAILESHSLogger() {
		return SHAILESHSLogger;
	}

	public static Logger getSUDHANSHUSLogger() {
		return SUDHANSHUSLogger;
	}

	public static Logger getSUNILMLogger() {
		return SUNILMLogger;
	}

	public static Logger getSWAPNAKLogger() {
		return SWAPNAKLogger;
	}

	public static Logger getSybintlogger() {
		return SYBINTLogger;
	}

	public static Logger getVINAYAKDLogger() {
		return VINAYAKDLogger;
	}

	public static Logger getSANDEEPKLogger() {
		return SANDEEPKLogger;
	}

	public static Logger getEDWLogger() {
		return EDWLogger;
	}

	public static Logger getRECONPLogger() {
		return RECONPLogger;
	}

	public static Logger getCAMPLogger() {
		return CAMPLogger;
	}

	public static Logger getGenericlogger() {
		return genericLogger;
	}

	public static Logger getQDESKLogger() {
		return QDESKLogger;
	}

	public static Logger getFIN_RAPGLogger() {
		return FIN_RAPGLogger;
	}

	public static Logger getIBGLogger() {
		return IBGLogger;
	}

	public static Logger getAUDITLogger() {
		return AUDITLogger;
	}

	public static Logger getPatchlogger() {
		return patchLogger;
	}

	public static Logger getCoreprocedurelogger() {
		return CoreProcedureLogger;
	}

}
