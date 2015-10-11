package com.vertica.app.sql.engine;

public enum ErrorEnum {
	
	CONSTRAINTS_VIOLATED(44001, "Integrity constraint violated"),
	ETL_ID_VIOLATED(44002, "ETL ID Processing violated");
	
	private int code;
	private String message;

	ErrorEnum(final int code, final String message){
		this.setCode(code);
		this.setMessage(message);
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	

}
