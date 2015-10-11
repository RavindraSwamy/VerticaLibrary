package com.vertica.app.sql.engine;

public class Constants {

	public static final int UDX_PRECISION_LENGTH = 50000;
	public static final int TIMESTAMP_MAX_PRECISION_LENGTH = 6;

	public static final String GET_MAX_TABLE_ID = "select max(a.table_id) "
			+ "from v_catalog.tables a ,v_catalog.users b,v_catalog.grants c ,v_catalog.all_tables d "
			+ "where upper(b.user_name) ilike upper(?) "
			+ "and a.owner_id = b.user_id and c.grantor_id=b.user_id and a.table_id=d.table_id "
			+ "and d.table_type in( 'TABLE','GLOBAL TEMPORARY')"
			+ "and (a.table_name not ilike 'DUMMY%' and a.table_name not ilike 'JAVA_%' "
			+ "and a.table_name not ilike 'sys%'and a.table_name not ilike 'rs_%' "
			+ "and a.table_name not ilike 'jdbc_%'and a.table_name not ilike 'EXCLUDE%' "
			+ "and a.table_name not ilike 'RowGen%'and a.table_name not ilike 'jdbc_%' "
			+ "and upper(trim(a.table_name)) ilike upper(trim(?))) ";

	public static final String GET_COUNT_OF_COLUMNS = "select COUNT(*) from v_catalog.columns a"
			+ " where "
			+ " a.table_id = ? "
			+ " and upper(trim(a.column_name)) = upper(trim(?))";

	public static final String GET_SRC_TGT_TABLE_FROM_SYB_ETL_UPD_WHERE_COLUMN = "select upper(trim(source_column)),upper(trim(target_column)) "
			+ " from DBA.SYB_ETL_UPD_WHERE_COLUMN "
			+ " where upper(trim(wh_table_name)) ilike upper(trim(?)) "
			+ " and upper(trim(src_system_code)) ilike upper(trim(?)) "
			+ " and etl_load_id = ?  order by upper(trim(source_column))";

	public static final String GET_SRC_TGT_COLUMN_FROM_SYB_ETL_UPD_WHERE_COLUMN_OTHER_SCHEMA = "select upper(trim(source_column)),upper(trim(target_column)) "
			+ " from DBA.SYB_ETL_UPD_WHERE_COLUMN_OTHER_SCHEMA "
			+ " where upper(trim(wh_table_name)) ilike upper(trim(?)) "
			+ " and upper(trim(src_system_code)) ilike upper(trim(?)) "
			+ " and etl_load_id = ?  order by upper(trim(source_column)) asc";

	public static final String GET_COLUMN_NAME_AND_POSITION = "Select upper(trim(a.column_name)),a.ordinal_position"
			+ " from v_catalog.columns a"
			+ " where a.table_id = ? "
			+ " and upper(trim(a.column_name)) not ilike 'PASS_FAIL_FLAG' "
			+ " order by a.ordinal_position asc ";

	public static final String GET_COLUMN_NAME_FROM_SYB_ETL_LTD_UPD_COLUMN = "select upper(trim(column_name)) "
			+ " from DBA.SYB_ETL_LTD_UPD_COLUMN "
			+ " where etl_load_id = ? "
			+ " and upper(trim(src_system_code)) = upper(trim(?)) "
			+ " and upper(trim(wh_table_name)) = upper(trim(wh_table_name)) ";
	
	public static final String GET_COLUMN_NAME_FROM_SYB_ETL_UPD_WHERE_COLUMN = "select upper(trim(source_column)),upper(trim(target_column)) "+
								" from DBA.SYB_ETL_UPD_WHERE_COLUMN "+
								" where upper(trim(wh_table_name)) ilike upper(trim(?)) "+
								" and upper(trim(src_system_code)) ilike upper(trim(?)) "+
								" and etl_load_id = ? order by upper(trim(source_column))";

	public static final String GET_COLUMN_NAME_FROM_SYB_ETL_LTD_UPD_COLUMN_OTHER_SCHEMA = "select upper(trim(column_name)) "
			+ " from DBA.SYB_ETL_LTD_UPD_COLUMN_OTHER_SCHEMA "
			+ " where etl_load_id = ? "
			+ " and upper(trim(src_system_code)) = upper(trim(?)) "
			+ " and upper(trim(wh_table_name)) = upper(trim(wh_table_name)) ";

	public static final String COUNT_FROM_SYB_ETL_UPD_WHERE_COLUMN = "select COUNT(*)  from DBA.SYB_ETL_UPD_WHERE_COLUMN "
			+ " where etl_load_id = ? "
			+ " and upper(trim(wh_table_name)) ilike upper(trim(?)) "
			+ " and upper(trim(target_column)) ilike upper(trim(?))";

	public static final String GET_FIXED_WIDTH_COLUMNS = "select column_name,column_width from DBA.SYB_FIXED_WIDTH_LOAD where etl_load_id = ? order by column_order asc ";
}
