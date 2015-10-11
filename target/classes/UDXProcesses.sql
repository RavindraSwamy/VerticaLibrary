
----------------------------------------------------------------------------------------------

SELECT * FROM   (SELECT SYSDATE :: timestamptz  AS os_date,
               Substr(node_name, Instr(node_name, 'node')) AS os_node_name,
		total_memory_bytes/1024/1024 as 'Total Memory (GB)',
               ( ( total_memory_bytes - ( total_memory_free_bytes + total_buffer_memory_bytes + total_memory_cache_bytes ) ) / 1024/ 1024 ) :: Number(30, 2)
               || ' (' ||(
               ( total_memory_bytes - ( total_memory_free_bytes + total_buffer_memory_bytes + total_memory_cache_bytes ) ) / total_memory_bytes  * 100 ) :: Number(4, 2) ||'%)'                           "usage"
        FROM   host_resources hr
               join nodes n
                 ON ( hr.host_name = n.node_address )
        WHERE  ( node_name like '%001%' )) a
       join (SELECT SYSDATE :: timestamptz                 AS rp_date,
                    node_name                              AS rp_node_name,
                    SUM(running_query_count)               AS
                    running_query_count,
                    SUM(( memory_inuse_kb / 1024 )) :: INT AS mem_inuse
             FROM   resource_pool_status
             WHERE  node_name like '%001%'
             GROUP  BY node_name) b
         ON ( 1 = 1 );


Sample output
-----------------

os_date						os_node_name	Total Memory (GB)		usage						rp_date						rp_node_name			running_query_count		mem_inuse
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
2015-09-08 13:18:11.27582	node0001		128,807.49219		58315.45 (45.27%)		2015-09-08 13:18:11.27582	v_iciciuatdb_node0001					3					28726

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT
A.NODE_NAME, A.SESSION_ID, MAX_MEMORY_JAVA_KB, B.STATEMENT_START,LAST_STATEMENT
FROM UDX_FENCED_PROCESSES A LEFT OUTER
JOIN SESSIONS B ON A.SESSION_ID = B.SESSION_ID
WHERE STATUS = 'UP'
AND LANGUAGE = 'JAVA'

----------------------------------------------------------------------------------------------
