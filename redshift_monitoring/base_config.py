base_config = {
	'redshift_connection' : {},
	'database' : {'opentsdb_url': 'http://<opentsdb_url>:4242/api/put/',
				  'statsd_ip': <statsd_ip>,
				  'statsd_port': <statsd_port>,
				  'mysql_endpoint': 'mysql+mysqldb://',
				  'mysql_user': <mysql_username>,
				  'mysql_port': <mysql_port>,
				  'mysql_dbname': <mysql_db_name>,
				  'mysql_pass' : <mysql_password>},
	'query' : {'query_dictionary': {
								'query1' : """SELECT * FROM stv_recents WHERE status = 'Running';""",
								'query2' : """SELECT * FROM stv_wlm_query_state;""",
								'query3' : """SELECT wlm.query AS query_id,
											         wlm.state,
											         wlm.service_class AS queue,
											         CONVERT_TIMEZONE('Asia/Calcutta',wlm.wlm_start_time) AS starttime,
											         wlm.slot_count,
											         pg_user.usename AS username,
											         ex.inner_bcast_count,
											         bcast.bcast_rows,
											         CAST((wlm.exec_time) AS float) / 1000000 AS exec_time,
											         CAST((wlm.queue_time) AS float) / 1000000 AS queue_time,
											         CAST(SUM(qs.workmem) AS float) / 1000000000 AS workmem,
											         SUM(CASE WHEN qs.is_diskbased = 't' THEN 1 ELSE 0 END) AS num_diskhits
											  FROM stv_wlm_query_state wlm
											    LEFT JOIN svv_query_state qs ON qs.query = wlm.query
											    LEFT JOIN pg_user ON qs.userid = pg_user.usesysid
											    LEFT JOIN (SELECT DISTINCT query,
											                      SUM(ROWS) AS bcast_rows
											               FROM stl_bcast
											               GROUP BY 1) bcast ON bcast.query = wlm.query
											    LEFT JOIN (SELECT DISTINCT ex.query,
											                      COUNT(*) inner_bcast_count
											               FROM stl_explain ex,
											                    stv_wlm_query_state wlm
											               WHERE wlm.query = ex.query
											               AND   wlm.state = 'Running'
											               AND   ex.plannode LIKE ('%%DS_BCAST_INNER%%')
											               GROUP BY 1) ex ON ex.query = wlm.query
											  GROUP BY 1,
											           2,
											           3,
											           4,
											           5,
											           6,
											           7,
											           8,
											           9,
											           10
											  ORDER BY 1,
											           2,
											           3;""",
								'query4' : """SELECT TRIM(pgn.nspname) AS SCHEMA,
							       						 SUM(b.mbytes)
														 FROM (SELECT db_id,
														              id,
														              name,
														              SUM(ROWS) AS ROWS
														       FROM stv_tbl_perm a
														       GROUP BY db_id,
														                id,
														                name) AS a
														   JOIN pg_class AS pgc ON pgc.oid = a.id
														   JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
														   JOIN pg_database AS pgdb ON pgdb.oid = a.db_id
														   JOIN (SELECT tbl, COUNT(*) AS mbytes FROM stv_blocklist GROUP BY tbl) b ON a.id = b.tbl
														 GROUP BY 1							 
														 ORDER BY 2;""",
								'query5':"""SELECT tab_all.*,
												   tab_m1.m1_num_scan,
												   tab_m1.m1_row_scan,
												   tab_m1.m1_avg_time,
											       tab_w1.w1_num_scan,
											       tab_w1.w1_row_scan,
											       tab_w1.w1_avg_time,
											       tab_d1.d1_num_scan,
											       tab_d1.d1_row_scan,
											       tab_d1.d1_avg_time,
											       tab_h6.h6_num_scan,
											       tab_h6.h6_row_scan,
											       tab_h6.h6_avg_time,
											       tab_h3.h3_num_scan,
											       tab_h3.h3_row_scan,
											       tab_h3.h3_avg_time
											FROM (SELECT "schema" AS schemaname,
											             "table" AS tablename,
											             pct_used AS pct_mem_used,
											             unsorted AS unsorted_rows,
											             stats_off AS statistics,
											             encoded AS is_encoded,
											             diststyle,
											             (CASE WHEN sortkey1 LIKE ('%%INTERLEAVED%%') THEN 'INTERLEAVED' ELSE sortkey1 END) sortkey1,
											             skew_sortkey1,
											             skew_rows
											      FROM svv_table_info
											      ORDER BY 1,
											               2) tab_all
											  LEFT JOIN (SELECT DISTINCT scan.perm_table_name AS m1_table_name,
											                    COUNT(*) AS m1_num_scan,
											                    AVG("rows") AS m1_row_scan,
											                    AVG(DATEDIFF (seconds,scan.starttime,scan.endtime))::FLOAT AS m1_avg_time
											             FROM stl_scan scan
											             WHERE starttime > GETDATE () -INTERVAL '1 month'
											             AND   starttime < GETDATE ()
											             AND   endtime > '2001-01-01 00:00:00'
											             GROUP BY 1) tab_m1 ON tab_all.tablename = tab_m1.m1_table_name
											  LEFT JOIN (SELECT DISTINCT scan.perm_table_name AS w1_table_name,
											                    COUNT(*) AS w1_num_scan,
											                    AVG("rows") AS w1_row_scan,
											                    AVG(DATEDIFF (seconds,scan.starttime,scan.endtime))::FLOAT AS w1_avg_time
											             FROM stl_scan scan
											             WHERE starttime > GETDATE () -INTERVAL '1 week'
											             AND   starttime < GETDATE ()
											             AND   endtime > '2001-01-01 00:00:00'
											             GROUP BY 1) tab_w1 ON tab_all.tablename = tab_w1.w1_table_name
											  LEFT JOIN (SELECT DISTINCT scan.perm_table_name AS d1_table_name,
											                    COUNT(*) AS d1_num_scan,
											                    AVG("rows") AS d1_row_scan,
											                    AVG(DATEDIFF (seconds,scan.starttime,scan.endtime))::FLOAT AS d1_avg_time
											             FROM stl_scan scan
											             WHERE starttime > GETDATE () -INTERVAL '1 day'
											             AND   starttime < GETDATE ()
											             AND   endtime > '2001-01-01 00:00:00'
											             GROUP BY 1) tab_d1 ON tab_all.tablename = tab_d1.d1_table_name
											  LEFT JOIN (SELECT DISTINCT scan.perm_table_name AS h6_table_name,
											                    COUNT(*) AS h6_num_scan,
											                    AVG("rows") AS h6_row_scan,
											                    AVG(DATEDIFF (seconds,scan.starttime,scan.endtime))::FLOAT AS h6_avg_time
											             FROM stl_scan scan
											             WHERE starttime > GETDATE () -INTERVAL '6 hours'
											             AND   starttime < GETDATE ()
											             AND   endtime > '2001-01-01 00:00:00'
											             GROUP BY 1) tab_h6 ON tab_all.tablename = tab_h6.h6_table_name
											  LEFT JOIN (SELECT DISTINCT scan.perm_table_name AS h3_table_name,
											                    COUNT(*) AS h3_num_scan,
											                    AVG("rows") AS h3_row_scan,
											                    AVG(DATEDIFF (seconds,scan.starttime,scan.endtime))::FLOAT AS h3_avg_time
											             FROM stl_scan scan
											             WHERE starttime > GETDATE () -INTERVAL '3 hours'
											             AND   starttime < GETDATE ()
											             AND   endtime > '2001-01-01 00:00:00'											
             								GROUP BY 1) tab_h3 ON tab_all.tablename = tab_h3.h3_table_name;""",
             					'query6':"""SELECT DISTINCT scan.query AS query_id,
											       wlm.service_class AS queue,
											       scan.perm_table_name AS tablename,
											       CONVERT_TIMEZONE('Asia/Calcutta',wlm.wlm_start_time) AS query_start_time,
											       (COUNT(CASE WHEN scan.is_rrscan = 't' THEN scan.is_rrscan ELSE NULL END)::float/ COUNT(*)::float)*100 AS range_scan_pct,
											       AVG("rows") AS rows_scan,
											       AVG(DATEDIFF (seconds,scan.starttime,scan.endtime)) AS avg_time
											FROM stl_scan scan,
											     stv_wlm_query_state wlm
											WHERE wlm.state = 'Running'
											AND   wlm.query = scan.query
											GROUP BY 1,
											         2,
											         3,
											         4											
											ORDER BY 1;"""},
			'metric_query_dictionary': {
								'running_queries'	: ['query1'],
								'wlm_metrics' : ['query2'],
								'query_level_metrics' : ['query3','query6'],
								'diskspace_metrics' : ['query4'],
								'table_level_metrics' : ['query5']
										},
			'query_frequency_dictionary' : {
								'query1' : 1,
								'query2' : 1,
								'query3' : 1,
								'query4' : 100,
								'query6' : 1,
								'query5' : 1000 }
			},
	'metrics' : {},
	'utility' : {'sleep_config': 60,
				 'job_reset_time': 3600}
	}

