import pandas as pd
import time
import copy
import numpy as np
import logging
from thread_utils import *

def compute_metric(thread_num,df_response):

	logging.info('%s : %d thread : computing json response for running_queries', 'system_tables',thread_num)
	print '%s : %d thread : computing json response for running_queries' %('system_tables',thread_num)

	return {user_name.rstrip():count for (user_name,count) in dict(pd.Series(df_response['user_name']).value_counts()).items()}

## Thread starts here
def run(thread_num,query_result_queue,db_queue,stop_thread,config_dict):

	metric_name = 'redshift.RunningQueries'
	db = 'opentsdb'
	thread_read = ThreadRead()
	thread_write = ThreadWrite(config_dict['general']['cluster_name'])

	while (not stop_thread.is_set()):

		payload = {}
		df_response = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query1')
		metric_dictionary = compute_metric(thread_num=thread_num,df_response=df_response)
		payload['opentsdb'] = thread_write.get_payload(db='opentsdb',metric_name=metric_name,metric_dictionary=metric_dictionary,tag_name='user_name')
		payload['statsd'] = thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=metric_dictionary,tag_name='null')
		thread_write.write_payload(payload=payload,db_queue=db_queue)