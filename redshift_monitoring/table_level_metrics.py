import pandas as pd
import time
import copy
import numpy as np
import logging
from thread_utils import *

class ComputeMetric():

	def __init__(self,thread_num,df_response):
		logging.info('%s : %d thread : computing json response for table_metrics', 'system_tables',thread_num)
		print '%s : %d thread : computing json response for table_metrics' %('system_tables',thread_num)
		self.df_response = df_response
		self.thread_num = thread_num

	def dist_sort_list(self):
		return self.df_response.replace(np.nan,19700101,regex=True).to_dict('index')

## Thread starts here
def run(thread_num,query_result_queue,db_queue,stop_thread,config_dict):

	metric_name = 'redshift.TableLevelMetrics'
	table_name = 'rs_table_monitor'
	thread_read = ThreadRead()
	thread_write = ThreadWrite(config_dict['general']['cluster_name'])

	while (not stop_thread.is_set()):
		payload = {}
		df_response, error = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query5')
		if error == -1:
			print 'Something broke. Skip this run of %s' %(metric_name)
		else:
			compute_metric = ComputeMetric(thread_num,df_response)
			metric_dictionary = compute_metric.dist_sort_list()
			payload['mysql'] = thread_write.get_payload(db='mysql',metric_name=table_name,metric_dictionary=metric_dictionary,tag_name='user_name')
		thread_write.write_payload(payload=payload,db_queue=db_queue)
