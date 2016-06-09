import pandas as pd
import time
import copy
import numpy as np
import logging
from thread_utils import *

def compute_metric(thread_num,df_response):
	logging.info('%s : %d thread : computing json response for query_level_metrics', 'system_tables',thread_num)
	print '%s : %d thread : computing json response for query_level_metrics' %('system_tables',thread_num)

	return df_response.to_dict('index')

class ComputeMetric():

	def __init__(self,thread_num,df_response_query,df_response_scan):
		logging.info('%s : %d thread : computing json response for query_level_metrics', 'system_tables',thread_num)
		print '%s : %d thread : computing json response for query_level_metrics' %('system_tables',thread_num)
		self.df_response_query = df_response_query
		self.df_response_scan = df_response_scan
		self.thread_num = thread_num

	def aggregate_data_query(self,df_response_query):
		df_response_query = df_response_query.replace(np.nan,0,regex=True)
		return df_response_query.replace({'username':{0:'NULL'}},regex=True).to_dict('index')

	def aggregate_data_scan(self,df_response_scan):
		return df_response_scan.to_dict('index')

	def query_time_series(self):
		df_dict = self.df_response_query[['query_id','workmem','num_diskhits']].set_index('query_id').to_dict('index')
		metric_name_list = ['redshift.QueryLevelMetrics'+'.'+str(key) for (key,val) in df_dict.items()]
		metric_val_list = [df_val for (query_id,df_val) in df_dict.items()]
		return metric_name_list, metric_val_list

## Thread starts here
def run(thread_num,query_result_queue,db_queue,stop_thread,config_dict):

	metric_name = 'redshift.QueryLevelMetrics'
	query_table_name = 'rs_query_monitor'
	scan_table_name = 'rs_scan_monitor'
	thread_read = ThreadRead()
	thread_write = ThreadWrite(config_dict['general']['cluster_name'])

	while (not stop_thread.is_set()):

		payload = {}
		df_response_query = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query3')
		df_response_scan = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query6')
		compute_metric = ComputeMetric(thread_num,df_response_query,df_response_scan)

		metric_dictionary = compute_metric.aggregate_data_query(df_response_query)
		payload['mysql'] = thread_write.get_payload(db='mysql',metric_name=query_table_name,metric_dictionary=metric_dictionary,tag_name='user_name')

		metric_dictionary = compute_metric.aggregate_data_scan(df_response_scan)
		payload['mysql'][scan_table_name] = thread_write.get_payload(db='mysql',metric_name=scan_table_name,metric_dictionary=metric_dictionary,tag_name='user_name')[scan_table_name]

		metric_name_list, metric_dictionary = compute_metric.query_time_series()
		payload['statsd'] = [val[0] for val in map(thread_write.get_payload_statsd,metric_name_list,metric_dictionary)]

		print payload['mysql']

		thread_write.write_payload(payload=payload,db_queue=db_queue)