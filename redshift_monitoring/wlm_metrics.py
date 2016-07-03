import pandas as pd
import time
import copy
import numpy as np
import logging
from thread_utils import *		

class ComputeMetric():

	def __init__(self,thread_num,df_response):
		logging.info('%s : %d thread : computing json response for wlm_metrics', 'system_tables',thread_num)
		print '%s : %d thread : computing json response for wlm_metrics' %('system_tables',thread_num)
		self.df_response = df_response
		self.thread_num = thread_num

	def running_query_count(self):
		return dict(self.df_response[self.df_response['state'] == 'Running         ']['service_class'].value_counts()), 'redshift.WlmRunningQueries'

	def queued_query_count(self):
		return dict(self.df_response[self.df_response['state'] == 'QueuedWaiting   ']['service_class'].value_counts()), 'redshift.WlmQueuedQueries'

	def returning_query_count(self):
		return dict(self.df_response[self.df_response['state'] == 'Returning       ']['service_class'].value_counts()), 'redshift.WlmReturningQueries'

	def running_query_avg_time(self):
		return dict(self.df_response[self.df_response['state'] == 'Running         '].groupby('service_class').agg({'exec_time':np.mean})['exec_time']), 'redshift.WlmRunningQueriesAvgTime'

	def queued_query_avg_time(self):
		return dict(self.df_response[self.df_response['state'] == 'QueuedWaiting   '].groupby('service_class').agg({'queue_time':np.mean})['queue_time']), 'redshift.WlmQueuedQueriesAvgTime'

	def returning_query_avg_time(self):
		return  dict(self.df_response[self.df_response['state'] == 'Returning       '].groupby('service_class').agg({'exec_time':np.mean})['exec_time']), 'redshift.WlmReturningQueriesAvgTime'

## Thread starts here
def run(thread_num,query_result_queue,db_queue,stop_thread,config_dict):

	metric_name = 'redshift.WlmMetrics'
	db = 'opentsdb'
	thread_read = ThreadRead()
	thread_write = ThreadWrite(config_dict['general']['cluster_name'])
	payload = {}

	while (not stop_thread.is_set()):
		payload = {}
		payload_opentsdb = []
		payload_statsd = []

		df_response, error = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query2')
		if error == -1:
			print 'Something broke. Skip this run of %s' %(metric_name)
		else:
			compute_metric = ComputeMetric(thread_num=thread_num,df_response=df_response)
			running_query_count, metric_name = compute_metric.running_query_count()
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=running_query_count,tag_name='service_class'))
			queued_query_count, metric_name = compute_metric.queued_query_count()
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=queued_query_count,tag_name='service_class'))
			returning_query_count, metric_name = compute_metric.returning_query_count()
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=returning_query_count,tag_name='service_class'))
			running_query_avg_time, metric_name = compute_metric.running_query_avg_time()
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=running_query_avg_time,tag_name='service_class'))
			queued_query_avg_time, metric_name = compute_metric.queued_query_avg_time()
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=queued_query_avg_time,tag_name='service_class'))
			returning_query_avg_time, metric_name = compute_metric.returning_query_avg_time()
			payload_opentsdb.extend(thread_write.get_payload(db='opentsdb',metric_name=metric_name,metric_dictionary=returning_query_avg_time,tag_name='service_class'))
			payload_statsd.extend(thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=returning_query_avg_time,tag_name='service_class'))
			payload['statsd'] = payload_statsd
		thread_write.write_payload(payload=payload,db_queue=db_queue)