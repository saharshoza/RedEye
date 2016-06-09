import pandas as pd
import time
import copy
import numpy as np
import logging
from thread_utils import *

def compute_metric(thread_num,df_response,total_disk_space_mb):

	logging.info('%s : %d thread : computing json response for diskspace_metrics', 'system_tables',thread_num)
	print '%s : %d thread : computing json response for diskspace_metrics' %('system_tables',thread_num)

	return {schema:(float(diskspace[0]*100)/float(total_disk_space_mb)) for (schema,diskspace) in df_response.set_index('schema').T.to_dict('list').items()}

## Thread starts here
def run(thread_num,query_result_queue,db_queue,stop_thread,config_dict):

	metric_name = 'redshift.DiskSchemaUsage'
	db = 'opentsdb'
	thread_read = ThreadRead()
	thread_write = ThreadWrite(config_dict['general']['cluster_name'])

	while (not stop_thread.is_set()):

		payload = {}
		df_response = thread_read.read_query_result(thread_num=thread_num,query_result_queue=query_result_queue,metric_name=metric_name,query_name='query4')
		metric_dictionary = compute_metric(thread_num=thread_num,df_response=df_response,total_disk_space_mb=config_dict['diskspace_metrics']['total_disk_space_mb'])
		payload['opentsdb'] = thread_write.get_payload(db='opentsdb',metric_name=metric_name,metric_dictionary=metric_dictionary,tag_name='schema')
		payload['statsd'] = thread_write.get_payload(db='statsd',metric_name=metric_name,metric_dictionary=metric_dictionary,tag_name='schema')
		thread_write.write_payload(payload=payload,db_queue=db_queue)
