import logging 
import time
import copy

class ThreadRead():

	## Read redshift query in query queue
	def read_query_result(self,thread_num,query_result_queue,metric_name,query_name):
		logging.info('%s : %d thread : waiting for result from redshift in %s' , 'system_tables', thread_num, metric_name)

		try:
			df_packet = query_result_queue.get()
			#print '%d thread: Inside thread read' %(thread_num)
			key_list = []
			for key in df_packet:
				key_list.append(key)
			print '%d thread: %s' %(thread_num,key_list)
			df_response = df_packet[query_name]
			error = 0
			logging.info('%s : %d thread : obtained df_response for %s and %s' , 'system_tables', thread_num, metric_name, query_name)
			print '%s : %d thread : obtained df_response for %s' %('system_tables',thread_num, metric_name)	
		except:
			df_response = None
			error = -1
			print '%s : %d thread : The thread read failed. Most likely cause is %s timed out' %('system_tables',thread_num, query_name)
			pass
		query_result_queue.task_done()
		return df_response, error

class ThreadWrite():

	def __init__(self,cluster_name):
		self.cluster_name = cluster_name
	## Create a payload for each of the databases
	def get_payload(self,db,metric_name, metric_dictionary,tag_name):
		if db == 'opentsdb':
			payload = self.get_payload_opentsdb(metric_name,metric_dictionary,tag_name)
		elif db == 'statsd':
			payload = self.get_payload_statsd(metric_name,metric_dictionary)
		elif db == 'mysql':
			payload = {metric_name : metric_dictionary}
		return payload

	def get_payload_statsd(self,metric_name,input_dict):
		if input_dict == {}:
			return [{metric_name+'.default'+'.'+self.cluster_name :0}]
		else:
			return [{metric_name+'.'+str(key)+'.'+self.cluster_name : value for (key,value) in input_dict.items()}]

	## Create a payload for opentsdb database
	def get_payload_opentsdb(self,metric_name,input_dict,tag_name):
		
		json_opentsdb = {}
		tags = {}
		tags['ClusterIdentifier'] = self.cluster_name
		json_queue = []
	
		if input_dict == {}:
			json_opentsdb['metric'] = str(metric_name)
			json_opentsdb['timestamp'] = int(time.time())
			json_opentsdb['value'] =  0
			json_opentsdb['tags'] = tags
			json_queue.append(json_opentsdb)
	
		else:
			for key in input_dict:
				json_opentsdb['metric'] = str(metric_name)
				json_opentsdb['timestamp'] = int(time.time())
				json_opentsdb['value'] = float(input_dict[key])
				tags[tag_name] = key
				json_opentsdb['tags'] = tags
				json_queue.append(copy.deepcopy(json_opentsdb))		
	
		return json_queue

	def write_payload(self,payload,db_queue):
		db_queue.put(payload)
		db_queue.task_done()
