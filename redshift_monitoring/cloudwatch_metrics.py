# Libraries to get cloudwatch metrics
from datetime import datetime
from datetime import timedelta
import boto3
from boto3.session import Session

import logging
#from statsd import StatsClient
#import json
import time
#import requests

# Post to database
from post_db import *


class CloudWatchMetrics():

	def __init__(self,config_dict):
		# Create boto3 session for getting cloudwatch metrics
		session = Session(aws_access_key_id=config_dict['redshift_connection']['aws_access_key_id']
			,aws_secret_access_key=config_dict['redshift_connection']['aws_secret_access_key']
			,region_name=config_dict['redshift_connection']['region_name'])
		self.cw = session.client('cloudwatch')
		self.name_space = 'AWS/Redshift'
		self.metric_name = ['CPUUtilization',
				  'NetworkReceiveThroughput',
				  'NetworkTransmitThroughput',
				  'PercentageDiskSpaceUsed',
				  'ReadIOPS',
				  'ReadLatency',
				  'ReadThroughput',
				  'WriteIOPS',
				  'WriteLatency',
				  'WriteThroughput']
		self.period = 60
		self.statistics = ['Average']
		self.unit = ['Percent',
			'Bytes/Second',
			'Bytes/Second',
			'Percent',
			'Count/Second',
			'Seconds',
			'Bytes/Second',
			'Count/Second',
			'Seconds',
			'Bytes/Second']
		self.log_identifier = 'cw_metrics'
		self.cluster_name = config_dict['redshift_connection']['cluster_name']
		self.num_nodes = config_dict['redshift_connection']['num_nodes_cluster']
		self.post_db = PostDB(db_queue=None,database_config=config_dict['database'])

	def get_metrics(self):
		response = [0] * len(self.metric_name)
		json_response_node = {}
		json_opentsdb = {}
		tags = {}
		tags['ClusterIdentifier'] = self.cluster_name

		for node_iter in range(0,self.num_nodes):
		
			start_time = datetime.datetime.utcnow() - timedelta(minutes=4)
			end_time = datetime.datetime.utcnow() - timedelta(minutes=3)
	
			if node_iter == self.num_nodes-1:
				node = 'Leader'
				dimensions = [{'Name': 'ClusterIdentifier', 'Value': self.cluster_name}, 
						  	  {'Name': 'NodeID', 'Value': 'Leader'}]
			else:
				node = 'Compute-'+str(node_iter)
				dimensions = [{'Name': 'ClusterIdentifier', 'Value': self.cluster_name}, 
						  {'Name': 'NodeID', 'Value': 'Compute-'+str(node_iter)}]
	
			for i in range(0,len(self.metric_name)):
				response[i] = self.cw.get_metric_statistics(Namespace=self.name_space,
									MetricName=self.metric_name[i],
									Dimensions=dimensions,
									StartTime=start_time,
									EndTime=end_time,
									Period=self.period,
									Statistics=self.statistics,
									Unit=self.unit[i])
			
				if response[i]['Datapoints'] == []:
					logging.info('%s : %s metric has no Datapoints on node %s', self.log_identifier, self.metric_name[i], node)
					print '%s : %s metric has no Datapoints on node %s' %(self.log_identifier, self.metric_name[i], node)
				else:
					self.post_db.post_statsd([{'redshift.'+response[i]['Label']+str('.')+str(node)+'.'+self.cluster_name : response[i]['Datapoints'][0][self.statistics[0]]}])
					#statsd.gauge('redshift.'+response[i]['Label']+str('.')+str(node), response[i]['Datapoints'][0][self.statistics[0]])
					#json_opentsdb['metric'] = str('redshift.')+response[i]['Label']
					#json_opentsdb['timestamp'] = int(time.time())
					#json_opentsdb['value'] = response[i]['Datapoints'][0][self.statistics[0]]
					#tags['Node'] = node
					#json_opentsdb['tags'] = tags
					
					#logging.info('%s : JSON posted is %s', self.log_identifier, json_opentsdb)
					#print '%s : JSON posted is %s' %(self.log_identifier, json_opentsdb)
					#self.post_db.post_opentsdb(json_opentsdb)
					#r = requests.post(url,data=json.dumps(json_opentsdb))
					#logging.info('%s : HTTP Response is %s', self.log_identifier, r)
					#print '%s : HTTP Response is %s' %(self.log_identifier, r)


