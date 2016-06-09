# Query and storage modules
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Utility modules
import json
import time
import requests
import os

# Multithreading
from Queue import Queue
import threading
from threading import Thread

# Exception cathcing modules
import traceback, os.path
import logging

# Post to database
from post_db import *

# Library get cloudwatch metrics
from cloudwatch_metrics import *

# Inherit base configuration
from base_config import *

db_queue = Queue()
query_result_queue = Queue()

query_result_df = {}

log_identifier = 'system_tables'

def create_rs_engine(log_identifier,redshift_connection):
	redshift_endpoint = redshift_connection['redshift_endpoint']
	redshift_user = redshift_connection['redshift_user']
	redshift_pass = redshift_connection['redshift_pass']
	port = redshift_connection['port']
	dbname = redshift_connection['dbname']
	engine_string = 'postgresql+psycopg2://'+redshift_user+':'+redshift_pass+'@'+redshift_endpoint+':'+str(port)+'/'+dbname
	print engine_string
	engine = create_engine(engine_string)
	logging.info('%s : Created redshift engine', log_identifier)
	print '%s : Created redshift engine' %(log_identifier)
	return engine

def sql_query(query_counter,engine,metric_list,query_frequency_dictionary,query_dictionary,sleep_config,log_identifier,redshift_connection,queue_push):
	# Get a list of only those queries that are divisible by the time period set by user
	query_list = [query for (query, period) in query_frequency_dictionary.items() if query_counter%query_frequency_dictionary[query] == 0]
	print query_list
	# Query redshift for each of the chosen queries
	for i in range(0,len(query_list)):
		try:
			print query_list[i]
			query_result_df[query_list[i]] = pd.read_sql_query(query_dictionary[query_list[i]],engine)
		except:
			print 'Something broke. connection failure'
			logging.exception('%s : Redshift connection failure', log_identifier)
			traceback.extract_stack()
			#print type(exception).__name__
			print query_counter
			time.sleep(sleep_config)
			engine = create_rs_engine(log_identifier=log_identifier,redshift_connection=redshift_connection)
			continue
	# Increment the count by 1
	query_counter += 1
	# Put the dataframes on a queue consumed by all threads.
	for i in range(0,queue_push):
		query_result_queue.put(query_result_df)

	return query_counter

def initiate_monitoring(config_dict):

	{base_config[key].update(val) for (key,val) in config_dict.items()}
	config_dict = base_config

	log_file = config_dict['utility']['LOG_FILE']
	metric_query_dictionary = config_dict['query']['metric_query_dictionary']
	redshift_connection = config_dict['redshift_connection']
	job_reset_time = config_dict['utility']['job_reset_time']
	sleep_config = config_dict['utility']['sleep_config']
	query_dictionary = config_dict['query']['query_dictionary']
	query_frequency_dictionary = config_dict['query']['query_frequency_dictionary']
	database_config = config_dict['database']

	# The metric collector would sleep every <job_reset_time> seconds. This variable will store the start time for it
	sys_rs_job_start_time = time.time()

	os.system('rm '+log_file)

	logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(message)s')

	log_identifier = 'system_tables'

	logging.info('%s : Start logging system_tables_rs metrics', log_identifier)
	print '%s : Start logging system_tables_rs metrics' %(log_identifier)

	# Create an engine for connecting and querying to redshift
	engine = create_rs_engine(log_identifier,redshift_connection)

	# Get the name of all the metrics and import them dynamically
	metric_list = [metric for (metric,query) in metric_query_dictionary.items()]
	metrics = map(__import__,metric_list)

	# Get total number of dataframes to push to queue
	queue_push = 0
	for key in metric_query_dictionary:
		queue_push += len(metric_query_dictionary[key])

	# Define an event to signal all threads to terminate
	stop_thread = threading.Event()

	# Spawn threads for every metric
	for i in range(0,len(metric_list)):
		worker = Thread(target=metrics[i].run,args=(i,query_result_queue,db_queue,stop_thread,config_dict['metrics'],))
		worker.setDaemon(True)
		worker.start()

	# A counter of how many times the redshift query API aws called
	query_counter = 0

	# Create an instance of PostDB()
	post_db = PostDB(db_queue,database_config)

	# Create Cloudwatch metrics instance
	cloudwatch_metrics = CloudWatchMetrics(config_dict)

	while 1:
		
		# Query the redshift cluster
		query_counter = sql_query(query_counter=query_counter,engine=engine,metric_list=metric_list,
			query_frequency_dictionary=query_frequency_dictionary,query_dictionary=query_dictionary,
			sleep_config=sleep_config,log_identifier=log_identifier,redshift_connection=redshift_connection,queue_push=queue_push)
		query_result_queue.join()

		# Wait for each of the database queues to join
		post_db.post_db(metric_list=metric_list)
		db_queue.join()

		# Get hardware metrics from cloudwatch
		cloudwatch_metrics.get_metrics()

		print 'Sleep for a while'
		# Sleep for a configured time
		time.sleep(sleep_config)

		sys_rs_job_end_time = time.time()

		if (sys_rs_job_end_time - sys_rs_job_start_time) > job_reset_time:
			stop_thread.set()
			break;

