# Motivation
Redshift is the distributed data warehousing solution by AWS. The redshift monitoring console displays:  

1. Hardware metrics like CPU, Disk Space, Read/Write IOPs for the clusters.  
2. Query level information such as:  
	a. Expected versus actual execution plan  
	b. Username query mapping  
	c. Time Taken for query  

## Redeye Overview
The tool gathers the following metrics on redshift performance:

1. Hardware Metrics:  
	a. CPU Utilization
	b. Disk Space Utilization
	c. Read/Write IOPs
	d. Read Latency/Throughput
	e. Write Latency/Throughput
	f. Network Transmit/Throughput

2. Software Metrics:  
	a. Aggregate Metrics:  
		i. Queries fired by user  
		ii. Queries in Queue/Running/Returning State in each queue  
		iii. Average time taken in Queue/Running/Returning State in each queue  
	b. Query Level Metrics: Number of diskhits, Number of rows broadcast across nodes, queue used and user at a query_id granularity  
	c. Table Level Metrics: Least and most used tables in warehouse  

# Requirements:
For this tool to work, you will need:

1. Statsd endpoint
2. Opentsdb endpoint
3. MySQL endpoint
4. Redshift Credentials

# Quick Setup

`vim $REDYEYE/redshift_monitoring/base_config.py`   
This file contains configuration properties common to all the clusters. Edit the statsd, opentsdb and mysql endpoints here.  
`mkdir $REDEYE/cluster_new`  
`cp $REDEYE/cluster_dir/config.py $REDEYE/cluster_new/`  
`vim $REDEYE/cluster_new/config.py`  
This file will contain configuration specific to your redshift console. Change the credentials to point to your cluster. You should provide username and password that can access the redshift system tables.  
`cp $REDEYE/cluster_dir/start_monitoring.py $REDEYE/cluster_new/`  

`python $REDEYE/cluster_new/start_monitoring.py`  

The tool has been designed to run for an hour. It can be scheduled as part of a workflow scheduler like Azkaban to keep the monitoring persistent.
