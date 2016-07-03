import json
import time
import requests
import sqlalchemy.orm
from sqlalchemy import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, mapper, clear_mappers
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, MetaData, Table, and_
import logging
from statsd import StatsClient
import os
import datetime
from datetime import timedelta

Base = declarative_base()

ist_delta = timedelta(hours=0,minutes=0)

log_identifier = 'system_tables'

class PostDB():

	def __init__(self,db_queue,database_config):
		mysql_endpoint = database_config['mysql_endpoint']
		mysql_user = database_config['mysql_user']
		mysql_port = database_config['mysql_port']
		mysql_dbname = database_config['mysql_dbname']
		mysql_pass = database_config['mysql_pass']
		statsd_ip = database_config['statsd_ip']
		statsd_port = database_config['statsd_port']
		self.opentsdb_url = database_config['opentsdb_url']
		query_tabname = database_config['mysql_table']['query_tab']
		table_tabname = database_config['mysql_table']['table_tab']
		query_scan_tab = database_config['mysql_table']['query_scan_tab']


		self.mysql_engine_string = mysql_endpoint+mysql_user+':'+mysql_pass+'@'+mysql_port+'/'+mysql_dbname
		self.db_queue = db_queue
		self.engine = create_engine(self.mysql_engine_string)
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()
		self.statsd = StatsClient(statsd_ip,statsd_port)
		self.RSQueryMonitor = self.get_query_object(query_tabname,self.engine)
		self.RSTableMonitor = self.get_table_object(table_tabname,self.engine)
		self.RSQueryScan = self.get_scan_object(query_scan_tab,self.engine)
	
	def post_db(self,metric_list):
		logging.info('%s : Waiting for json response on %d', log_identifier, os.getpid())
		#print '%s : Waiting for json response on %d' %(log_identifier, os.getpid())
		for i in range(0,len(metric_list)):
			payload = self.db_queue.get()
			logging.info('%s : JSON response received is %s', log_identifier, payload)
			#print '%s : JSON response received is %s' %(log_identifier, payload)
			for key in payload:
				if key == 'opentsdb':
					r = self.post_opentsdb(payload[key])
				elif key == 'mysql':
					self.post_mysql(payload[key])
				elif key == 'statsd':
					self.post_statsd(payload[key])

	def post_statsd(self,payload):
		for metric in range(0,len(payload)):
			for key in payload[metric]:
				self.statsd.gauge(key, payload[metric][key])

	def post_opentsdb(self,payload):
		r = requests.post(self.opentsdb_url,data=json.dumps(payload))
		logging.info('%s : HTTP response received is %s', log_identifier, r)
		print '%s : HTTP response received is %s' %(log_identifier, r)
		return r

	def post_mysql(self,payload):
		for key in payload:
			if key == 'rs_query_monitor':
				table_rows_list = payload[key]
				for row in table_rows_list:
					table_rows_dict = table_rows_list[row]
					if self.session.query(self.RSQueryMonitor).filter(self.RSQueryMonitor.query_id == table_rows_dict['query_id']).count() == 0:
						self.session.add(self.RSQueryMonitor(query_id=table_rows_dict['query_id'],username=table_rows_dict['username'].strip(),
							workmem=table_rows_dict['workmem'], num_diskhits=table_rows_dict['num_diskhits'], exec_time=table_rows_dict['exec_time'],
							queue_time=table_rows_dict['queue_time'], slot_count=table_rows_dict['slot_count'],
							starttime=table_rows_dict['starttime'],state=table_rows_dict['state'].strip(),queue=table_rows_dict['queue'],
							inner_bcast_count=table_rows_dict['inner_bcast_count'],bcast_rows=table_rows_dict['bcast_rows'],
							last_modified_on=datetime.datetime.utcnow()+ist_delta))
					else:
						row = self.session.query(self.RSQueryMonitor).filter(self.RSQueryMonitor.query_id == table_rows_dict['query_id']).first()
						row.queue = table_rows_dict['username'].strip()
						row.workmem = table_rows_dict['workmem']
						row.num_diskhits = table_rows_dict['num_diskhits'] + row.num_diskhits
						row.exec_time = table_rows_dict['exec_time']
						row.queue_time = table_rows_dict['queue_time']
						row.slot_count = table_rows_dict['slot_count']
						row.starttime = table_rows_dict['starttime']
						row.state = table_rows_dict['state'].strip()
						row.queue = table_rows_dict['queue']
						row.inner_bcast_count = table_rows_dict['inner_bcast_count']
						row.bcast_rows = table_rows_dict['bcast_rows']
						row.last_modified_on = datetime.datetime.utcnow()+ist_delta
				self.session.commit()
				max_lmd = self.session.query(func.max(self.RSQueryMonitor.last_modified_on)).all()[0][0]
				done_rows = self.session.query(self.RSQueryMonitor).filter(~self.RSQueryMonitor.state.in_('Done ')).filter(self.RSQueryMonitor.last_modified_on < max_lmd).all()
				for row in range(0,len(done_rows)):
					done_rows[row].state = 'Done'
				self.session.commit()
			if key == 'rs_table_monitor':
				table_rows_list = payload[key]
				for row in table_rows_list:
					table_rows_dict = table_rows_list[row]
					if self.session.query(self.RSTableMonitor).filter(and_(self.RSTableMonitor.schemaname == table_rows_dict['schemaname'].strip(), self.RSTableMonitor.tablename == table_rows_dict['tablename'].strip())).count() == 0:
						self.session.add(self.RSTableMonitor(schemaname=table_rows_dict['schemaname'].strip(),tablename=table_rows_dict['tablename'].strip(),pct_mem_used=table_rows_dict['pct_mem_used'],
							unsorted_rows=table_rows_dict['unsorted_rows'], statistics=table_rows_dict['statistics'], is_encoded=table_rows_dict['is_encoded'],diststyle=table_rows_dict['diststyle'],
							sortkey1=table_rows_dict['sortkey1'],skew_sortkey1=table_rows_dict['skew_sortkey1'],skew_rows=table_rows_dict['skew_rows'],m1_num_scan=table_rows_dict['m1_num_scan'],
							m1_row_scan=table_rows_dict['m1_row_scan'],m1_avg_time=table_rows_dict['m1_avg_time'],w1_num_scan=table_rows_dict['w1_num_scan'],w1_row_scan=table_rows_dict['w1_row_scan'],
							w1_avg_time=table_rows_dict['w1_avg_time'],d1_num_scan=table_rows_dict['d1_num_scan'],d1_row_scan=table_rows_dict['d1_row_scan'],d1_avg_time=table_rows_dict['d1_avg_time'],
							h6_num_scan=table_rows_dict['h6_num_scan'],h6_row_scan=table_rows_dict['h6_row_scan'], h6_avg_time=table_rows_dict['h6_avg_time'],h3_num_scan=table_rows_dict['h3_num_scan'],
							h3_row_scan=table_rows_dict['h3_row_scan'],h3_avg_time=table_rows_dict['h3_avg_time'],last_modified_on=datetime.datetime.utcnow()+ist_delta))
					else:
						row = self.session.query(self.RSTableMonitor).filter(and_(self.RSTableMonitor.schemaname == table_rows_dict['schemaname'].strip(), self.RSTableMonitor.tablename == table_rows_dict['tablename'].strip()))
						row.pct_mem_used = table_rows_dict['pct_mem_used']
						row.unsorted_rows = table_rows_dict['unsorted_rows']
						row.statistics = table_rows_dict['statistics']
						row.is_encoded = table_rows_dict['is_encoded']
						row.diststyle = table_rows_dict['diststyle']
						row.sortkey1 = table_rows_dict['sortkey1']
						row.skew_sortkey1 = table_rows_dict['skew_sortkey1']
						row.skew_rows = table_rows_dict['skew_rows']
						row.m1_num_scan = table_rows_dict['m1_num_scan']
						row.m1_avg_time = table_rows_dict['m1_avg_time']
						row.m1_row_scan = table_rows_dict['m1_row_scan']
						row.w1_num_scan = table_rows_dict['w1_num_scan']
						row.w1_row_scan = table_rows_dict['w1_row_scan']
						row.w1_avg_time = table_rows_dict['w1_avg_time']
						row.d1_num_scan = table_rows_dict['d1_num_scan']
						row.d1_row_scan = table_rows_dict['d1_row_scan']
						row.d1_avg_time = table_rows_dict['d1_avg_time']
						row.h6_num_scan = table_rows_dict['h6_num_scan']
						row.h6_row_scan = table_rows_dict['h6_row_scan']
						row.h6_avg_time = table_rows_dict['h6_avg_time']
						row.h3_num_scan = table_rows_dict['h3_num_scan']
						row.h3_row_scan = table_rows_dict['h3_row_scan']
						row.h3_avg_time = table_rows_dict['h3_avg_time']
						row.last_modified_on = datetime.datetime.utcnow()+ist_delta
				self.session.commit()
			if key == 'rs_scan_monitor':
				table_rows_list = payload[key]
				for row in table_rows_list:
					table_rows_dict = table_rows_list[row]
					if self.session.query(self.RSQueryScan).filter(and_(RSQueryScan.query_id == table_rows_dict['query_id'],self.RSQueryScan.tablename == table_rows_dict['tablename'].strip(), self.RSQueryScan.queue == table_rows_dict['queue'])):
						self.session.add(self.RSQueryScan(query_id=table_rows_dict['query_id'],queue=table_rows_dict['queue'],tablename=table_rows_dict['tablename'].strip(),
							query_start_time=table_rows_dict['query_start_time'], range_scan_pct=table_rows_dict['range_scan_pct'], rows_scan=table_rows_dict['rows_scan'],
							avg_time=table_rows_dict['avg_time'],last_modified_on=datetime.datetime.utcnow()+ist_delta))
					else:
						row = self.session.query(self.RSQueryScan).filter(and_(RSQueryScan.query_id == table_rows_dict['query_id'],self.RSQueryScan.tablename == table_rows_dict['tablename'].strip(), self.RSQueryScan.queue == table_rows_dict['queue']))
						row.range_scan_pct = table_rows_dict['range_scan_pct']
						row.rows_scan = table_rows_dict['rows_scan']
						row.avg_time = table_rows_dict['avg_time']
						row.last_modified_on = datetime.datetime.utcnow()+ist_delta
				self.session.commit()

	def get_query_object(self,tablename,engine):
		metadata = MetaData(bind=engine)
		rs_query_monitor = Table(tablename, metadata,
    	    Column('query_id', Integer(), primary_key=True),
    	    Column('username', String(255)),
    	    Column('workmem', String(255)),
    	    Column('num_diskhits', Integer()),
    	    Column('inner_bcast_count', Integer()),
    	    Column('bcast_rows', Integer()),
    	    Column('exec_time', Integer()),
    	    Column('slot_count', Integer()),
    	    Column('queue_time', Integer()),
    	    Column('starttime', DateTime(), default=datetime.datetime.utcnow()+ist_delta),
    	    Column('state', String(255)),
    	    Column('queue', Integer()),
    	    Column('last_modified_on', DateTime(), default=datetime.datetime.utcnow()+ist_delta))
		rs_query_monitor.create(checkfirst=True)
		clear_mappers()
		mapper(RSQueryMonitor, rs_query_monitor)
		return RSQueryMonitor

	def get_table_object(self,tablename,engine):
		metadata = MetaData(bind=engine)
		rs_tab_monitor = Table(tablename, metadata,
			Column('id', Integer(), primary_key=True),
			Column('schemaname', String(255), nullable=False),
			Column('tablename', String(255), nullable=False),
			Column('pct_mem_used', Float()),
			Column('unsorted_rows', Float()),
			Column('statistics', Float()),
			Column('is_encoded', String(255)),
			Column('diststyle', String(255)),
			Column('sortkey1', String(255)),
			Column('skew_sortkey1', String(255)),
			Column('skew_rows', Float()),
			Column('m1_num_scan', Float()),
			Column('m1_row_scan', Float()),
			Column('m1_avg_time', Float()),
			Column('w1_num_scan', Float()),
			Column('w1_row_scan', Float()),
			Column('w1_avg_time', Float()),
			Column('d1_num_scan', Float()),
			Column('d1_row_scan', Float()),
			Column('d1_avg_time', Float()),
			Column('h6_num_scan', Float()),
			Column('h6_row_scan', Float()),
			Column('h6_avg_time', Float()),
			Column('h3_num_scan', Float()),
			Column('h3_row_scan', Float()),
			Column('h3_avg_time', Float()),
			Column('last_modified_on', DateTime(), default=datetime.datetime.utcnow()+ist_delta))
		rs_tab_monitor.create(checkfirst=True)
		#clear_mappers()
		mapper(RSTableMonitor, rs_tab_monitor)
		return RSTableMonitor

	def get_scan_object(self,tablename,engine):
		metadata = MetaData(bind=engine)
		rs_scan_monitor = Table(tablename,metadata,
			Column('id', Integer(), primary_key=True),
			Column('query_id', Integer(),nullable=False),
			Column('queue', Integer(),nullable=False),
			Column('tablename', String(255),nullable=False),
			Column('query_start_time',DateTime(),default=datetime.datetime.utcnow()+ist_delta),
			Column('range_scan_pct', Integer()),
			Column('rows_scan', Integer()),
			Column('avg_time', Integer()),
			Column('last_modified_on',DateTime(),default=datetime.datetime.utcnow()+ist_delta))
		rs_scan_monitor.create(checkfirst=True)
		mapper(RSQueryScan,rs_scan_monitor)
		return RSQueryScan


class RSQueryMonitor(object):
	def __init__(self,query_id,username,workmem,num_diskhits,exec_time,slot_count,
				 inner_bcast_count,bcast_rows,
				 queue_time,starttime,state,queue,last_modified_on):
		self.query_id = query_id
		self.username = username
		self.inner_bcast_count = inner_bcast_count
		self.bcast_rows = bcast_rows
		self.workmem = workmem
		self.num_diskhits = num_diskhits
		self.exec_time = exec_time
		self.slot_count = slot_count
		self.queue_time = queue_time
		self.starttime = starttime
		self.state = state
		self.queue = queue
		self.last_modified_on = last_modified_on
	def __repr__(self):
		return "<RSQueryMonitor(query_id='%s', username='%s' workmem='%s', num_diskhits='%s', exec_time='%s', slot_count='%s', queue_time='%s', starttime='%s', state='%s', queue='%s', last_modified_on='%s')>" % (self.query_id, self.username, self.workmem, self.num_diskhits, self.exec_time, self.slot_count, self.queue_time, self.starttime, self.state, self.queue, self.last_modified_on)

class RSTableMonitor(object):
	def __init__(self,schemaname,tablename,pct_mem_used,unsorted_rows,statistics,is_encoded,diststyle,sortkey1,skew_sortkey1,skew_rows,
				m1_num_scan,m1_row_scan,m1_avg_time,w1_num_scan,w1_row_scan,w1_avg_time,d1_num_scan,d1_row_scan,d1_avg_time,h6_num_scan,
				h6_row_scan,h6_avg_time,h3_num_scan,h3_row_scan,h3_avg_time,last_modified_on):
		self.schemaname = schemaname
		self.tablename = tablename
		self.pct_mem_used = pct_mem_used
		self.unsorted_rows = unsorted_rows
		self.statistics = statistics
		self.is_encoded = is_encoded
		self.diststyle = diststyle
		self.sortkey1 = sortkey1
		self.skew_sortkey1 = skew_sortkey1
		self.skew_rows = skew_rows
		self.m1_num_scan = m1_num_scan
		self.m1_row_scan = m1_row_scan
		self.m1_avg_time = m1_avg_time
		self.w1_num_scan = w1_num_scan
		self.w1_row_scan = w1_row_scan
		self.w1_avg_time = w1_avg_time
		self.d1_num_scan = d1_num_scan
		self.d1_row_scan = d1_row_scan
		self.d1_avg_time = d1_avg_time
		self.h6_num_scan = h6_num_scan
		self.h6_row_scan = h6_row_scan
		self.h6_avg_time = h6_avg_time
		self.h3_num_scan = h3_num_scan
		self.h3_row_scan = h3_row_scan
		self.h3_avg_time = h3_avg_time
		self.last_modified_on = last_modified_on
	def __repr__(self):
		return "<RSTableMonitor(schemaname='%s', tablename='%s', pct_mem_used='%f', unsorted_rows='%s', statistics='%f', is_encoded='%s', diststyle='%s', sortkey1='%s', skew_sortkey1='%s', skew_rows='%s', last_modified_on='%s')>" % (self.schemaname, self.tablename, self.pct_mem_used, self.unsorted_rows, self.statistics, self.is_encoded, self.diststyle, self.sortkey1, self.skew_sortkey1, self.skew_rows, self.last_modified_on)

class RSQueryScan(object):
	def __init__(self,query_id,queue,tablename,query_start_time,range_scan_pct,rows_scan,avg_time,last_modified_on):
		self.query_id = query_id
		self.queue = queue
		self.tablename = tablename
		self.query_start_time = query_start_time
		self.range_scan_pct = range_scan_pct
		self.rows_scan = rows_scan
		self.avg_time = avg_time
		self.last_modified_on = last_modified_on
	def __repr__(self):
		return "<RSQueryScan(query_id='%s',queue='%s','tablename'='%s',range_scan_pct='%s',rows_scan='%s',avg_time='%s'>"