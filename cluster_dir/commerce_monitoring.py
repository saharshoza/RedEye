from config import *
import os, sys, inspect

if __name__ == "__main__":

	cmd_folder = os.path.realpath('../')

	## Azkaban operates from different directory, but adds this path as first element in its system path
	if sys.path[0] == '':
		sys.path.insert(1,cmd_folder)
	else:
		sys.path.insert(1,sys.path[0][:sys.path[0].rfind("/")])

	redshift_monitoring = map(__import__,['redshift_monitoring'])

	cmd_folder = os.path.realpath('../redshift_monitoring')
	if sys.path[0] == '':
		sys.path.insert(1,cmd_folder)
	else:
		sys.path.insert(1,sys.path[0][:sys.path[0].rfind("/")]+'/redshift_monitoring')

	redshift_monitoring[0].initiate_monitoring(config_dict)

	