#!/usr/bin/env python

'''
Monitoring console for AWS job. Please install BeautifulSoup, install and configure AWS tools.

@author Jiayu Zhou
'''

import sys
import subprocess
import os
import json
import urllib2
import re
from subprocess import call
from bs4 import BeautifulSoup
from time import gmtime, strftime
from time import sleep

# usage of the program. 
def show_usage():
	print "USAGE: emr_monitor [cluster_id]"

# check the master DNS given a cluster ID. 
def get_master_dns(cluster_id):
	#from cluster_id to master_id
	#if the cluster id is not found or master dns not available, return empty string. 
	tt = os.popen("aws emr describe-cluster --output json --cluster-id " + cluster_id + " | grep MasterPublicDnsName").read()
	if not 'MasterPublicDnsName' in tt:
		print 'Cluster Master DNS does not exist.'
		return ''
	master_dns = tt.split("\"")[3]
	print 'Master DNS: ', master_dns

	return master_dns

def get_status_str(cluster_id):
	ss = os.popen("aws emr describe-cluster --output json --cluster-id " + cluster_id + " | grep State").read()
	status = ss.split("\"")[3]
	#print 'Cluster Status: ', status
	return status

# check if the cluster is running 
def is_cluster_running(cluster_id):
	status = get_status_str(cluster_id)
	return is_status_running(status)

# check if the cluster is waiting 
def is_cluster_waiting(cluster_id):
	status = get_status_str(cluster_id)
	return is_status_waiting(status)

# check if the cluster is terminated.
def is_cluster_terminated(cluster_id):
	status = get_status_str(cluster_id)
	return is_status_terminated(status)

# check if the cluster is in on of the starting phases (STARTING or BOOTSTRAPPING)
def is_cluster_starting(cluster_id):
	status = get_status_str(cluster_id)
	return is_status_starting(status)


# check if the status string is starting
def is_status_starting(status):
	return  (('STARTING' in status) | ('BOOTSTRAPPING' in status))

# check if the status string is running
def is_status_running(status):
	return 'RUNNING' in status	

# check if the status string is running
def is_status_running_or_waiting(status):
	return (('RUNNING' in status) | ('WAITING' in status))

# check if the status string is waiting
def is_status_waiting(status):
	return 'WAITING' in status

# check if the status string is terminated
def is_status_terminated(status):
	return (('COMPLETED' in status) | ('SHUTTING_DOWN' in status) | ('TERMINATED' in status) | ('FAILED' in status))


# get formatted string for time 
def time_str():
	return strftime("%Y-%m-%d %H:%M:%S", gmtime())

# given a master DNS, the routine checks the yarn resource manager and then go through each 
# hadoop/spark job. 
def search_hadoop_application(master_dns, status):
	resource_manager_loc = "http://" + master_dns + ":9026/cluster"
	#print resource_manager_loc
	resource_manager_content = get_url_content(resource_manager_loc)
	#print resource_manager_content

	# identify running applications. 
	app_list = [ elem for elem in resource_manager_content.split('\n') if (('application_' in elem) & ('RUNNING' in elem)) ]
	if app_list:
		print time_str(), 'Identified ', len(app_list), ' running jobs.'
		#if there are apps in the app_list, we start to process each app. 
		for app_item in app_list:
			try:
				# extract application id
				searchObj = re.findall( r">application_.*</a>\",\"hadoop\"", app_item)	
				application_id = searchObj[0].split(">")[1].split("<")[0]

				# process application case by case. 
				# the case can be extended to handle other types of jobs. 
				if 'SPARK' in app_item:
					parse_spark_info(master_dns, application_id)
				elif 'MAPREDUCE' in app_item:
					parse_mapreduce_info(master_dns, application_id)
				else:
					parse_otherjob_info(master_dns, application_id)

			except:
				print time_str(), 'Error in reading application information: ', application_id
	else:
		if is_status_waiting(status): 
			print time_str(), " Cluster is waiting. Please shut down the cluster or start new jobs."
		else:
			print time_str(), ' Cluster is doing something but no running Hadoop jobs. '

# status for active spark job, 
def parse_spark_info(master_dns, application_id):

	spark_page_loc = "http://" + master_dns + ":9046/proxy/"+application_id
	#print spark_page_loc

	response = urllib2.urlopen(spark_page_loc)
	soup = BeautifulSoup(response.read(), from_encoding=response.info().getparam('charset'))

	stage_complete = [ liItem for liItem in soup.find_all('li') if 'Completed' in liItem.text ][0].text.split('\n')[2].strip()
	stage_active   = [ liItem for liItem in soup.find_all('li') if 'Active' in liItem.text ][0].text.split('\n')[2].strip()
	stage_failed   = [ liItem for liItem in soup.find_all('li') if 'Failed' in liItem.text ][0].text.split('\n')[2].strip()

	print '>> Spark Job [Completed',stage_complete,'][Failed', stage_failed, '][Active',stage_active, '] Track URL: ', spark_page_loc

# status for active mapreduce jobs
def parse_mapreduce_info(master_dns, application_id):
	print '>> MapReduce Job [',application_id,']'

# status for other active jobs. 
def parse_otherjob_info(master_dns, application_id):
	print '>> Other Hadoop Job [',application_id,']'	

# read the content of a given webpage into a string. 
def get_url_content(address):
	response = urllib2.urlopen(address)
	html = response.read()
	return html

# the sleeping rate should be larger than 40, if not the it will exceed the maximal rate of AWS API 
def sleeping_rate():
	return 60

if __name__ == "__main__":
	if len(sys.argv) < 2:
		show_usage()
	else:
		cluster_id = sys.argv[1]

		# starting. 
		while is_cluster_starting(cluster_id):
			print time_str()," Cluster is being prepared. "
			sleep(sleeping_rate())

		# if the cluster is not in starting phase, then 
		# we try to retrieve master DNS info. 
		master_dns = get_master_dns(cluster_id)

		if not master_dns:
			print time_str(), " Error: Master DNS not available. "
		else:
			try:
				# if master DNS is available.
				while(True):
					status = get_status_str(cluster_id)
					if is_status_running_or_waiting(status):
						# if the cluster is currently running, we try to check yarn 
						# applications on master DNS 
						#
						# Note: even when the cluster is waiting we will still have 
						#       to check resource manager, because users may manually 
						#       submit hadoop jobs through the console.
						search_hadoop_application(master_dns, status)

						# and sleep for a while. 
						sleep(sleeping_rate())
					else:
						print time_str(), " Cluster is down. "
						break
			except:
				print time_str(), " Error: ", sys.exc_info()[0]

