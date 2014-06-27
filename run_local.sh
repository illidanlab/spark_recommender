#!/usr/bin/env bash
#
# This is the run file wrapper for local running.
#
# Reference: 
#   http://docs.oracle.com/javase/tutorial/deployment/jar/update.html
#

# this file needs to be updated for each version. 
jarFile="./target/recsys-spark-0.0.1.jar"

# check jar utility 
jarCMD=`which jar | wc -l`
if [ $jarCMD -ge 0 ]; 
then
	echo "jar command found. "
else
	echo "ERROR the jar command not found. "
	exit 1
fi


if [ $# -ge 1 ];
then 

	jobFile=$1

	echo "Job file specified: "$1

	if [ -f $jobFile ];
	then
		echo "Job file found."
		echo "Injecting job file into recommender system jar file..."
		echo "Running: jar uf $jarFile $jobFile"
		#jar uf $jarFile $jobFile
		echo "Running: java -jar $jarFile"
		#java -jar $jarFile
		echo "Done."
		
		exit 0
	else 
		echo "ERROR: Job file not found. "
		exit 2
	fi
else

echo "ERROR: No job file found! "
exit 3

fi