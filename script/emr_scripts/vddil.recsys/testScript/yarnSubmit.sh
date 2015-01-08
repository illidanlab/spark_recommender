#!/bin/bash

SBTJAR=/home/hadoop/aws_test/samsung-vd-recommender-system_2.10-1.0.jar
SPARK_BIN=/home/hadoop/spark/bin/
JOB_XML="hdfs:///aws_test_job.xml"

#export HADOOP_CONF_DIR=/etc/hadoop/conf
#export YARN_CONF_DIR=/etc/hadoop/conf
#export HADOOP_CLASSPATH=$CLASSPATH

#To deploy on yarn cluster
$SPARK_BIN/spark-submit \
      --class com.samsung.vddil.recsys.Pipeline \
     --master yarn-cluster \
     --driver-memory $1 --executor-memory $2 --executor-cores $3 --num-executors $4  $SBTJAR $JOB_XML
