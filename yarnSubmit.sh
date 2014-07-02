#!/bin/bash

DIR=/home/m3.sharma/installSrc/sparkHQ/spark
MVNJAR=$HOME/dev/github/recsys-spark/target/recsys-spark-0.0.1.jar
SBTJAR=$HOME/dev/github/recsys-spark/target/scala-2.10/samsung-vd-recommender-system_2.10-1.0.jar
SPARK_BIN=$HOME/installSrc/spark-1.0.0-bin-hadoop2/bin/

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=$CLASSPATH
export SPARK_JAVA_OPTS+="-Dspark.shuffle.spill=false"
#$DIR/bin/spark-class org.apache.spark.deploy.yarn.Client \
$SPARK_BIN/spark-submit \
      --class com.samsung.vddil.recsys.TestObj \
	    --master yarn \
	    --deploy-mode client \
	    --executor-memory 2G --executor-cores 2 --num-executors 100  $SBTJAR

