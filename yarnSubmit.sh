#!/bin/bash
MVNJAR=$PWD/target/recsys-spark-0.0.1.jar
SBTJAR=$PWD/target/scala-2.10/samsung-vd-recommender-system_2.10-1.0.jar
SPARK_BIN=$HOME/installSrc/spark/spark/bin/
JOB_XML="hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/test_job.xml"

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=$CLASSPATH
#export SPARK_JAVA_OPTS+="-Dspark.shuffle.spill=false"
#export SPARK_JAVA_OPTS+="-Xmx6g"

sbt package

#To deploy on yarn cluster with local driver
#$SPARK_BIN/spark-submit \
#      --class com.samsung.vddil.recsys.Pipeline \
#     --master yarn \
#     --deploy-mode client \
#     --driver-memory 1G \
#     --queue vddil \
#     --executor-memory 4G --executor-cores 2 --num-executors 50 \
#      $SBTJAR $JOB_XML

#To deploy on yarn cluster
$SPARK_BIN/spark-submit \
      --class com.samsung.vddil.recsys.Pipeline \
     --master yarn-cluster \
     --executor-memory 4G --executor-cores 2 --num-executors 100  $SBTJAR $JOB_XML  
 
#To run locally
#$SPARK_BIN/spark-submit \
#      --class com.samsung.vddil.recsys.Pipeline \
#      --master local[8] \
#      $SBTJAR $JOB_XML

