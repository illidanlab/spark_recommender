#!/bin/bash
#
# The recsys-spark job submission script 
# 
# 1) Submit without compile
#    ./submit.sh <local job filename>
# 2) Submit with compile 
#    ./submit.sh <local job filename> *
#
# Things to configure before running script 
# 1) Set spark executable: $SPARK_BIN. 
# 2) Set resource: $SPARK_EXE_MEMORY, $SPARK_EXEC_CORES and $SPARK_EXEC_NUM


SPARK_BIN=$HOME/installSrc/spark/spark/bin/
SPARK_EXEC_MEMORY="8G"
SPARK_EXEC_CORES=2
SPARK_EXEC_NUM=20
SPARK_DRIVER_MEMORY="4G"

if [ $# -lt 1 ]; then
    echo "Job file has not specified. Abort."
    exit -1
else
    echo "Job file specified: $1 "
fi

if [ ! -f $1 ]; then
    echo "Job file not found. Abort."
    exit -1
fi  


# this is the folder for the job file. 
server_job_workspace="hdfs://gnosis-01-01-01.crl.samsung.com:8020/apps/vddil/recsys/jobfiles"

MVNJAR=$PWD/target/recsys-spark-0.0.1.jar
SBTJAR=$PWD/target/scala-2.10/samsung-vd-recommender-system_2.10-1.0.jar
#SBTJAR=$PWD/target/scala-2.10/samsung-recsys-assembly.jar

if [ $# -ge 2 ]; then
    echo "Compiling"
    sbt package
    #sbt assembly
fi


local_job_file=$1
file_name=$USER"_"$(basename $local_job_file)
server_job_file="$server_job_workspace/$file_name"

echo "Uploading job file $server_job_file ..."

echo "Checking if the server file exists"
if hdfs dfs -test -e $server_job_file; then
    echo "Server file found, removing..."
    hadoop fs -rm -skipTrash $server_job_file
else
    echo "Server file not found. Proceed."
fi

hadoop fs -copyFromLocal $local_job_file $server_job_file

echo "Server file copied."

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=$CLASSPATH

#To deploy on yarn cluster
cmd="$SPARK_BIN/spark-submit \
      --class com.samsung.vddil.recsys.Pipeline \
     --master yarn-cluster \
     --executor-memory $SPARK_EXEC_MEMORY \
     --executor-cores  $SPARK_EXEC_CORES \
     --driver-memory   $SPARK_DRIVER_MEMORY \
     --num-executors   $SPARK_EXEC_NUM \
     $SBTJAR $server_job_file --queue vddil"  
 
echo $cmd
$cmd
