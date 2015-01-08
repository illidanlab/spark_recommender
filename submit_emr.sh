#!/bin/bash
if [ $# -lt 3 ]; then
	echo "Usage: ./submit_emr.sh <job_file> <node_num> <num_executors> [compile]"
	echo "Example: ./submit_emr.sh ~/Desktop/emr/emr_jobs/aws_test_job_s3_st.xml 200 100 [compile]"
	echo "        This runs the job on 50 nodes with 200 executors"
	exit -1
fi

## 
# Die if anything happens
#set -e
# Show details
#set -x

cluster_name="recommender-system-pipeline-output"
cluster_node_num=$2

job_file=$1
memory_driver="10G"
memory_exeutor="20G"
num_cores="6"
num_executors=$3

spark_default_parallelism=1600
#running_job_file="running_job.xml"
running_job_file=$USER"_"$(basename $job_file)

#set up directory
emr_dir=$HOME/real-time/elastic-mapreduce-cli/

#compile
if [ $# -ge 4 ]; then
    sbt clean package
fi

#upload program
echo "uploading program to S3"
sbt_jar="./target/scala-2.10/samsung-vd-recommender-system_2.10-1.0.jar"
if [ -f $sbt_jar ]; then 
	s3cmd put $sbt_jar s3://vddil.recsys.east/aws_test/samsung-vd-recommender-system_2.10-1.0.jar	
else
	echo "Cannot find sbt build file: $sbt_jar, abort. "
	exit -2
fi

##start cluster
cluster_result=$($emr_dir/elastic-mapreduce --create --name $cluster_name --ami-version 3.3.1 \
        --instance-group master --instance-count 1 --instance-type c3.4xlarge \
        --instance-group core --instance-count $cluster_node_num --instance-type m3.2xlarge \
		--bootstrap-action s3://support.elasticmapreduce/bootstrap-actions/ami/3.2.1/CheckandFixMisconfiguredMounts.bash \
		--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --bootstrap-action s3://support.elasticmapreduce/spark/install-spark --args "-g,-v,1.1.1.e"\
		--bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar \
            --args "s3://support.elasticmapreduce/spark/start-history-server" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar \
            --args "s3://support.elasticmapreduce/spark/configure-spark.bash,\
				spark.default.parallelism=$spark_default_parallelism,\
				spark.storage.memoryFraction=0.4,\
				spark.locality.wait.rack=0" \
        --alive --region us-east-1 --key-pair DMC_DEV)

#get cluster master DNS
cluster_id=$(echo $cluster_result | cut -d" " -f4)
echo "Cluster ID: $cluster_id"

$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 \
	--jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
	--args "s3://vddil.recsys/testScript/aws_run_job.sh,$running_job_file,$memory_driver,$memory_exeutor,$num_cores,$num_executors"

# STEP: copy results back. 
#$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
        #         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

echo "Logging to the master. Please get application id using *yarn application -list* and disconnect to continue. "
$emr_dir/elastic-mapreduce -j $cluster_id --ssh
