#!/bin/bash
if [ $# -lt 3 ]; then
	echo "Usage: ./submit_emr.sh <job_file> <node_num> <num_executors> [compile]"
	echo "Example: ./submit_emr.sh ~/Desktop/emr/emr_jobs/aws_test_job_s3_st.xml 50 200 [compile]"
	echo "        This runs the job on 50 nodes with 200 executors"
	exit -1
fi

## 
# Die if anything happens
set -e
# Show details
set -x

cluster_name="recommender-system-pipeline"
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
emr_dir=$HOME/emr

#compile
if [ $# -ge 4 ]; then
    sbt clean package
fi

#upload program
echo "uploading program to S3"
sbt_jar="./target/scala-2.10/samsung-vd-recommender-system_2.10-1.0.jar"
if [ -f $sbt_jar ]; then 
	s3cmd put $sbt_jar s3://vddil.recsys/testScript/samsung-vd-recommender-system_2.10-1.0.jar	
else
	echo "Cannot find sbt build file: $sbt_jar, abort. "
	exit -2
fi

#upload jobfile
echo "uploading job file to S3"
if [ -f $job_file ]; then 
	s3cmd put $job_file s3://vddil.recsys/testScript/$running_job_file
else
	echo "Cannot find file: $job_file, abort"
	exit -3
fi

##start cluster
cluster_result=$($emr_dir/elastic-mapreduce --create --name $cluster_name --ami-version 3.2.1 \
        --instance-group master --instance-count 1 --instance-type c3.4xlarge \
        --instance-group core --instance-count $cluster_node_num --instance-type c3.8xlarge \
		--bootstrap-action s3://support.elasticmapreduce/bootstrap-actions/ami/3.2.1/CheckandFixMisconfiguredMounts.bash \
		--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --bootstrap-action s3://support.elasticmapreduce/spark/install-spark --args "-g"\
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

echo "*To SSH to the master:"
echo "  $emr_dir/elastic-mapreduce -j $cluster_id --ssh"
echo " "
echo "*SSH Tunnel for Web Interface"
echo "  $emr_dir/elastic-mapreduce -j $cluster_id --sock"
echo " "
echo "*Hadoop Web Interface (check )"
echo "  http://master_dns:9026/cluster/app/<application_id>"
echo "*Spark Web Interface for Stages"
echo "  http://master_dns:9046/proxy/<application_id>/"
echo "*Spark Web Interface for Stages"
echo "  http://master_dns:18080/"
echo " "
echo "*Remeber to TERMINATE the alive cluster when it is done."
echo "  $emr_dir/elastic-mapreduce --terminate $cluster_id"

##submit jobs 

# STEP: copy data from S3 to local HDFS. 
#./elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
#         --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"
#
#./elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
#         --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

# STEP: run the main program 
#$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
        #         --args "s3://vddil.recsys/testScript/aws_test_t3.sh,$memory_driver,$memory_exeutor,$num_cores,$num_executors"

$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 \
	--jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
	--args "s3://vddil.recsys/testScript/aws_run_job.sh,$running_job_file,$memory_driver,$memory_exeutor,$num_cores,$num_executors"

# STEP: copy results back. 
#$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
        #         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

echo "Logging to the master. Please get application id using *yarn application -list* and disconnect to continue. "
$emr_dir/elastic-mapreduce -j $cluster_id --ssh

#display information 
# BUG: these information are gone somehow. 
master_dns=$($emr_dir/elastic-mapreduce --describe $cluster_id | grep MasterPublicDnsName | cut -d"\"" -f4)
echo "Master DNS: $master_dns"

echo "*To SSH to the master:"
echo "  $emr_dir/elastic-mapreduce -j $cluster_id --ssh"
echo " "
echo "*SSH Tunnel for Web Interface"
echo "  $emr_dir/elastic-mapreduce -j $cluster_id --sock"
echo " "
echo "*Hadoop Web Interface (check )"
echo "  http://$master_dns:9026/cluster/app/<application_id>"
echo "*Spark Web Interface for Stages"
echo "  http://$master_dns:9046/proxy/<application_id>/"
echo "*Spark Web Interface for Stages"
echo "  http://$master_dns:18080/"
echo " "
echo "*Remeber to TERMINATE the alive cluster when it is done."
echo "  $emr_dir/elastic-mapreduce --terminate $cluster_id"

#$emr_dir/elastic-mapreduce -j $cluster_id --sock

exit 0
