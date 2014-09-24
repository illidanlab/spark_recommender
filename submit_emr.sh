#!/bin/bash
if [ $# -lt 3 ]; then
	echo "Usage: submit_emr.sh <job_file> <num_cores> <num_executors> [compile]"
	exit -1
fi

## 
# Die if anything happens
set -e
# Show details
set -x

cluster_name="recommender-system-pipeline"
job_file=$1
memory_driver="7G"
memory_exeutor="7G"
num_cores=$2
num_executors=$3

#set up directory
emr_dir=$HOME/emr/

#compile
if [ $# -ge 4 ]; then
    sbt clean assembly
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
	s3cmd put $job_file s3://vddil.recsys/testScript/aws_test_job.xml
else
	echo "Cannot find file: $job_file, abort"
	exit -3
fi

##start cluster
cluster_result=$($emr_dir/elastic-mapreduce --create --name $cluster_name --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.2xlarge \
        --instance-group core --instance-count 50 --instance-type c3.4xlarge \
        --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py \
        	--args "BA" \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" \
        --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV)

#get cluster master DNS
cluster_id=$(echo $cluster_result | cut -d" " -f4)
echo "Cluster ID: $cluster_id"


##submit jobs 

# STEP: copy data from S3 to local HDFS. 
#./elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
#         --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"
#
#./elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
#         --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

# STEP: run the main program 
$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
         --args "s3://vddil.recsys/testScript/aws_test_t3.sh,$memory_driver,$memory_exeutor,$num_cores,$num_executors"

# STEP: copy results back. 
$emr_dir/elastic-mapreduce -j $cluster_id --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

#display information
read -p "Wait until master DNS shown in web, and press [Enter] key to continue..."

master_dns=$($emr_dir/elastic-mapreduce --describe $cluster_id | grep MasterPublicDnsName | cut -d"\"" -f4)
echo "Master DNS: $master_dns"

echo "*To SSH to the master:"
echo "  aws emr ssh --cluster-id $cluster_id --key-pair-file ~/DMC_DEV.pem"
echo "  OR) ssh hadoop@$master_dns -i ~/DMC_DEV.pem"
echo " "
echo "*SSH Tunnel for Web Interface"
echo "  aws emr socks --cluster-id $cluster_id --key-pair-file ~/DMC_DEV.pem"
echo "  OR) ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=10 -ND 8157 -i ~/DMC_DEV.pem hadoop@$master_dns"
echo " "
echo "*Hadoop Web Interface (check )"
echo "  http://$master_dns:9026/cluster/app/<application_id>"
echo "*Spark Web Interface for Stages"
echo "  http://$master_dns:9046/proxy/<application_id>/"
echo " "
echo "*Remeber to TERMINATE the alive cluster when it is done."
echo "  $emr_dir/elastic-mapreduce --terminate $cluster_id"

exit 0