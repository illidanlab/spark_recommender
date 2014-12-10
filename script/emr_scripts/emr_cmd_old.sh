./elastic-mapreduce --create --name spark102c3-4x-oneday-test --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.2xlarge \
        --instance-group core --instance-count 2 --instance-type c3.4xlarge \
        --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py \
        	--args "BA" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" \
        --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV


./elastic-mapreduce --create --name spark102c3-4x-6-6-2-200 --ami-version 3.1.1 --instance-group master --bid-price 1.00 --instance-count 1 --instance-type c3.2xlarge --instance-group core --bid-price 1.00 --instance-count 10 --instance-type c3.4xlarge --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py --args "BA" --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" --pig-interactive --alive --region us-east-1 


./elastic-mapreduce -j j-2XM4BWJLY51PJ --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://dmgsample/rovi,--dest,/rovi"

./elastic-mapreduce -j j-2XM4BWJLY51PJ --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://dmgsample/duid-program-watchTime,--dest,/duid-program-watchTime"

./elastic-mapreduce -j j-2XM4BWJLY51PJ --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
--args "s3://lab-emr/temp/aws_test_t3.bash,7G,7G,2,100"


hadoop jar /home/hadoop/lib/emr-s3distcp-1.0.jar --src s3://vd.infra.dev.dms.spark/dms_raw_data/rovi --dest /rovi 
hadoop jar /home/hadoop/lib/emr-s3distcp-1.0.jar --src s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime --dest /duid-program-watchTime

hadoop jar /home/hadoop/lib/emr-s3distcp-1.0.jar --src /workspace/exp_features_20140501_20140507/data/ContAggData_df445bd602f3c5c23a7056c117509e23_fece528296468e4431963fec6401afae_plainText_userItemMatrix --dest s3://vddil.recsys/dumpData/ContAggData_df445bd602f3c5c23a7056c117509e23_fece528296468e4431963fec6401afae_plainText_userItemMatrix

hadoop jar /home/hadoop/lib/emr-s3distcp-1.0.jar --src /workspace/exp_features_20140501_20140507/data/ContAggData_df445bd602f3c5c23a7056c117509e23_fece528296468e4431963fec6401afae_plainText_ItemFeatureMatrix --dest s3://vddil.recsys/dumpData/ContAggData_df445bd602f3c5c23a7056c117509e23_fece528296468e4431963fec6401afae_plainText_ItemFeatureMatrix



./elastic-mapreduce --create --name spark102c3-4x-ssh-test --ami-version 3.1.1 --instance-group master --instance-count 1 --instance-type c3.2xlarge --instance-group core --bid-price 1.00 --instance-count 10 --instance-type c3.4xlarge --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py --args "BA" --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV


./elastic-mapreduce -j j-26I8WK21TSUVM --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/rovi,--dest,/rovi"

./elastic-mapreduce -j j-26I8WK21TSUVM --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/duid-program-watchTime,--dest,/duid-program-watchTime"

./elastic-mapreduce -j j-26I8WK21TSUVM --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vd.infra.dev.dms.spark/aws_test_t3.sh,7G,7G,2,100"




echo $cid    ===>  Created job flow j-LSJKKC6NI5HB 

./elastic-mapreduce --create --name spark102c3-4x-ssh-test --ami-version 3.1.1 --instance-group master --instance-count 1 --instance-type c3.2xlarge --instance-group core --instance-count 10 --instance-type c3.4xlarge --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py --args "BA" --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV

./elastic-mapreduce -j j-1DKMVVI5ERE0D --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"

./elastic-mapreduce -j j-1DKMVVI5ERE0D --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

./elastic-mapreduce -j j-1DKMVVI5ERE0D --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vd.infra.dev.dms.spark/dms_raw_data/aws_test_t3.sh,7G,7G,2,100"


./elastic-mapreduce --create --name spark102c3-4x-oneday-test --ami-version 3.1.1 --instance-group master --instance-count 1 --instance-type c3.2xlarge --instance-group core --instance-count 20 --instance-type c3.4xlarge --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py --args "BA" --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV

./elastic-mapreduce -j j-36HNI3A29ENE9 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_test_t3.sh,7G,7G,2,100"






>>>>>>>>>>>>>> S3 test

//the following works. 
sc.textFile("hdfs:///rovi/20140801/program_desc.txt.gz").first
sc.textFile("s3n://vd.infra.dev.dms.spark/dms_raw_data/rovi/20140801/program_desc.txt.gz").first
sc.textFile("hdfs:///duid-program-watchTime/20140802/*").first

<<<<<<<<<<<<<< S3 test






>>>>>>>>>>>>> FINAL Aug

~/emr/elastic-mapreduce --create --name recommender_system_pipeline --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.2xlarge \
        --instance-group core --instance-count 50 --instance-type c3.4xlarge \
        --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py \
        	--args "BA" \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" \
        --pig-interactive --alive --region us-east-1 --key-pair DMC_DEV

~/emr/elastic-mapreduce -j j-16H44FSTYWA0 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"

~/emr/elastic-mapreduce -j j-16H44FSTYWA0 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

~/emr/elastic-mapreduce -j j-PN9RP114SDV0 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_test_t3.sh,8G,8G,4,100"

./elastic-mapreduce -j j-1TF23X143SJMS --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

<<<<<<<<<<<<< FINAL Aug


>>>>>>>>>>>>> FINAL May

~/emr/elastic-mapreduce --create --name recommender_system_pipeline --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.4xlarge \
        --instance-group core --instance-count 50 --instance-type c3.8xlarge \
        --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py \
            --args "BA" \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
            --args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" \
        --alive --region us-east-1 --key-pair DMC_DEV

~/emr/elastic-mapreduce -j j-2Z5KPBF939G4X --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi_May,--dest,/rovi"

~/emr/elastic-mapreduce -j j-2Z5KPBF939G4X --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime_May,--dest,/duid-program-watchTime"

~/emr/elastic-mapreduce -j j-2Z5KPBF939G4X --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_test_t3.sh,10G,10G,4,100"

./elastic-mapreduce -j j-1TF23X143SJMS --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

<<<<<<<<<<<<< FINAL May

aws emr ssh --cluster-id j-BO90EG96PWZL --key-pair-file ~/DMC_DEV.pem
ssh hadoop@ec2-54-165-111-126.compute-1.amazonaws.com -i ~/DMC_DEV.pem

aws emr socks --cluster-id j-1TF23X143SJMS --key-pair-file ~/DMC_DEV.pem 

#spark shell.
ssh -i ~/DMC_DEV.pem -N -L 8157:ec2-54-165-120-141.compute-1.amazonaws.com:4040 hadoop@ec2-54-165-120-141.compute-1.amazonaws.com

#job
ssh -i ~/DMC_DEV.pem -N -L 8157:ec2-54-165-120-141.compute-1.amazonaws.com:9046 hadoop@ec2-54-165-120-141.compute-1.amazonaws.com 

yarn application -list -appStates FINISHED 

# yarn resource manager UI
http://ec2-54-165-206-164.compute-1.amazonaws.com:9026/cluster/app/application_1411081705416_0005
# SPARK progress
http://ec2-54-172-48-127.compute-1.amazonaws.com:9046/proxy/application_1412378738340_0004/

SSH to get the master, use 
yarn application -list
to get the application id, 





