############### FINAL Aug 1.1.0

~/emr/elastic-mapreduce --create --name recommender_system_pipeline_spark110 --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.4xlarge \
        --instance-group core --instance-count 100 --instance-type c3.4xlarge \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --bootstrap-action s3://support.elasticmapreduce/spark/install-spark --args "-v,1.1.0" \
        --alive --region us-east-1 --key-pair DMC_DEV

~/emr/elastic-mapreduce -j j-929IQ3XSEFG0 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"

~/emr/elastic-mapreduce -j j-929IQ3XSEFG0 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

~/emr/elastic-mapreduce -j j-929IQ3XSEFG0 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_run_job.sh,aws_test_job.xml.Aug,12G,12G,6,100"

~/emr/elastic-mapreduce -j j-929IQ3XSEFG0 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_run_job.sh,aws_test_job_2week.xml,12G,12G,6,100"

./elastic-mapreduce -j j-1TF23X143SJMS --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

http://ec2-54-172-48-127.compute-1.amazonaws.com:9046/proxy/application_1412378738340_0004/

############### FINAL Aug 1.1.0

############### FINAL Aug S3

~/emr/elastic-mapreduce --create --name recommender_system_pipeline_spark110S3expdata --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.4xlarge \
        --instance-group core --instance-count 100 --instance-type c3.4xlarge \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --bootstrap-action s3://support.elasticmapreduce/spark/install-spark \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar \
            --args "s3://support.elasticmapreduce/spark/start-history-server" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar \
            --args "s3://support.elasticmapreduce/spark/configure-spark.bash,spark.default.parallelism=1600,spark.locality.wait.rack=0" \
        --alive --region us-east-1 --key-pair DMC_DEV

~/emr/elastic-mapreduce -j j-3MEYVSLLSM2E1 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_run_job.sh,running_job.xml,12G,12G,6,100"

~/emr/elastic-mapreduce -j j-2YO97JENMP487 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

http://ec2-54-172-120-28.compute-1.amazonaws.com:9046/proxy/application_1413581301760_0003/
http://ec2-54-172-234-195.compute-1.amazonaws.com:18080/
http://ec2-54-172-234-195.compute-1.amazonaws.com:80/ganglia

############### FINAL Aug S3






############### FINAL Aug 1.0.1

~/emr/elastic-mapreduce --create --name recommender_system_pipeline --ami-version 3.1.1 \
        --instance-group master --instance-count 1 --instance-type c3.8xlarge \
        --instance-group core --instance-count 100 --instance-type c3.8xlarge \
        --bootstrap-action s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py \
        	--args "BA" \
        --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
        	--args "-y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=/tmp/logs" \
        --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://fbaldo-analytics/steps/spark/1.0.2/spark_1.0.2.py,STEP" \
        --alive --region us-east-1 --key-pair DMC_DEV

~/emr/elastic-mapreduce -j j-2V17LISV041X8 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/rovi,--dest,/rovi"

~/emr/elastic-mapreduce -j j-2V17LISV041X8 --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args "--src,s3://vd.infra.dev.dms.spark/dms_raw_data/duid-program-watchTime,--dest,/duid-program-watchTime"

~/emr/elastic-mapreduce -j j-2V17LISV041X8 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_run_job.sh,aws_test_job.xml.Aug,12G,12G,6,200"

~/emr/elastic-mapreduce -j j-2V17LISV041X8 --region us-east-1 --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar --args "s3://vddil.recsys/testScript/aws_run_job.sh,aws_test_job_2week.xml,12G,12G,6,200"

./elastic-mapreduce -j j-1TF23X143SJMS --region us-east-1 --jar /home/hadoop/lib/emr-s3distcp-1.0.jar \
         --args "--src,/workspace,--dest,s3://vddil.recsys/workspace"

############### FINAL Aug 1.0.1






scp -i /Users/jiayu.zhou/DMC_DEV.pem hadoop@ec2-54-172-120-28.compute-1.amazonaws.com:/home/hadoop/test.log.gz .

