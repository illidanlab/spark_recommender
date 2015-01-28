#!/bin/bash

set -x

#rm -r /home/hadoop/aws_test

#hadoop fs -rm /aws_test_job.xml

mkdir aws_test
job_file=$1
echo $job_file
jar_file=$6
hadoop fs -copyToLocal s3://vddil.recsys/testScript/$job_file /home/hadoop/aws_test/$job_file
hadoop fs -copyToLocal s3://vddil.recsys/testScript/$jar_file /home/hadoop/aws_test/$jar_file
hadoop fs -copyToLocal s3://vddil.recsys/testScript/yarnSubmit_multiple.sh /home/hadoop/aws_test/yarnSubmit_multiple.sh


hadoop fs -put /home/hadoop/aws_test/$1 /aws_test_job.xml

bash /home/hadoop/aws_test/yarnSubmit_multiple.sh $2 $3 $4 $5 $6
