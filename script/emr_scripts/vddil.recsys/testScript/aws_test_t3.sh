#!/bin/bash

set -x

rm -r /home/hadoop/aws_test

hadoop fs -rm /aws_test_job.xml

hadoop fs -get s3://vddil.recsys/testScript/ /home/hadoop/aws_test

hadoop fs -put /home/hadoop/aws_test/aws_test_job.xml /aws_test_job.xml

bash /home/hadoop/aws_test/yarnSubmit.sh $1 $2 $3 $4
