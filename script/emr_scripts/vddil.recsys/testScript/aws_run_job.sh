#!/bin/bash

set -x

rm -r /home/hadoop/aws_test

hadoop fs -rm /aws_test_job.xml

hadoop fs -get s3://vddil.recsys/testScript/ /home/hadoop/aws_test

hadoop fs -put /home/hadoop/aws_test/$1 /aws_test_job.xml

bash /home/hadoop/aws_test/yarnSubmit.sh $2 $3 $4 $5
