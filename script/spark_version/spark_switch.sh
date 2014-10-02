#!/usr/bin/env bash 

echo "Spark Version Switcher"
if [ $# -ne 1 ]; then 
    echo "Useage: ./switch.sh <spark version>"
    echo "        ./switch.sh snapshot"
    exit -1
fi

if [ $1 == 'snapshot' ]; then
    spark_folder="$PWD/spark-snapshot"
    #spark_folder="/home/m3.sharma/installSrc/spark/spark"
    echo "Spark folder: $spark_folder"

    echo "**Please build Snapshot using following command"
    echo "./spark/sbt/sbt clean"
    echo "./spark/sbt/sbt -Dhadoop.version=2.4.0 -Pyarn assembly"
    echo "./spark/sbt/sbt clean & ./spark/sbt/sbt -Dhadoop.version=2.4.0 -Pyarn assembly"
else
    spark_folder="$PWD/spark-$1-bin-hadoop2"
fi

echo "Spark folder: $spark_folder"

if [  -d $spark_folder  ]; then
    echo "Spark folder found. "
else
    echo "Spark folder not found. "
    exit -1
fi

spark_symbol_folder="$PWD/spark"

if [ -h $spark_symbol_folder ]; then
   echo "Previous symbolic link detected and removed. "
   rm $spark_symbol_folder
fi

ln -s $spark_folder $spark_symbol_folder

echo "Symbolic link created at: $spark_symbol_folder"
