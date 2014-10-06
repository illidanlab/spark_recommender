#!/usr/bin/env bash


####
# generate filtered schdule for test purpose. 
####

pid_list_file="../../data/itemList.txt"
full_data_folder="$HOME/workspace/data/rovi_hq"
filter_data_folder="../../data/ROVI"

dates="20140522;20140523;20140524;20140525"

pid_length=$(cat $pid_list_file | wc -l)
echo "pid_list size:"$pid_length

for date in $(echo $dates | tr ";" "\n" )
do
	echo "generate schedule for" $date
	source_file="$full_data_folder"/"$date"/"schedule.txt.gz"
	target_file="$filter_data_folder"/"$date"/"schedule.txt.gz"
	echo "Source file: "$source_file
	echo "Target file: "$target_file
       
	if [ -f $source_file ]; then

		if [ -f $target_file ]; then
			rm $target_file
		fi

		cat $source_file | gzip -d | ./filter_schedule.py $pid_list_file | gzip > $target_file
		echo "verification"
		cat $target_file | gzip -d | cut -d"|" -f5 | sort | uniq | wc -l
	else
		echo "Cannot find source file"
	fi	
done
