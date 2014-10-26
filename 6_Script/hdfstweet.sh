#!/bin/bash

#Author: Piyush Routray
#Description: script to check for twitter data files in local folder, 
#			  copy it to the HDFS and Run pig script on the file.


#Check if any new files have been added
#Assumming twitter file would be .json

for file in ~/tweetlog/*json
do
	#name variable has timestamp and filename
	time="$(date +%Y%m%d_%H%M%S)"
	name="$(basename "$file" .json)"
	fname=$time$name

	#move to hdfs
	hdfs dfs -put "$file" "/DataFolder/Script/$fname"
        echo "File $fname was successfully moved to the HDFS." 
        #remove from local directory
	mv $file ~/TweetArchive
	
	#Run the pig script
	echo "Attempting PIG Script NOW!"
	pig -param filename=$fname -x mapreduce hdfsToMongodb.pig
done

