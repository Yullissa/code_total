#!/bin/bash
basedir1="/user/azkaban/camus/indata_str_vertical_documents/hourly/201{7-06,7-07,7-08,7-09,7-10,7-11,7-12,8-01,8-02}-*/*"
basedir2="/user/azkaban/camus/indata_str_documents_info/hourly/201{7-06,7-07,7-08,7-09,7-10,7-11,7-12,8-01,8-02}-*/*"

function getdir(){
	for element in `hadoop fs -ls $1`
	do
		if [ ${element##*.} == "gz" ]
		then
			hadoop fs -text ${element} | python postdata.py
			#echo "hello"
			#echo ${element}
			#`hadoop fs -text ${element} > text.txt`
			#echo "mid"
			#`python postdata.py`
			#echo "end"
			#`rm text.txt`
		fi
	done
}

getdir $basedir1
getdir $basedir2

