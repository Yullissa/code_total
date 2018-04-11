#! /bin/bash

#echo $1
INPUT=/user/azkaban/camus/indata_str_vertical_documents/hourly/2018-{01,02,03}-*/*
OUTPUT=/user/xinfeixiang/oppobrowser/oppo_clicks/$1/

echo $INPUT
echo $OUTPUT
hadoop fs -rm -r -skipTrash $OUTPUT
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
        -input $INPUT \
        -output $OUTPUT \
        -mapper ./step1_click_extractor.py \
        -file step1_click_extractor.py  \
        -jobconf mapred.job.name="step1_click_extractor" \
        -jobconf mapred.reduce.tasks=5