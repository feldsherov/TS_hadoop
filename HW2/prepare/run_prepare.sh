#!/bin/sh
hadoop fs -rm -r ./page_rank/prepare

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file mapper.py reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/patents/cite75_99.txt \
    -output ./page_rank/prepare
