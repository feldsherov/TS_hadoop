#!/bin/sh
hadoop fs -rm -r ./aggregate

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file value_histogram_mapper.py \
    -mapper 'value_histogram_mapper.py 1 4' \
    -reducer aggregate \
    -input /data/patents/apat63_99.txt \
    -output ./aggregate
