#!/bin/sh
hadoop fs -rm -r ./my_aggregate/streaming

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file value_histogram_mapper.py value_histogram_reducer.py \
    -mapper 'value_histogram_mapper.py 1 4' \
    -reducer value_histogram_reducer.py \
    -input /data/patents/apat63_99.txt \
    -output ./my_aggregate/streaming
