#!/bin/sh
hadoop fs -rm -r ./page_rank/streaming/*
hadoop fs -cp ./page_rank/prepare ./page_rank/streaming/step_0


for i in {1..30}
do
    echo Step $i
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
        -numReduceTasks 3 \
	-file mapper.py reducer.py \
        -mapper mapper.py \
        -reducer reducer.py \
        -input ./page_rank/streaming/step_$(($i - 1)) \
        -output ./page_rank/streaming/step_$i
done
