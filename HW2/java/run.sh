#!/bin/bash
hadoop fs -rm -r page_rank/java/*
hadoop fs -cp page_rank/prepare page_rank/java/step_0

hadoop jar PageRank.jar org.myorg.PageRank page_rank/java/step_0 page_rank/java

