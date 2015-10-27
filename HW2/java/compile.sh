#!/bin/bash
rm -rf classes
rm -rf PageRank.jar
mkdir classes

javac -d classes/ PageRank.java
jar -cvf PageRank.jar -C classes/ ./
