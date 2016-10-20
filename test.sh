#!/bin/bash

mvn package

scp target/TP2-0.1.jar hadoop:TP2.jar
#ssh hadoop "hdfs dfs -rm -r out || true && hadoop jar TP2.jar CountNumberOfFirstNameByNumberOfOrigin /res/prenoms.csv out && rm -rf out || true && hdfs dfs -get out"
#ssh hadoop "hdfs dfs -rm -r out || true && hadoop jar TP2.jar CountFirstNameByOrigin /res/prenoms.csv out && rm -rf out || true && hdfs dfs -get out"
ssh hadoop "hdfs dfs -rm -r out || true && hadoop jar TP2.jar ProportionMaleFemale /res/prenoms.csv out && rm -rf out || true && hdfs dfs -get out"



rm -rf output
scp -r hadoop:out output


cat output/part-00000 |column -t
