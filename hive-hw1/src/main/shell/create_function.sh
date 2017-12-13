#!/bin/bash

export BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$HADOOP_HOME/bin/hdfs dfs -mkdir /udf
$HADOOP_HOME/bin/hdfs dfs -rm -f /udf/*
$HADOOP_HOME/bin/hdfs dfs -put $BASE/../../../target/*.jar /udf
$HADOOP_HOME/bin/hdfs dfs -put $BASE/../../../lib/*.jar /udf

$HIVE_HOME/bin/beeline -f $BASE/../sql/create_function.sql