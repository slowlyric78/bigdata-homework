#!/bin/bash

export BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Prepare directories
$HADOOP_HOME/bin/hdfs dfs -mkdir /data
$HADOOP_HOME/bin/hdfs dfs -mkdir /data/flights

# Load data
$HADOOP_HOME/bin/hdfs dfs -put $BASE/../../../data/*.csv /data/flights
$HIVE_HOME/bin/hive -f $BASE/../sql/create_tables.sql
$HIVE_HOME/bin/hive -f $BASE/../sql/load_data.sql