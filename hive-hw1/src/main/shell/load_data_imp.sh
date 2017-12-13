#!/bin/bash

export BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create input foldre
$HADOOP_HOME/bin/hdfs dfs -mkdir /data
$HADOOP_HOME/bin/hdfs dfs -mkdir /data/impression

# Load data
for f in $BASE/../../../data/imp.*.txt; do $HADOOP_HOME/bin/hdfs dfs -put $f /data/impression; done
$HADOOP_HOME/bin/hdfs dfs -put $BASE/../../../data/city.en.txt /data/impression
$HIVE_HOME/bin/beeline -f $BASE/../sql/create_tables_imp.sql
$HIVE_HOME/bin/beeline -f $BASE/../sql/load_data_imp.sql