#!/bin/bash
#
# Ingestion folder init script
#

. my.conf

sudo $MY_HDP/sbin/start-dfs.sh

$MY_HDP/bin/hdfs dfs -mkdir /impression
$MY_HDP/bin/hdfs dfs -mkdir /impression/input
$MY_HDP/bin/hdfs dfs -mkdir /impression/output
$MY_HDP/bin/hdfs dfs -put ../data/* /impression/input

# sudo $MY_HDP/sbin/stop-dfs.sh