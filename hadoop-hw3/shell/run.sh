#!/bin/bash
#
# Job running script
#

. my.conf

$MY_HDP/bin/hdfs dfs -rm /impression/output/*
$MY_HDP/bin/hdfs dfs -rmdir /impression/output
$MY_HDP/bin/yarn jar ../target/hadoopBasicHomework3-1.0-SNAPSHOT.jar com.epam.bd.ImpressionDriver /impression/input /impression/output
