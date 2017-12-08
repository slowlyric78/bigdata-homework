#!/bin/bash

# Create workspace
$HADOOP_HOME/bin/hadoop fs -mkdir /user
$HADOOP_HOME/bin/hadoop fs -mkdir /user/hive
$HADOOP_HOME/bin/hadoop fs -mkdir /user/hive/warehouse

# Init local metadata
$HIVE_HOME/bin/schematool -dbType derby -initSchema