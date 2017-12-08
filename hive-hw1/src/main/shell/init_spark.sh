#!/bin/bash

export SPARK_HOME=/opt/spark-2.2.0-bin-hadoop2.7
export HIVE_HOME=/opt/apache-hive-2.3.2

# set hive.execution.engine=spark

ln -s $SPARK_HOME/jars/scala-library-2.11.8.jar $HIVE_HOME/lib/scala-library-2.11.8.jar
ln -s $SPARK_HOME/jars/spark-core_2.11-2.2.0.jar $HIVE_HOME/lib/spark-core_2.11-2.2.0.jar
ln -s $SPARK_HOME/jars/spark-network-common_2.11-2.2.0.jar $HIVE_HOME/lib/spark-network-common_2.11-2.2.0.jar