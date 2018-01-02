#!/bin/bash

kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic mytopic
kafka-topics --zookeeper localhost:2181 --list

