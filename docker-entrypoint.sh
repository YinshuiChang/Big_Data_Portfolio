#!/usr/bin/env sh
sudo service ssh start
$HADOOP_HOME/sbin/start-all.sh

# keep the container running indefinitely
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log