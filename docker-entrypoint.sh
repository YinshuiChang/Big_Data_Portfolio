#!/usr/bin/env sh
sudo service ssh start
$HADOOP_HOME/sbin/start-all.sh

jupyter lab --ip 0.0.0.0 --no-browser --allow-root

# keep the container running indefinitely
# tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log