#!/bin/bash
NAMENODE_DIR="/opt/hadoop/data/nameNode"
if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
else
    echo "NameNode already formatted. Skipping format step."
fi
echo "Starting HDFS NameNode..."
hdfs namenode
