#!/bin/bash
DATANODE_DIR="/opt/hadoop/data/dataNode"
# echo "Cleaning DataNode directory..."
# rm -rf "$DATANODE_DIR"/*
chown -R hadoop:hadoop "$DATANODE_DIR"
chmod 755 "$DATANODE_DIR"
echo "Starting HDFS DataNode..."
hdfs datanode
