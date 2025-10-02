# HDFS on Kubernetes - Troubleshooting Guide

## Quick Setup

```bash
# Deploy HDFS client
kubectl run -n hdfs --image apache/hadoop:3.4.0 hdfs-client --env="HADOOP_USER_NAME=stackable" -- sleep infinity
kubectl exec -n hdfs -it hdfs-client -- bash

# Create alias
alias hdfs-cmd='hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020'
```

## Essential Commands

```bash
# Check cluster status
hdfs-cmd -df

# List files
hdfs-cmd -ls /
hdfs-cmd -ls -R /user

# File operations
hdfs-cmd -mkdir -p /user/myuser
hdfs-cmd -put local.txt /user/myuser/
hdfs-cmd -get /user/myuser/file.txt ./
hdfs-cmd -cat /path/to/file

# Permissions
hdfs-cmd -chmod 755 /path
hdfs-cmd -chown user:group /path
```

## Working Example

```bash
# Full example with Parquet data
export HADOOP_USER_NAME=stackable
hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020 -df
hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020 -ls /user/185/raw/20200101/
```
