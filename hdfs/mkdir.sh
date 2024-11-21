#!/bin/bash

# This script creates a directory in HDFS and sets the owner to user 185

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

# Wait for HDFS statefulset to be available
# TODO improve this
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=hdfs --timeout=300s -n default
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=zookeeper --timeout=300s -n default
sleep 10

hdfs_url="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default:8020"

# Check if pod hdfs-client exists
if ! kubectl get -n default pod hdfs-client &> /dev/null; then
    kubectl run -n default --image apache/hadoop:3.4.0 hdfs-client -- sleep infinity
fi

kubectl wait -n default --for=condition=ready pod/hdfs-client

kubectl exec -it hdfs-client -- sh -c "export HADOOP_USER_NAME=stackable && \
    hdfs dfs -fs $hdfs_url -mkdir -p /user/185 && \
    hdfs dfs -fs $hdfs_url -chown 185:hdfs /user/185 && \
    hdfs dfs -fs $hdfs_url -chmod 700 /user/185"
