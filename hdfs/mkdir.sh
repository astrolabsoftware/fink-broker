#!/bin/bash

# This script creates a directory in HDFS and sets the owner to user 185

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

NS=hdfs

timeout=300s

# Wait for HDFS statefulset to be available
# TODO improve this
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=hdfs --timeout=$timeout -n $NS
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=zookeeper --timeout=$timeout -n $NS
sleep 60

hdfs_url="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.$NS:8020"

# Check if pod hdfs-client exists
if ! kubectl get -n "$NS" pod hdfs-client &> /dev/null; then
    kubectl run -n "$NS" --image apache/hadoop:3.4.0 hdfs-client -- sleep infinity
fi

kubectl wait -n "$NS" --for=condition=ready pod/hdfs-client --timeout=$timeout

kubectl exec -n "$NS" -it hdfs-client -- sh -c "export HADOOP_USER_NAME=stackable && \
    hdfs dfs -fs $hdfs_url -mkdir -p /user/185 && \
    hdfs dfs -fs $hdfs_url -chown 185:hdfs /user/185 && \
    hdfs dfs -fs $hdfs_url -chmod 700 /user/185"
