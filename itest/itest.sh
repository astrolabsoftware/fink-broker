#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Launch integration tests for fink-broker

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

. $DIR/../conf.sh

echo $IMAGE

echo "Create S3 bucket"
kubectl port-forward -n minio-dev svc/minio 9000 &
finkctl --config $DIR/finkctl.yaml --secret $DIR/finkctl.secret.yaml --endpoint=localhost:9000 s3 makebucket

echo "Create spark ServiceAccount"
# see https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac
kubectl create serviceaccount spark --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark \
  --namespace=default --dry-run=client -o yaml | kubectl apply -f -

readonly SPARK_LOG_FILE="/tmp/spark-stream2raw.log"

tasks="stream2raw raw2science distribution"

# Iterate the string variable using for loop
for task in $tasks; do
  finkctl --config $DIR/finkctl.yaml --secret $DIR/finkctl.secret.yaml spark \
    --minimal $task --image $IMAGE >& "/tmp/$task.log" &
done

loop=true
counter=0
expected_pods=6
while $loop
do
  pod_count=$(kubectl get pod -l spark-role=driver --field-selector=status.phase==Running \
    -o go-template='{{printf "%d\n" (len  .items)}}')

  if [ $pod_count -eq $expected_pods ]; then
    loop=false
  elif [ $counter -gt 5 ]; then
    loop=false
  fi
  echo "Wait for Spark pods to be created"
  echo "---------------------------------"
  sleep 2
  let counter=counter+1
  echo "Pods:"
  echo "-----"
  kubectl get pods
done

# Debug in case of not expected behaviour
if [ "$pod_count" -ne $expected_pods ]
then
  for task in $tasks; do
    echo "--------- $task log file ---------"
    cat "/tmp/$task.log"
  done
  kubectl describe pods -l "spark-role in (executor, driver)"
  kubectl get pods
fi

# TODO add a cli option
# kubectl delete pod -l "spark-role in (executor, driver)"


