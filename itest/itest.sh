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
finkctl -config $DIR/finkctl.yaml --secret $DIR/finkctl.secret.yaml spark --minimal stream2raw --image $IMAGE > /tmp/spark-stream2raw.log &

finkctl --config $DIR/finkctl.yaml --secret $DIR/finkctl.secret.yaml spark --minimal raw2science --image $IMAGE &

finkctl --config $DIR/finkctl.yaml --secret $DIR/finkctl.secret.yaml spark --minimal distribution --image $IMAGE &

COUNTER=0
while [ $(kubectl get pod -l spark-role --field-selector=status.phase==Running -o go-template='{{printf "%d\n" (len  .items)}}') -ne 2 \
  -o $COUNTER -lt 20 ]
do
  echo "Wait for Spark pods to be created"
  echo "---------------------------------"
  sleep 2
  echo "spark-submit logs (30 lines):"
  echo "-----------------------------"
  tail -n 30 "$SPARK_LOG_FILE"
  let COUNTER=COUNTER+1
  echo "Pods:"
  echo "-----"
  kubectl get pods
  kubectl describe pods -l "spark-role in (executor, driver)"
done

kubectl describe pods -l "spark-role in (executor, driver)"

# TODO a cli option
# kubectl delete pod -l "spark-role in (executor, driver)"


