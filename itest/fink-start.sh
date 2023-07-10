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
export FINKCONFIG="$DIR"
finkctl --endpoint=localhost:9000 s3 makebucket

echo "Create spark ServiceAccount"
# see https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac
kubectl create serviceaccount spark --dry-run=client -o yaml | kubectl apply -f -

NS=$(kubectl get sa -o=jsonpath='{.items[0]..metadata.namespace}')
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=$NS:spark \
  --namespace=default --dry-run=client -o yaml | kubectl apply -f -

if [ "$NOSCIENCE" = true ];
then
  NOSCIENCE_OPT="--noscience"
else
  NOSCIENCE_OPT=""
fi

if [ "$MINIMAL" = true ];
then
  MINIMAL_OPT="--minimal"
else
  MINIMAL_OPT=""
fi

tasks="stream2raw raw2science distribution"
for task in $tasks; do
  finkctl run $MINIMAL_OPT $NOSCIENCE_OPT $task --image $IMAGE >& "/tmp/$task.log" &
done

# Wait for Spark pods to be created and warm up
# Debug in case of not expected behaviour
if ! finkctl wait tasks --timeout=180s
then
  for task in $tasks; do
    echo "--------- $task log file ---------"
    cat "/tmp/$task.log"
  done
  kubectl describe pods -l "spark-role in (executor, driver)"
  kubectl get pods
fi
