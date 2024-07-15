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

# Used only to set path to spark-submit.sh
. $DIR/../conf.sh

usage() {
  echo "Usage: $0 [-e] [-f finkconfig] [-i image]"
  echo "  Run end-to-end tests, fink configuration file and fink-broker image are automatically set"
  echo "  -h: display this help"
}

# Parse option for finkctl configuration file and fink-broker image
while getopts "h" opt; do
  case $opt in
    h) usage ; exit 0 ;;
  esac
done

NS=spark

# Prepare e2e tests
. $CIUXCONFIG
IMAGE="$CIUX_IMAGE_URL"
echo "Use CIUX_IMAGE_URL to set fink-broker image: $CIUX_IMAGE_URL"
if [[ "$IMAGE" =~ "-noscience" ]];
then
  FINKCONFIG="$DIR/finkconfig_noscience"
else
  FINKCONFIG="$DIR/finkconfig"
fi

kubectl config set-context --current --namespace="$NS"

echo "Create secrets"
while ! kubectl get secret fink-producer --namespace kafka
do
  echo "Waiting for secret/fink-producer in ns kafka"
  sleep 10
done
finkctl createsecrets

# Wait for Spark pods to be created and warm up
# Debug in case of not expected behaviour
# Science setup is VERY slow to start, because of raw2science-exec pod
# TODO use helm --wait with a pod which monitor the status of the Spark pods?

# 5 minutes timeout
timeout="300"

counter=0
max_retries=3
# Sometimes spark pods crashes and finktctl wait may fail
# even if Spark pod will be running after a while
# TODO implement the retry in "finkctl wait"
while ! finkctl wait tasks --timeout="${timeout}s"; do
  if [ $counter -gt $max_retries ]; then
    echo "ERROR: unable to start fink-broker in $timeout"
    echo "ERROR: enabling interactive access for debugging purpose"
    sleep 7200
    exit 1
  fi
  echo "Spark applications"
  echo "---------------"
  kubectl get sparkapplications
  kubectl logs -n spark-operator -l app.kubernetes.io/instance=spark-operator
  echo "Pods description"
  echo "----------------"
  kubectl describe pods -l "spark-role in (executor, driver)"
  kubectl get pods
  echo "ERROR: Spark pods are not running after $timeout, retry $counter/$max_retries"
  sleep 60
  timeout=$((timeout+300))
  counter=$((counter+1))
done

kubectl describe pods -l "spark-role in (executor, driver)"
kubectl get pods
echo "SUCCESS: fink-broker is running"
