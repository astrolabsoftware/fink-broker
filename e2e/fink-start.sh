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
  echo "  -e: run end-to-end tests, fink configuration file and fink-broker image are automatically set and -f/-i options are ignored"
  echo "  -f: finkctl configuration file"
  echo "  -i: fink-broker image, overriden by CIUX_IMAGE_URL environment variable if -e option is set"
  echo "  -N: night to process, e.g. 20210101"
  echo "  -t: process night for the current date, e.g. $(date +%Y%m%d), overrides -N option"
  echo "  -h: display this help"
}

E2E_TEST=false
NOSCIENCE_OPT=""
IMAGE_OPT=""
NIGHT_OPT=""

# Parse option for finkctl configuration file and fink-broker image
while getopts "ef:i:N:th" opt; do
  case $opt in
    e) E2E_TEST=true;;
    f) FINKCONFIG=$OPTARG ;;
    h) usage ; exit 0 ;;
    N) NIGHT_OPT="-N $OPTARG" ;;
    t) NIGHT_OPT="-N $(date +%Y%m%d)" ;;
    i) IMAGE_OPT="--image $OPTARG" ;;
  esac
done

NS=spark
echo "Create $NS namespace"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace="$NS"

if [ $E2E_TEST = true ];
then
  . $CIUXCONFIG
  IMAGE="$CIUX_IMAGE_URL"
  echo "Use CIUX_IMAGE_URL to set fink-broker image: $CIUX_IMAGE_URL"
  if [[ "$IMAGE" =~ "-noscience" ]];
  then
    VALUE_FILE="$DIR/../chart/values-ci-noscience.yaml"
    FINKCONFIG="$DIR/finkconfig_noscience"
  else
    VALUE_FILE="$DIR/../chart/values-ci.yaml"
    FINKCONFIG="$DIR/finkconfig"
  fi
  kubectl port-forward -n minio svc/minio 9000 &
  # Wait to port-forward to start
  sleep 2
  echo "Create S3 bucket"
  # TODO find and alternate way to create bucket which remove use of FINKCONFIG
  # replaced by helm value file
  export FINKCONFIG
  finkctl --endpoint=localhost:9000 s3 makebucket
fi

# Start fink-broker
echo "Start fink-broker"
helm install --debug fink "$DIR/../chart" -f "$VALUE_FILE" \
  --set image.repository="$CIUX_IMAGE_REGISTRY" \
  --set image.name="$CIUX_IMAGE_NAME" \
  --set image.tag="$CIUX_IMAGE_TAG"


# Wait for Spark pods to be created and warm up
# Debug in case of not expected behaviour
# Science setup is VERY slow to start, because of raw2science-exec pod

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
  echo "Spark log files"
  echo "---------------"
  for task in $tasks; do
    echo "--------- $task log file ---------"
    cat "/tmp/$task.log"
  done
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
