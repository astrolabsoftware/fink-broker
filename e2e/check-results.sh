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

# Create docker image containing Fink packaged for k8s

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
monitoring=false
SUFFIX="noscience"

usage () {
  echo "Usage: $0 [-h] [-m]"
  echo "  -m: Check monitoring is enabled"
  echo "  -s: If not equal to 'noscience' use the science algorithms during the tests (default: noscience)"
  echo "  -h: Display this help"
  echo ""
  echo " Check that the expected topics are created"
  exit 1
}

# Get options for suffix
while getopts hms: opt; do
  case ${opt} in
    h )
      usage
      exit 0
      ;;
    m )
      monitoring=true
      ;;
    s) SUFFIX="$OPTARG" ;;
    \? )
      usage
      exit 1
      ;;
  esac
done

# TODO improve management of expected topics
# for example in finkctl.yaml
if [ "$SUFFIX" = "noscience" ];
then
  expected_topics="18"
else
  expected_topics="11"
fi

count=0
while ! finkctl wait topics --expected "$expected_topics" --timeout 60s -v1 > /dev/null
do
    echo "INFO: Waiting for expected topics: $expected_topics"
    sleep 5
    echo "INFO: List pods in spark namespace:"
    kubectl get pods -n spark
    if [ $(kubectl get pods -n spark -l app.kubernetes.io/instance=fink-broker --field-selector=status.phase!=Running | wc -l) -ge 1 ];
    then
        echo "ERROR: fink-broker has crashed" 1>&2
        # Useful for debugging on github actions
        echo "ERROR: enabling interactive access for debugging purpose" 1>&2
        kubectl get pods -n spark -l app.kubernetes.io/instance=fink-broker
        sleep 7200
        exit 1
    fi
    count=$((count+1))
    if [ $count -eq 10 ]; then
        echo "ERROR: Timeout waiting for topics to be created" 1>&2
        kubectl logs -l sparkoperator.k8s.io/launched-by-spark-operator=true  --tail -1
        echo "PODS"
        kubectl get pods -A
        echo "FINK KAFKA TOPICS"
        finkctl get topics
        sleep 7200
        exit 1
    fi
done
finkctl get topics

if $monitoring;
then
    if kubectl exec -it -n spark fink-broker-stream2raw-driver -- curl http://localhost:8090/metrics  | grep jvm > /dev/null
    then
        echo "Prometheus exporter is enabled"
    else
        echo "ERROR: Prometheus exporter is not enabled" 1>&2
        exit 1
    fi

    echo "Checking spark metrics are available in prometheus"
    exp="ztf"
    for task in "stream2raw-driver" "stream2raw-$exp" "raw2science-driver" "raw2science-$exp" "distribution-driver" "distribute-$exp"
    do
         if kubectl exec -t -n monitoring prometheus-prometheus-stack-kube-prom-prometheus-0 -- promtool query range --start 1690724700 http://localhost:9090 jvm_threads_state | grep "$task" > /dev/null
          then
              echo "  Metrics for $task are available"
          else
              echo "  ERROR: Metrics for $task are not available" 1>&2
              exit 1
          fi
    done

fi

echo "INFO: Fink-broker is running and all topics are created"
