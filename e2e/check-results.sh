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
  expected_topics="20"
else
  # 3 topics do not send results
  expected_topics="16"
fi

# Wait for topics to be created, and check if fink-broker has not crashed in the meantime
# display logs of failed pods if any, and of running pods if no topics after 10 attempts (5 minutes)
count=0
max_attempts=10
selector="spark-app-name"
while ! finkctl wait topics --expected "$expected_topics" --timeout 60s -v1 > /dev/null
do
    echo "INFO: Waiting for expected topics: $expected_topics"
    sleep 5
    echo "INFO: List pods in spark namespace:"
    kubectl get pods -n spark

    crashed_pods=$(kubectl get pods -n spark -l $selector --field-selector=status.phase=Failed -o name)
    if [ -n "$crashed_pods" ]; then
      echo "ERROR: crashed pods found: $crashed_pods" 1>&2
          for pod in $crashed_pods
          do
              echo "--- POD: $pod ---"
              kubectl logs "$pod" -n spark
          done
          running_pods=$(kubectl get pods -n spark -l $selector --field-selector=status.phase=Running -o name)
          if [ -n "$running_pods" ]; then
              echo "INFO: logs of running pods:"
              for pod in $running_pods                do
                  echo "--- POD: $pod ---"
                  kubectl logs "$pod" -n spark --tail -1
              done
          fi
      echo "ERROR: fink-broker has crashed" 1>&2
      # echo "ERROR: enabling interactive access for debugging purpose" 1>&2
      # sleep 7200
      exit 1
    fi

    count=$((count+1))
    if [ $count -eq $max_attempts ]; then
      pods=$(kubectl get pods -n spark -l $selector -o name)
      for pod in $pods
      do
          echo "--- POD: $pod ---"
          kubectl logs "$pod" -n spark --tail -1
      done
      echo "ERROR: fink-broker did not produce expected results after ~10 minutes" 1>&2
      # echo "ERROR: enabling interactive access for debugging purpose" 1>&2
      # sleep 7200
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
