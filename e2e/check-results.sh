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
mode="basic"
night=""
prefix="/user/185"
report_timeout=600

usage () {
  echo "Usage: $0 [-h] [--basic|--advanced|--report] [-m] [-s <suffix>] [-n <night>] [-p <prefix>]"
  echo "  --basic:    Check that the expected topics are created (default)"
  echo "  --advanced: Check the balance reported by 'finkctl get balance'"
  echo "  --report:   Check the balance printed by the report CronJob itself"
  echo "  -m: Check monitoring is enabled (--basic only)"
  echo "  -s: Specify suffix ('noscience' or 'science'). Default: noscience"
  echo "  -n: Observing night to check (YYYYMMDD, --advanced only)."
  echo "      Default: every night found, checked on the TOTAL row"
  echo "  -p: HDFS path prefix holding the datasets (--advanced only). Default: /user/185"
  echo "  -h: Display this help"
  echo ""
  echo " Two levels of checking, run as separate CI steps so a failure names"
  echo " itself: --basic asserts the broker produced its topics, --advanced"
  echo " asserts the alerts can be accounted for from end to end."
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --basic) mode="basic" ; shift ;;
    --advanced) mode="advanced" ; shift ;;
    --report) mode="report" ; shift ;;
    -m) monitoring=true ; shift ;;
    -s) SUFFIX="$2" ; shift 2 ;;
    -n) night="$2" ; shift 2 ;;
    -p) prefix="$2" ; shift 2 ;;
    -h) usage ; exit 0 ;;
    *) echo "Unknown option: $1" 1>&2 ; usage ; exit 1 ;;
  esac
done

# Validate suffix value
if [ -n "$SUFFIX" ] && [ "$SUFFIX" != "noscience" ] && [ "$SUFFIX" != "science" ]; then
    echo "Error: suffix must be 'noscience' or 'science'"
    usage
    exit 1
fi

# Assert a balance report accounts for alerts at both ends of the broker.
# $1: file holding the report, $2: night to look at, empty for the TOTAL row.
#
# Rows printed by printReport:
#   <night>  IN(kafka) RAW(f) RAW SCI(f) SCI DISTRIB
#   TOTAL    IN(kafka)                       DISTRIB
#
# Without an explicit night the TOTAL row is read: the night the run pinned
# lives in the fink-cd values, and duplicating it here would rot silently.
assert_balance () {
  local file="$1" want_night="$2" row consumed distributed consumed_col distributed_col

  if [ -n "$want_night" ]; then
    row=$(grep -E "^[[:space:]]+${want_night}[[:space:]]" "$file" || true)
    consumed_col=2
    distributed_col=7
  else
    row=$(grep -E "^[[:space:]]+TOTAL[[:space:]]" "$file" || true)
    consumed_col=2
    distributed_col=3
  fi
  if [ -z "$row" ]; then
    echo "ERROR: no balance row${want_night:+ for night $want_night} in the report" 1>&2
    return 1
  fi

  consumed=$(echo "$row" | awk -v c="$consumed_col" '{print $c}')
  distributed=$(echo "$row" | awk -v c="$distributed_col" '{print $c}')

  local name value
  for name in consumed distributed; do
    eval "value=\$$name"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
      echo "ERROR: $name is not a count: '$value'" 1>&2
      echo "       row: $row" 1>&2
      return 1
    fi
    if [ "$value" -le 0 ]; then
      echo "ERROR: $name is $value, expected alerts to have gone through" 1>&2
      echo "       row: $row" 1>&2
      return 1
    fi
  done

  echo "INFO: balance is consistent: $consumed alerts consumed, $distributed distributed"
  return 0
}

# --report: same assertion, but on what the report CronJob itself printed.
#
# --advanced runs finkctl from the runner, with the runner's kubeconfig. It
# says nothing about the CronJob the chart deploys: its image, its
# ServiceAccount, the Roles letting it exec into the hdfs and kafka pods, or
# the arguments Helm renders for it. Those only break in a cluster, and this
# is where they are exercised.
#
# CI sets report.schedule to every minute, so a Job appears within the minute.
# Early Jobs may legitimately find nothing -- the streaming jobs are still
# warming up -- so a Job whose report does not add up is not a failure on its
# own: wait for a later one, and fail on the timeout.
if [ "$mode" = "report" ]; then
  cronjob="fink-broker-report"
  deadline=$(( SECONDS + report_timeout ))

  echo "INFO: Waiting for a run of cronjob/$cronjob to account for the alerts"
  while [ $SECONDS -lt $deadline ]; do
    for job in $(kubectl get jobs -n spark \
        -o jsonpath="{range .items[?(@.status.succeeded==1)]}{.metadata.name}{'\n'}{end}" \
        2>/dev/null | grep "^${cronjob}-" | sort -r); do
      out="/tmp/${job}.out"
      kubectl logs -n spark "job/${job}" > "$out" 2>&1 || continue
      if assert_balance "$out" "" > /dev/null 2>&1; then
        echo "INFO: report produced by job/${job}"
        cat "$out"
        assert_balance "$out" ""
        exit 0
      fi
    done
    sleep 10
  done

  echo "ERROR: no run of cronjob/$cronjob accounted for the alerts within ${report_timeout}s" 1>&2
  kubectl get cronjob,jobs -n spark 1>&2 || true
  for job in $(kubectl get jobs -n spark -o name 2>/dev/null | grep "$cronjob"); do
    echo "--- logs of $job ---" 1>&2
    kubectl logs -n spark "$job" --tail -1 1>&2 || true
  done
  exit 1
fi

# --advanced: account for the alerts that went through the broker.
#
# The parsing and the arithmetic behind `get balance` are covered by unit tests
# in the finkctl repository. What cannot be tested there is the part touching a
# live cluster: locating the HDFS and Kafka pods, being allowed to exec into
# them, and the output format of the tools it drives. That is what this checks.
#
# Counts are not asserted against fixed values -- the alert simulator does not
# produce a deterministic number of alerts. Only that both ends of the broker
# moved, which is enough to catch a broken pod lookup, a denied exec or a
# changed output format.
#
# HDFS only: balance reads the datasets from inside the namenode pod, so it has
# nothing to read when the run stores its data in S3.
if [ "$mode" = "advanced" ]; then
  night_args=()
  if [ -n "$night" ]; then
    night_args+=(--night "$night")
  fi

  out="/tmp/finkctl-balance.out"
  echo "INFO: Running finkctl get balance${night:+ for night $night}"
  if ! finkctl get balance --prefix "$prefix" "${night_args[@]}" > "$out" 2>&1; then
    echo "ERROR: finkctl get balance failed" 1>&2
    cat "$out" 1>&2
    exit 1
  fi
  cat "$out"

  if ! assert_balance "$out" "$night"; then
    exit 1
  fi
  exit 0
fi

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
# display logs of failed pods if any, and of running pods if no topics after 10 attempts (~10 minutes)
count=0
max_attempts=20
selector="spark-app-name"
err_msg=""
while ! finkctl wait topics --expected "$expected_topics" --timeout 60s -v1 > /dev/null
do
    echo "INFO: Waiting for expected topics: $expected_topics, attempt: $((count+1))/$max_attempts"
    sleep 5
    echo "INFO: List pods in spark namespace:"
    kubectl get pods -n spark

    crashed_pods=$(kubectl get pods -n spark -l $selector --field-selector=status.phase=Failed -o name)
    if [ -n "$crashed_pods" ]; then
      echo "ERROR: crashed pods found: $crashed_pods" 1>&2
          for pod in $crashed_pods
          do
              echo "--- Logs for crashed Pod: $pod ---"
              kubectl logs "$pod" -n spark
          done
          running_pods=$(kubectl get pods -n spark -l $selector --field-selector=status.phase=Running -o name)
          if [ -n "$running_pods" ]; then
              echo "INFO: logs of running pods:"
              for pod in $running_pods; do
                  echo "--- Logs for running Pod: $pod ---"
                  kubectl logs "$pod" -n spark --tail -1
              done
          fi
      err_msg="ERROR: fink-broker has crashed" 1>&2
      # echo "ERROR: enabling interactive access for debugging purpose" 1>&2
      # sleep 7200
      break
    fi

    count=$((count+1))
    if [ $count -eq $max_attempts ]; then
      pods=$(kubectl get pods -n spark -l $selector -o name)
      for pod in $pods
      do
          echo "--- Logs for Pod: $pod ---"
          kubectl logs "$pod" -n spark --tail -1
      done
      err_msg="ERROR: fink-broker did not produce expected results after ~20 minutes"
      # echo "ERROR: enabling interactive access for debugging purpose" 1>&2
      # sleep 7200
      break
    fi
done
finkctl get topics

if [ -n "$err_msg" ]; then
  echo "$err_msg" 1>&2
  exit 1
fi

if $monitoring;
then
    echo "Checking prometheus exporter is enabled in fink-broker"
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
