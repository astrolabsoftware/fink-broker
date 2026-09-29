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
report_timeout=600

usage () {
  echo "Usage: $0 [-h] [--basic|--report] [-m] [-s <suffix>]"
  echo "  --basic:    Check that the expected topics are created (default)"
  echo "  --report:   Check the balance printed by the report CronJob"
  echo "  -m: Check monitoring is enabled (--basic only)"
  echo "  -s: Specify suffix ('noscience' or 'science'). Default: noscience"
  echo "  -h: Display this help"
  echo ""
  echo " Two levels of checking, run as separate CI steps so a failure names"
  echo " itself: --basic asserts the broker produced its topics, --report"
  echo " asserts the alerts can be accounted for from end to end."
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --basic) mode="basic" ; shift ;;
    --report) mode="report" ; shift ;;
    -m) monitoring=true ; shift ;;
    -s) SUFFIX="$2" ; shift 2 ;;
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
# $1: file holding the report.
#
# The TOTAL row printed by printReport sums both ends over every night:
#   TOTAL    IN(kafka)                       DISTRIB
# It is read rather than a night row: the night the run pinned lives in the
# fink-cd values, and duplicating it here would rot silently.
assert_balance () {
  local file="$1" row consumed distributed

  row=$(grep -E "^[[:space:]]+TOTAL[[:space:]]" "$file" || true)
  if [ -z "$row" ]; then
    echo "ERROR: no TOTAL row in the report" 1>&2
    return 1
  fi

  consumed=$(echo "$row" | awk '{print $2}')
  distributed=$(echo "$row" | awk '{print $3}')

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

# --report: account for the alerts that went through the broker, from what the
# report CronJob of the chart printed. finkctl reads HDFS directly, so it must
# run in the cluster: this exercises its image, its ServiceAccount, the Role
# letting it exec into the Kafka pods and the arguments Helm renders for it.
#
# The parsing and the arithmetic behind `get balance` are covered by unit
# tests in the finkctl repository. Counts are not asserted against fixed
# values -- the alert simulator does not produce a deterministic number of
# alerts. Only that both ends of the broker moved.
#
# CI sets report.schedule to every minute, so a Job appears within the minute.
# Early Jobs may legitimately find nothing -- the streaming jobs are still
# warming up -- so a Job whose report does not add up is not a failure on its
# own: wait for a later one, and fail on the timeout.
#
# A failed Job is, though. The CronJob runs with restartPolicy=OnFailure: a run
# hitting a transient error (a pod not ready yet) restarts its container until
# it succeeds, and a Job only turns Failed once its backoffLimit is exhausted,
# i.e. on a persistent error. A report with no night yet is not an error. Checking only for a successful Job
# would let such a failure hide behind a later run that succeeds, while at CC
# the report runs once a day and that failure is all there is.
if [ "$mode" = "report" ]; then
  cronjob="fink-broker-report"
  deadline=$(( SECONDS + report_timeout ))

  # Print the Jobs of the CronJob that ended in the Failed condition.
  failed_jobs() {
    kubectl get jobs -n spark \
      -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.status.conditions[?(@.type=='Failed')].status}{'\n'}{end}" \
      2>/dev/null | awk -F '\t' -v cj="$cronjob" 'index($1, cj "-") == 1 && $2 == "True" { print $1 }'
  }

  echo "INFO: Waiting for a run of cronjob/$cronjob to account for the alerts"
  while [ $SECONDS -lt $deadline ]; do
    failed=$(failed_jobs)
    if [ -n "$failed" ]; then
      echo "ERROR: cronjob/$cronjob has failed runs: $(echo $failed)" 1>&2
      for job in $failed; do
        echo "--- job/$job ---" 1>&2
        kubectl describe job -n spark "$job" 1>&2 || true
        kubectl logs -n spark "job/$job" --tail -1 1>&2 || true
      done
      exit 1
    fi

    for job in $(kubectl get jobs -n spark \
        -o jsonpath="{range .items[?(@.status.succeeded==1)]}{.metadata.name}{'\n'}{end}" \
        2>/dev/null | grep "^${cronjob}-" | sort -r); do
      out="/tmp/${job}.out"
      kubectl logs -n spark "job/${job}" > "$out" 2>&1 || continue
      if assert_balance "$out" > /dev/null 2>&1; then
        echo "INFO: report produced by job/${job}"
        cat "$out"
        assert_balance "$out"
        # Not a failure, but the sign of a run started too early or of a
        # flaky dependency: say so, with the logs of the failed attempts.
        kubectl get pods -n spark -l job-name \
          -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.status.containerStatuses[0].restartCount}{'\n'}{end}" \
          2>/dev/null | awk -F '\t' -v cj="$cronjob" 'index($1, cj "-") == 1 && $2 > 0' \
          | while IFS=$'\t' read -r pod restarts; do
              echo "WARNING: pod/$pod restarted $restarts time(s), last failed attempt:"
              kubectl logs -n spark "$pod" --previous --tail 20 || true
            done
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

# TODO improve management of expected topics
# for example in finkctl.yaml
if [ "$SUFFIX" = "noscience" ];
then
  expected_topics="20"
else
  # 3 topics do not send results
  expected_topics="18"
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
