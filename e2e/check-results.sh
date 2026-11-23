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
# Endpoint probed to tell whether the CDS xmatch service, called by the
# science modules of fink-science, is reachable. Overridable for testing.
CDS_XMATCH_PROBE_URL="${CDS_XMATCH_PROBE_URL:-http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync/tables?action=getVizieRTableNames&RESPONSEFORMAT=json}"

# Succeed if the CDS xmatch service answers. Redirects are followed, as the
# Python requests library used by fink-science does.
cds_reachable () {
  curl -sfL -o /dev/null --max-time 15 "$CDS_XMATCH_PROBE_URL"
}

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

# --report: account for the alerts that went through the broker, from what the
# report CronJob of the chart printed. finkctl reads HDFS directly, so it must
# run in the cluster: this exercises its image, its ServiceAccount, the Role
# letting it exec into the Kafka pods and the arguments Helm renders for it.
#
# finkctl decides the balance, next to the code printing the report: after
# the TOTAL row it prints "BALANCE: ok ..." when both ends of the broker moved,
# "BALANCE: empty ..." otherwise. Counts are not asserted against fixed values
# -- the alert simulator does not produce a deterministic number of alerts.
#
# CI sets report.schedule to every minute, so a Job appears within the minute.
# Early Jobs may legitimately find nothing -- the streaming jobs are still
# warming up -- so an empty balance is not a failure on its own: wait for a
# later Job, and fail on the timeout. Only the latest succeeded Job decides:
# counts are cumulative, so an "ok" followed by an "empty" is a regression.
#
# A failed Job is a failure, though. The CronJob runs with
# restartPolicy=OnFailure: a run hitting a transient error (a pod not ready
# yet) restarts its container until it succeeds, and a Job only turns Failed
# once its backoffLimit is exhausted, i.e. on a persistent error. A report with
# no night yet is not an error. Checking only for a successful Job would let
# such a failure hide behind a later run that succeeds, while at CC the report
# runs once a day and that failure is all there is.
if [ "$mode" = "report" ]; then
  cronjob="report"
  deadline=$(( SECONDS + report_timeout ))

  # Print the Jobs of the CronJob that ended in the Failed condition.
  failed_jobs() {
    kubectl get jobs -n fink-broker \
      -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.status.conditions[?(@.type=='Failed')].status}{'\n'}{end}" \
      2>/dev/null | awk -F '\t' -v cj="$cronjob" 'index($1, cj "-") == 1 && $2 == "True" { print $1 }'
  }

  # Print the most recent succeeded Job of the CronJob, by completion time.
  latest_succeeded_job() {
    kubectl get jobs -n fink-broker \
      -o jsonpath="{range .items[?(@.status.succeeded==1)]}{.status.completionTime}{'\t'}{.metadata.name}{'\n'}{end}" \
      2>/dev/null | awk -F '\t' -v cj="$cronjob" 'index($2, cj "-") == 1' | sort | tail -1 | cut -f2
  }

  echo "INFO: Waiting for a run of cronjob/$cronjob to account for the alerts"
  while [ $SECONDS -lt $deadline ]; do
    failed=$(failed_jobs)
    if [ -n "$failed" ]; then
      echo "ERROR: cronjob/$cronjob has failed runs: $(echo $failed)" 1>&2
      for job in $failed; do
        echo "--- job/$job ---" 1>&2
        kubectl describe job -n fink-broker "$job" 1>&2 || true
        kubectl logs -n fink-broker "job/$job" --tail -1 1>&2 || true
      done
      exit 1
    fi

    job=$(latest_succeeded_job)
    if [ -n "$job" ] && log=$(kubectl logs -n fink-broker "job/${job}" 2>&1); then
      verdict=$(grep '^BALANCE: ' <<< "$log" || true)
      case "$verdict" in
        "BALANCE: ok "*)
          echo "INFO: report produced by job/${job}"
          echo "$log"
          # Not a failure, but the sign of a run started too early or of a
          # flaky dependency: say so, with the logs of the failed attempts.
          kubectl get pods -n fink-broker -l job-name \
            -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.status.containerStatuses[0].restartCount}{'\n'}{end}" \
            2>/dev/null | awk -F '\t' -v cj="$cronjob" 'index($1, cj "-") == 1 && $2 > 0' \
            | while IFS=$'\t' read -r pod restarts; do
                echo "WARNING: pod/$pod restarted $restarts time(s), last failed attempt:"
                kubectl logs -n fink-broker "$pod" --previous --tail 20 || true
              done
          exit 0 ;;
        "BALANCE: empty "*) ;;
        *)
          echo "ERROR: job/${job} printed no BALANCE verdict, is its finkctl image too old?" 1>&2
          echo "$log" 1>&2
          exit 1 ;;
      esac
    fi
    sleep 10
  done

  echo "ERROR: no run of cronjob/$cronjob accounted for the alerts within ${report_timeout}s" 1>&2
  kubectl get cronjob,jobs -n fink-broker 1>&2 || true
  for job in $(kubectl get jobs -n fink-broker -o name 2>/dev/null | grep "$cronjob"); do
    echo "--- logs of $job ---" 1>&2
    kubectl logs -n fink-broker "$job" --tail -1 1>&2 || true
  done
  exit 1
fi

# The topics to wait for, by name. CI selects the science build with an empty
# suffix, not 'science'.
cds_up=true
without=()
if [ "$SUFFIX" = "noscience" ]; then
  expected_file="$DIR/expected-topics-noscience.yaml"
else
  expected_file="$DIR/expected-topics-science.yaml"
  # While the CDS xmatch service is down, SIMBAD classes are empty and the
  # filters relying on them produce no alert: their topics, the "cds" group,
  # are not required.
  if ! cds_reachable; then
    cds_up=false
    without=(--without cds)
    echo "WARNING: CDS xmatch unreachable ($CDS_XMATCH_PROBE_URL):" \
      "SIMBAD-based filters produce no alerts, not waiting for their topics" 1>&2
  fi
fi

# Succeed once every expected topic exists. On timeout, finkctl names the
# missing topics.
topics_ready () {
  finkctl wait topics --expected-file "$expected_file" "${without[@]}" \
    --timeout 60s -v1
}

# Wait for topics to be created, and check if fink-broker has not crashed in the meantime
# display logs of failed pods if any, and of running pods if no topics after 10 attempts (~10 minutes)
count=0
max_attempts=20
selector="spark-app-name"
err_msg=""
while ! topics_ready
do
    echo "INFO: Waiting for the topics of $(basename "$expected_file"), attempt: $((count+1))/$max_attempts"
    sleep 5
    echo "INFO: List pods in fink-broker namespace:"
    kubectl get pods -n fink-broker

    crashed_pods=$(kubectl get pods -n fink-broker -l $selector --field-selector=status.phase=Failed -o name)
    if [ -n "$crashed_pods" ]; then
      echo "ERROR: crashed pods found: $crashed_pods" 1>&2
          for pod in $crashed_pods
          do
              echo "--- Logs for crashed Pod: $pod ---"
              kubectl logs "$pod" -n fink-broker
          done
          running_pods=$(kubectl get pods -n fink-broker -l $selector --field-selector=status.phase=Running -o name)
          if [ -n "$running_pods" ]; then
              echo "INFO: logs of running pods:"
              for pod in $running_pods; do
                  echo "--- Logs for running Pod: $pod ---"
                  kubectl logs "$pod" -n fink-broker --tail -1
              done
          fi
      err_msg="ERROR: fink-broker has crashed" 1>&2
      # echo "ERROR: enabling interactive access for debugging purpose" 1>&2
      # sleep 7200
      break
    fi

    count=$((count+1))
    if [ $count -eq $max_attempts ]; then
      pods=$(kubectl get pods -n fink-broker -l $selector -o name)
      for pod in $pods
      do
          echo "--- Logs for Pod: $pod ---"
          kubectl logs "$pod" -n fink-broker --tail -1
      done
      err_msg="ERROR: fink-broker did not produce expected results after ~20 minutes"
      # The CDS may have gone down after the first probe: say so. CI selects
      # the science build with an empty suffix, not 'science'.
      if [ "$SUFFIX" != "noscience" ] && $cds_up && ! cds_reachable; then
        err_msg="$err_msg (CDS xmatch became unreachable during the run:"
        err_msg="$err_msg SIMBAD-based filters may have produced no alerts)"
      fi
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
    # Named after the workflow which created it
    driver=$(kubectl get pods -n fink-broker -l spark-role=driver -o name | grep -m1 -- "-stream2raw-driver$")
    if kubectl exec -n fink-broker "$driver" -- curl http://localhost:8090/metrics  | grep jvm > /dev/null
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
