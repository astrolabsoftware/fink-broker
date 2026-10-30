#!/bin/bash

# Show the runs of the nights, nightly and catch-up, and their state

# @author  Fabrice Jammes

set -euo pipefail

namespace="fink-broker"
lost=false
report=false

usage () {
  echo "Usage: $0 [-l] [-r] [-h]"
  echo "  -l: Count the executors lost by each job, from its driver logs (slow)"
  echo "  -r: Print the latest balance report"
  echo "  -h: Display this help"
  echo ""
  echo " List every workflow of the night template, oldest first, with its"
  echo " night, phase and duration, then the jobs and executors of the runs"
  echo " still running. A nightly run is on the UTC day it was created."
}

while getopts hlr opt; do
  case ${opt} in
    h ) usage ; exit 0 ;;
    l ) lost=true ;;
    r ) report=true ;;
    \? ) usage 1>&2 ; exit 1 ;;
  esac
done

kubectl get workflow -n "$namespace" -o json | jq -r '
  def secs: if . then fromdateiso8601 else now end;
  def hm: (. / 3600 | floor | tostring) + "h"
    + (. % 3600 / 60 | floor | tostring | if length < 2 then "0" + . else . end);
  ["WORKFLOW", "NIGHT", "PHASE", "STARTED", "FINISHED", "DURATION"],
  (.items | sort_by(.metadata.creationTimestamp)[]
    | (.spec.arguments.parameters // [] | map(select(.name == "night"))[0].value // "") as $night
    | [ .metadata.name,
        (if ($night | test("^[0-9]{8}$")) then $night
         else .metadata.creationTimestamp[0:10] | gsub("-"; "") end),
        (.status.phase // "Pending"),
        (.status.startedAt // "-"),
        (.status.finishedAt // "-"),
        (if .status.startedAt then
           (.status.finishedAt | secs) - (.status.startedAt | secs) | hm
         else "-" end) ])
  | @tsv' | column -t

running=$(kubectl get workflow -n "$namespace" \
  -l workflows.argoproj.io/phase=Running -o name | cut -d/ -f2)
for workflow in $running; do
  echo ""
  echo "--- $workflow: jobs ---"
  kubectl get sparkapplication -n "$namespace" -o custom-columns=JOB:.metadata.name,STATE:.status.applicationState.state,ATTEMPTS:.status.submissionAttempts,START:.status.lastSubmissionAttemptTime \
    | awk -v w="$workflow" 'NR == 1 || $1 ~ "^" w "-"'
  echo "--- $workflow: pods ---"
  kubectl get pod -n "$namespace" -o wide --no-headers \
    | awk -v w="$workflow" '$1 ~ "^" w "-.*-(driver|exec-[0-9]+)$" {print $1, $3, "restarts=" $4, $7}' \
    | column -t
done

if $lost; then
  echo ""
  echo "--- executors lost, per job ---"
  for driver in $(kubectl get pod -n "$namespace" -o name | grep -- '-driver$' | cut -d/ -f2); do
    count=$(kubectl logs -n "$namespace" "$driver" 2>/dev/null \
      | grep -ciE "lost executor|OOMKilled|exit code 137" || true)
    echo "${driver%-driver} $count"
  done | column -t
fi

if $report; then
  job=$(kubectl get job -n "$namespace" --sort-by=.metadata.creationTimestamp -o name \
    | grep /report- | tail -1)
  echo ""
  echo "--- latest report: $job ---"
  kubectl logs -n "$namespace" "$job"
fi
