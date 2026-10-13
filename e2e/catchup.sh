#!/bin/bash

# Catch a ZTF night up through the Argo WorkflowTemplate fink-broker-night

# @author  Fabrice Jammes

set -euo pipefail

namespace="spark"
template="fink-broker-night"
night=""
wait=false
timeout=900

usage () {
  echo "Usage: $0 -n <night> [-w] [-t <timeout>] [-h]"
  echo "  -n: Night to catch up, YYYYMMDD (UTC)"
  echo "  -w: Wait for the workflow to end, exit non-zero if it did not succeed"
  echo "  -t: Seconds to wait with -w. Default: $timeout"
  echo "  -h: Display this help"
  echo ""
  echo " Submit a workflow running the three jobs of the night from the"
  echo " checkpoints of the daily run, and print its name on stdout. The"
  echo " workflow refuses a night still open (see nightCloseHour in the chart),"
  echo " and waits for any other catch-up of the same night to end."
}

while getopts hn:t:w opt; do
  case ${opt} in
    h ) usage ; exit 0 ;;
    n ) night="$OPTARG" ;;
    t ) timeout="$OPTARG" ;;
    w ) wait=true ;;
    \? ) usage 1>&2 ; exit 1 ;;
  esac
done

if [ -z "$night" ]; then
  usage 1>&2
  exit 1
fi

workflow=$(kubectl create -n "$namespace" -o jsonpath='{.metadata.name}' -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: catchup-$night-
spec:
  arguments:
    parameters:
      - name: night
        value: "$night"
  workflowTemplateRef:
    name: $template
EOF
)
echo "$workflow"
echo "INFO: workflow/$workflow submitted for night $night" 1>&2

if ! $wait; then
  exit 0
fi

deadline=$(( SECONDS + timeout ))
while true; do
  phase=$(kubectl get workflow -n "$namespace" "$workflow" -o jsonpath='{.status.phase}')
  case "$phase" in
    Succeeded)
      echo "INFO: workflow/$workflow succeeded" 1>&2
      exit 0 ;;
    Failed|Error)
      echo "ERROR: workflow/$workflow ended $phase:" \
        "$(kubectl get workflow -n "$namespace" "$workflow" -o jsonpath='{.status.message}')" 1>&2
      break ;;
  esac
  if [ $SECONDS -ge $deadline ]; then
    echo "ERROR: workflow/$workflow still ${phase:-pending} after ${timeout}s" 1>&2
    break
  fi
  sleep 10
done

kubectl get workflow -n "$namespace" "$workflow" \
  -o jsonpath='{range .status.nodes.*}{.displayName}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}' 1>&2 || true
# The guard says why it refused the night; the drivers exist only past it
echo "--- logs of the workflow pods ---" 1>&2
kubectl logs -n "$namespace" -l "workflows.argoproj.io/workflow=$workflow" \
  -c main --prefix --tail 20 1>&2 || true
for job in stream2raw raw2science distribution; do
  pod="$workflow-$job-driver"
  if kubectl get pod -n "$namespace" "$pod" > /dev/null 2>&1; then
    echo "--- logs of $pod ---" 1>&2
    kubectl logs -n "$namespace" "$pod" --tail 50 1>&2 || true
  fi
done
exit 1
