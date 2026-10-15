#!/bin/bash

# Catch a ZTF night up through the Argo WorkflowTemplate night

# @author  Fabrice Jammes

set -euo pipefail

namespace="fink-broker"
template="night"
night=""
wait=false
timeout=900

usage () {
  echo "Usage: $0 [-n <night>] [-w] [-t <timeout>] [-h]"
  echo "  -n: Night to catch up, YYYYMMDD (UTC). Default: the default night of"
  echo "      the WorkflowTemplate, set without a nightly run only (CI)"
  echo "  -w: Wait for the workflow to end, exit non-zero if it did not succeed"
  echo "  -t: Seconds to wait with -w. Default: $timeout"
  echo "  -h: Display this help"
  echo ""
  echo " Submit a workflow running the three jobs of the night from the"
  echo " checkpoints of the nightly run, and print its name on stdout. An open"
  echo " night (until nightCloseHour, next day) runs live until it closes; a"
  echo " closed one stops once caught up. The workflow waits for any other run"
  echo " of the same night, nightly or catch-up, to end."
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
  night=$(kubectl get workflowtemplate -n "$namespace" "$template" \
    -o jsonpath='{.spec.arguments.parameters[?(@.name=="night")].value}')
  if [ -z "$night" ]; then
    echo "ERROR: no night given, and $template has no default night" 1>&2
    usage 1>&2
    exit 1
  fi
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
