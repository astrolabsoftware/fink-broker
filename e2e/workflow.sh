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
# Checks that submit a night workflow through the WorkflowTemplate, then check
# how it starts or ends. The checks that only read state are in
# check-results.sh.

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
mode=""
catchup_timeout=300

usage () {
  echo "Usage: $0 [-h] --catchup|--catchup-open|--nightly"
  echo "  --catchup:  Check that catching up the night of the run does nothing more"
  echo "  --catchup-open: Check that catching up today's night runs until it closes"
  echo "  --nightly:  Check that a nightly run, as submitted by the CronWorkflow,"
  echo "              starts today's night with its deadline"
  echo "  -h: Display this help"
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --catchup) mode="catchup" ; shift ;;
    --catchup-open) mode="catchup-open" ; shift ;;
    --nightly) mode="nightly" ; shift ;;
    -h) usage ; exit 0 ;;
    *) echo "Unknown option: $1" 1>&2 ; usage ; exit 1 ;;
  esac
done

if [ -z "$mode" ]; then
  echo "ERROR: no mode given" 1>&2
  usage
fi

# --catchup: once the workflow which ran the night of the CI is over, catch
# that night up again. The night is then complete, so each job of the second
# workflow must find its _DONE marker and exit at once: nothing is processed
# nor distributed twice.
#
# Waiting for the first workflow is not needed for correctness, the per-night
# mutex would hold the second one, but it bounds the time each may take.
if [ "$mode" = "catchup" ]; then
  jobs="stream2raw raw2science distribution"
  deadline=$(( SECONDS + catchup_timeout ))

  first=$(kubectl get workflows -n fink-broker --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[0].metadata.name}')
  if [ -z "$first" ]; then
    echo "ERROR: no workflow ran the night of the CI" 1>&2
    exit 1
  fi
  night=$(kubectl get workflow -n fink-broker "$first" \
    -o jsonpath='{.spec.arguments.parameters[?(@.name=="night")].value}')

  echo "INFO: Waiting for workflow/$first, running night $night, to succeed"
  while true; do
    phase=$(kubectl get workflow -n fink-broker "$first" -o jsonpath='{.status.phase}')
    case "$phase" in
      Succeeded) break ;;
      Failed|Error)
        echo "ERROR: workflow/$first ended $phase" 1>&2
        kubectl get workflow -n fink-broker "$first" -o jsonpath='{.status.message}' 1>&2 || true
        exit 1 ;;
    esac
    if [ $SECONDS -ge $deadline ]; then
      echo "ERROR: workflow/$first still ${phase:-pending} after ${catchup_timeout}s" 1>&2
      kubectl get workflows,sparkapplications -n fink-broker 1>&2 || true
      exit 1
    fi
    sleep 10
  done

  echo "INFO: Catching night $night up"
  workflow=$("$DIR"/catchup.sh -n "$night" -w -t "$catchup_timeout")

  for job in $jobs; do
    # Read the whole log first: grep -q stops at the first match, and the
    # SIGPIPE it would send to kubectl fails the pipeline under pipefail.
    log=$(kubectl logs -n fink-broker "$workflow-$job-driver")
    if ! grep -q "Night $night already .*, exiting normally" <<< "$log"; then
      echo "ERROR: $job processed night $night again instead of exiting at once" 1>&2
      kubectl logs -n fink-broker "$workflow-$job-driver" --tail 50 1>&2 || true
      exit 1
    fi
  done
  echo "INFO: catch-up of night $night found it complete and did nothing more"
  exit 0
fi

# Check a run of today's open night, just submitted as $workflow for $night:
# its guard must end <guard phase>. When it lets the night through, the jobs
# start with -night and the <expected deadline>, and the run holds the mutex
# of the night; when it refuses it, its log must hold <refusal>.
# Usage: check_open_run <guard phase> <expected deadline> [<refusal>]
check_open_run() {
  local guard_phase="$1" expected="$2" refusal="${3:-}"
  local phase result job app args arg mutex log

  # status.nodes is a map, which jsonpath cannot filter: list it instead
  guard_node() {
    kubectl get workflow -n fink-broker "$workflow" \
      -o jsonpath='{range .status.nodes.*}{.displayName}{"\t"}{.phase}{"\t"}{.outputs.parameters[?(@.name=="state")].value} {.outputs.parameters[?(@.name=="deadline")].value}{"\n"}{end}' |
      awk -F '\t' -v field="$1" '$1 == "guard" { print $field }'
  }
  while true; do
    phase=$(guard_node 2)
    case "$phase" in
      Succeeded|Failed|Error) break ;;
    esac
    if [ $SECONDS -ge $deadline ]; then
      echo "ERROR: guard of workflow/$workflow still ${phase:-pending} after ${catchup_timeout}s" 1>&2
      exit 1
    fi
    sleep 5
  done
  if [ "$phase" != "$guard_phase" ]; then
    echo "ERROR: guard of workflow/$workflow ended $phase, expected $guard_phase" 1>&2
    kubectl logs -n fink-broker -l "workflows.argoproj.io/workflow=$workflow" -c main --tail 20 1>&2 || true
    exit 1
  fi

  if [ "$guard_phase" = "Failed" ]; then
    log=$(kubectl logs -n fink-broker -l "workflows.argoproj.io/workflow=$workflow" -c main --tail -1)
    if ! grep -q "$refusal" <<< "$log"; then
      echo "ERROR: workflow/$workflow failed, but not on the guard's refusal:" 1>&2
      echo "$log" 1>&2
      exit 1
    fi
    return
  fi

  # Output parameters of the guard, as "<state> <deadline>"
  result=$(guard_node 3)
  if [ "$result" != "open $expected" ]; then
    echo "ERROR: guard decided '$result', expected 'open $expected'" 1>&2
    exit 1
  fi
  for job in $jobs; do
    app="$workflow-$job"
    until args=$(kubectl get sparkapplication -n fink-broker "$app" -o jsonpath='{.spec.arguments}' 2> /dev/null); do
      if [ $SECONDS -ge $deadline ]; then
        echo "ERROR: sparkapplication/$app not created after ${catchup_timeout}s" 1>&2
        exit 1
      fi
      sleep 5
    done
    for arg in "\"-night\",\"$night\"" "\"-exit_at\",\"$expected\"" \
      "\"-night_state\",\"open\""; do
      if ! grep -qF -- "$arg" <<< "$args"; then
        echo "ERROR: sparkapplication/$app lacks $arg: $args" 1>&2
        exit 1
      fi
    done
  done
  # The running night holds its mutex, shared by the nightly run and the
  # catch-ups: the night must be resolved in its name, not left as an
  # expression
  mutex=$(kubectl get workflow -n fink-broker "$workflow" \
    -o jsonpath='{.status.synchronization.mutex.holding[*].mutex}')
  if [ "$mutex" != "fink-broker/Mutex/fink-night-$night" ]; then
    echo "ERROR: workflow/$workflow holds mutex '$mutex'," \
      "expected fink-broker/Mutex/fink-night-$night" 1>&2
    exit 1
  fi
}

# --catchup-open: catch today's night up, e.g. to restart a failed nightly
# run. The night is open, so its jobs run live until it closes, the next day:
# the run is deleted once checked.
if [ "$mode" = "catchup-open" ]; then
  jobs="stream2raw raw2science distribution"
  deadline=$(( SECONDS + catchup_timeout ))

  night=$(date -u +%Y%m%d)
  close_hour=$(kubectl get workflowtemplate -n fink-broker night \
    -o jsonpath='{.spec.templates[?(@.name=="guard")].container.args[3]}')
  expected="$(date -u -d "${night:0:4}-${night:4:2}-${night:6:2} + 1 day" +%Y-%m-%d)T$(printf %02d "$close_hour"):00:00Z"
  echo "INFO: Catching today's night $night up, until $expected"
  workflow=$("$DIR"/catchup.sh -n "$night")
  trap 'kubectl delete workflow -n fink-broker "$workflow" --wait > /dev/null 2>&1 || true' EXIT

  check_open_run Succeeded "$expected"
  echo "INFO: catch-up of today's night $night started its jobs, stopping at $expected"
  exit 0
fi

# --nightly: submit the workflow the nightly CronWorkflow submits (same
# template, mode and night expression; the CronWorkflow itself is not deployed
# in CI), check its night and deadline, then delete it: an open night runs
# until its deadline.
if [ "$mode" = "nightly" ]; then
  jobs="stream2raw raw2science distribution"
  deadline=$(( SECONDS + catchup_timeout ))
  template="night"

  workflow=$(kubectl create -n fink-broker -o jsonpath='{.metadata.name}' -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: nightly-check-
spec:
  arguments:
    parameters:
      - name: night
        value: "{{workflow.creationTimestamp.Y}}{{workflow.creationTimestamp.m}}{{workflow.creationTimestamp.d}}"
      - name: mode
        value: nightly
  workflowTemplateRef:
    name: $template
EOF
)
  trap 'kubectl delete workflow -n fink-broker "$workflow" --wait > /dev/null 2>&1 || true' EXIT

  # Today's night is the UTC date of the submission, even if this check
  # crosses midnight
  created=$(kubectl get workflow -n fink-broker "$workflow" -o jsonpath='{.metadata.creationTimestamp}')
  night=$(date -u -d "$created" +%Y%m%d)
  exit_at=$(kubectl get workflowtemplate -n fink-broker "$template" \
    -o jsonpath='{.spec.templates[?(@.name=="guard")].container.args[4]}')
  expected="${night:0:4}-${night:4:2}-${night:6:2}T${exit_at}:00Z"
  echo "INFO: workflow/$workflow runs night $night, expected deadline $expected"

  # Past its deadline (a CI run in the evening), the guard must refuse the
  # night; before, it must let it through with the deadline
  if [ "$(date -u -d "$created" +%s)" -ge "$(date -u -d "$expected" +%s)" ]; then
    check_open_run Failed "$expected" "Nightly run of $night started past its deadline"
    echo "INFO: nightly run of $night refused past its deadline $expected"
  else
    check_open_run Succeeded "$expected"
    echo "INFO: nightly run of $night started its jobs, stopping at $expected"
  fi
  exit 0
fi

echo "ERROR: unknown mode $mode" 1>&2
exit 1
