#!/bin/bash

# Run fink-broker e2e tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")


export ARGOCD_CONFIG_PATH=/var/lib/fink-ci/argocd.config

echo "=== Fink-Broker E2E Test Suite ==="
echo "Start time: $START_TIME"
echo "=================================="

usage () {
  echo "Usage: $0 [-c] [-h] [-m] [-s <suffix>]"
  echo "  -s: Specify suffix ('noscience' or 'science'). Default: noscience"
  echo "  -c: Cleanup the cluster after the tests"
  echo "  -i: Specify input survey. Default: ztf"
  echo "  -m: Install monitoring stack"
  echo "  -h: Display this help"
  echo ""
  echo " Run fink-broker e2e tests, using source code from the parent directory."
  exit 1
}

SUFFIX="noscience"

ciux_version=v0.0.7-rc6

src_dir=$DIR/..
cleanup=false
build=false
e2e=false
monitoring=false
push=false
storage="hdfs"
CIUX_IMAGE_URL="undefined"
input_survey="ztf"

token="${TOKEN:-}"

# Get options for suffix
while getopts hcms:i:S: opt; do
  case ${opt} in

    c )
      cleanup=true
      ;;
    h )
      usage
      exit 0
      ;;
    m )
      monitoring=true
      ;;
    s )
      SUFFIX="$OPTARG"
      ;;
    i )
      input_survey="$OPTARG"
      ;;
    S) storage="$OPTARG" ;;
    \? )
      usage
      exit 1
      ;;
  esac
done

# Validate suffix value
if [ -n "$SUFFIX" ] && [ "$SUFFIX" != "noscience" ] && [ "$SUFFIX" != "science" ]; then
    echo "Error: suffix must be 'noscience' or 'science'"
    usage
    exit 1
fi

export SUFFIX

go install github.com/k8s-school/ciux@"$ciux_version"

echo "Ignite the project using ciux"
ciux ignite --selector itest "$src_dir" --build "$SUFFIX"

# Build step
echo "Phase: Build"
if $src_dir/build.sh -s "$SUFFIX" -i $input_survey; then
    build=true
else
    echo "ERROR: Build failed" 1>&2
    exit 1
fi

# e2e tests step
cluster=$(ciux get clustername "$src_dir")
echo "Delete the cluster $cluster if it already exists"
ktbx delete --name "$cluster" || true

echo "Phase: Prerequisites Installation"
echo "Create a Kubernetes cluster (Kind), Install OLM and ArgoCD operators."
monitoring_opt=""
if [ $monitoring = true ]
then
  monitoring_opt="-m"
fi
if ! $DIR/prereq-install.sh -k "v0.20.0" $monitoring_opt; then
    echo "ERROR: Prerequisites installation failed" 1>&2
    exit 1
fi

$(ciux get image --check $src_dir --build "$SUFFIX" --env)
if [ $CIUX_BUILD = true ]; then
  kind load docker-image $CIUX_IMAGE_URL --name "$cluster"
fi

echo "Phase: ArgoCD Deployment"
echo "Run ArgoCD to install the whole fink e2e tests stack"
if ! $DIR/argocd.sh -i "ci-$SUFFIX" -S "$storage" $monitoring_opt; then
    echo "ERROR: ArgoCD deployment failed" 1>&2
    exit 1
fi

echo "Phase: Night Run"
echo "Run the night through the WorkflowTemplate, as any catch-up."
echo "Its night is the one of the topic written by the simulator."
night=$(finkctl get sim-night --timeout 5m)
if ! $DIR/catchup.sh -n "$night"; then
    echo "ERROR: Night submission failed" 1>&2
    exit 1
fi

echo "Phase: Test Results Check"
echo "Check the results of the tests."
if $DIR/check-results.sh --basic -s "$SUFFIX" $monitoring_opt; then
    e2e=true
else
    echo "ERROR: Test results check failed" 1>&2
    exit 1
fi

# Reported apart from the check above, so a failure says which of the two it
# was. HDFS only: the report CronJob reads the datasets from the namenode.
if [ "$storage" = "hdfs" ]; then
    echo "Phase: Balance Check"
    echo "Account for the alerts which went through the broker."
    if ! $DIR/check-results.sh --report; then
        echo "ERROR: Balance check failed" 1>&2
        exit 1
    fi
fi

echo "Phase: Catch-up Check"
echo "Catch the night of the run up, which must find it complete."
if ! $DIR/workflow.sh --catchup-done; then
    echo "ERROR: Catch-up check failed" 1>&2
    exit 1
fi

echo "Phase: Open Night Catch-up Check"
echo "Catch today's night up, which runs until it closes."
if ! $DIR/workflow.sh --catchup-open; then
    echo "ERROR: Open night catch-up check failed" 1>&2
    exit 1
fi

echo "Phase: Nightly Run Check"
echo "Submit a nightly run as the CronWorkflow does, check its night and deadline."
if ! $DIR/workflow.sh --nightly; then
    echo "ERROR: Nightly run check failed" 1>&2
    exit 1
fi

echo "Phase: Image Push"
echo "Push the image to Container Registry"
if $src_dir/push-image.sh; then
    push=true
else
    echo "ERROR: Image push failed" 1>&2
    exit 1
fi

# Get final stats for display
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TOTAL_DURATION=$(( $(date -d "$END_TIME" +%s) - $(date -d "$START_TIME" +%s) ))

echo "=================================="
echo "🎉 FINK E2E TESTS COMPLETED!"
echo "Duration: ${TOTAL_DURATION}s"
echo "=================================="

