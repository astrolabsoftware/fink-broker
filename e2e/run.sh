#!/bin/bash

# Run fink-broker e2e tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Test report file (YAML format for better GHA integration)
HOME_CI_RESULT_FILE="${HOME_CI_RESULT_FILE:-}"
if [ -z "$HOME_CI_RESULT_FILE" ]; then
    TEST_REPORT="$DIR/../e2e-report.yaml"
    echo "WARNING: HOME_CI_RESULT_FILE is not set, using temporary file $TEST_REPORT"
else
    TEST_REPORT="$HOME_CI_RESULT_FILE"
fi

# Create log directory if it doesn't exist
mkdir -p "$(dirname "$TEST_REPORT")"

# Initialize YAML report
home-ci-reporter init "$TEST_REPORT" "fink-broker"

echo "=== Fink-Broker E2E Test Suite ==="
echo "Start time: $START_TIME"
echo "Test report: $TEST_REPORT"
echo "=================================="

usage () {
  echo "Usage: $0 [-c] [-h] [-m] [-s]"
  echo "  -s: Use the science algorithms during the tests"
  echo "  -c: Cleanup the cluster after the tests"
  echo "  -i: Specify input survey. Default: ztf"
  echo "  -m: Install monitoring stack"
  echo "  -h: Display this help"
  echo ""
  echo " Run fink-broker e2e tests, using source code from the parent directory."
  exit 1
}

SUFFIX="noscience"

ciux_version=v0.0.7-rc1

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
while getopts hcmsiS: opt; do
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
      SUFFIX=""
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

export SUFFIX

function dispatch()
{
    if [ "$SUFFIX" = "" ]; then
      echo "Running e2e tests with science algorithms"
      event_type="e2e-science"
    else
      echo "Running e2e tests without science algorithms"
      event_type="e2e-noscience"
    fi

    url="https://api.github.com/repos/astrolabsoftware/fink-broker/dispatches"

    payload="{\"build\": $build,\"e2e\": $e2e,\"push\": $push, \"cluster\": \"$cluster\", \"image\": \"$CIUX_IMAGE_URL\"}"
    echo "Payload: $payload"

    if [ -z "$token" ]; then
      echo "No token provided, skipping GitHub dispatch"
    else
      echo "Dispatching event to GitHub"
      curl -L \
      -X POST \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer $token" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      $url \
      -d "{\"event_type\":\"$event_type\",\"client_payload\":$payload}" || echo "ERROR Failed to dispatch event" >&2
    fi

    if [ $cleanup = true -a $e2e = true ]; then
      echo "Delete the cluster $cluster"
      ktbx delete --name "$cluster"
    else
      echo "Cluster $cluster kept for debugging"
    fi
}

trap dispatch EXIT

go install github.com/k8s-school/ciux@"$ciux_version"

echo "Ignite the project using ciux"

# Build step
echo "Phase: Build"
if $src_dir/build.sh -s "$SUFFIX" -i $input_survey; then
    home-ci-reporter step "build" "passed" "Build completed successfully" --file "$TEST_REPORT"
    build=true
else
    home-ci-reporter step "build" "failed" "Build failed" --file "$TEST_REPORT"
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
if $DIR/prereq-install.sh $monitoring_opt; then
    home-ci-reporter step "prereq_install" "passed" "Prerequisites installed successfully" --file "$TEST_REPORT"
else
    home-ci-reporter step "prereq_install" "failed" "Prerequisites installation failed" --file "$TEST_REPORT"
    exit 1
fi

$(ciux get image --check $src_dir --suffix "$SUFFIX" --env)
if [ $CIUX_BUILD = true ]; then
  kind load docker-image $CIUX_IMAGE_URL --name "$cluster"
fi

echo "Phase: ArgoCD Deployment"
echo "Run ArgoCD to install the whole fink e2e tests stack"
if $DIR/argocd.sh -s "$SUFFIX" -S "$storage" $monitoring_opt; then
    home-ci-reporter step "argocd_deploy" "passed" "ArgoCD deployment completed successfully" --file "$TEST_REPORT"
else
    home-ci-reporter step "argocd_deploy" "failed" "ArgoCD deployment failed" --file "$TEST_REPORT"
    exit 1
fi

echo "Phase: Test Results Check"
echo "Check the results of the tests."
if $DIR/check-results.sh -s "$SUFFIX" $monitoring_opt; then
    home-ci-reporter step "test_check" "passed" "Test results check completed successfully" --file "$TEST_REPORT"
    e2e=true
else
    home-ci-reporter step "test_check" "failed" "Test results check failed" --file "$TEST_REPORT"
    exit 1
fi

echo "Phase: Image Push"
echo "Push the image to Container Registry"
if $src_dir/push-image.sh; then
    home-ci-reporter step "image_push" "passed" "Image pushed to registry successfully" --file "$TEST_REPORT"
    push=true
else
    home-ci-reporter step "image_push" "failed" "Image push failed" --file "$TEST_REPORT"
    exit 1
fi

# Finalize the report
home-ci-reporter finalize --file "$TEST_REPORT"

# Get final stats for display
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TOTAL_DURATION=$(( $(date -d "$END_TIME" +%s) - $(date -d "$START_TIME" +%s) ))

echo "=================================="
echo "🎉 FINK E2E TESTS COMPLETED!"
echo "Duration: ${TOTAL_DURATION}s"
echo "Test report: $TEST_REPORT"
echo "=================================="

