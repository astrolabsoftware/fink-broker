#!/bin/bash

# Run fink-broker e2e tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage () {
  echo "Usage: $0 [-s]"
  echo "  -s: Use the science algorithms during the tests"
  echo "  -c: Cleanup the cluster after the tests"
  exit 1
}

SUFFIX="noscience"

ciux_version=v0.0.4-rc8
export CIUXCONFIG=$HOME/.ciux/ciux.sh

src_dir=$DIR/..
cleanup=false
build=false
e2e=false
push=false

token="${TOKEN:-}"

# Get options for suffix
while getopts hcs opt; do
  case ${opt} in
    s )
      SUFFIX=""
      ;;
    c )
      cleanup=true
      ;;
    h )
      usage
      exit 0
      ;;
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
mkdir -p ~/.ciux

# Build step
$src_dir/build.sh -s "$SUFFIX"
build=true

# e2e tests step
ciux ignite --selector itest "$src_dir" --suffix "$SUFFIX"

cluster=$(ciux get clustername "$src_dir")
echo "Delete the cluster $cluster if it already exists"
ktbx delete --name "$cluster" || true

echo "Create a Kubernetes cluster (Kind), Install OLM and ArgoCD operators."
$DIR/prereq-install.sh

. $CIUXCONFIG
if [ $CIUX_BUILD = true ]; then
  kind load docker-image $CIUX_IMAGE_URL --name "$cluster"
fi

echo "Run ArgoCD to install the whole fink e2e tests stack"
$DIR/argocd.sh

echo "Check the results of the tests."
$DIR/check-results.sh
e2e=true

echo "Push the image to Container Registry"
$src_dir/push-image.sh
push=true

