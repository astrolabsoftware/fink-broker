#!/bin/bash

# Run fink-broker e2e tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage () {
  echo "Usage: $0 [-s]"
  echo "  -s: Use the science algorithms during the tests"
  exit 1
}

suffix="noscience"

# Get options for suffix
while getopts hs opt; do
  case ${opt} in
    s )
      suffix=""
      ;;
    h )
      echo "Usage: cmd [-s]"
      exit 0
      ;;
    \? )
      echo "Usage: cmd [-s]"
      exit 1
      ;;
  esac
done

export suffix
export CIUXCONFIG=$HOME/.ciux/ciux.sh

echo "Update source code"
cd $DIR/..
git pull

ciux_version=v0.0.4-rc6
go install github.com/k8s-school/ciux@"$ciux_version"

echo "Ignite the project using `ciux`."
mkdir -p ~/.ciux

ciux ignite --selector itest $PWD --suffix "$suffix"

cluster_name="$USER-$(git rev-parse --abbrev-ref HEAD)"
echo "Delete the cluster $cluster_name if it already exists"
ktbx delete --name "$cluster_name" || true

echo "Create a Kubernetes cluster (Kind), Install OLM and ArgoCD operators."
$DIR/e2e/prereq-install.sh

echo "Run ArgoCD to install the whole fink e2e tests stack"
$DIR/e2e/argocd.sh

echo "Check the results of the tests."
$DIR/e2e/check-results.sh
