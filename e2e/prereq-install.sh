#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

kind_version_opt=""
monitoring=false
src_dir=$DIR/..
# Check if running in github actions
GITHUB_ACTIONS=${GITHUB_ACTIONS:-false}

usage () {
    echo "Usage: $0 [-k kind_version] [-m]"
    echo "  -k kind_version: kind version to install"
    echo "  -m: install monitoring stack"
    exit 1
}

# Get kind version from option -k
while getopts mk:h flag
do
    case "${flag}" in
        k) kind_version_opt=--kind-version=${OPTARG};;
        m) monitoring=true;;
        h) usage;;
        *) usage; exit 1;;
    esac
done

# Install ktbx
if [ $GITHUB_ACTIONS == false ]; then
  ciux ignite --selector itest "$src_dir"
fi


ktbx install kind $kind_version_opt
ktbx install kubectl
ktbx install helm

# Check if a kubernetes context is already set for a non-kind cluster, otherwise create a kind cluster
context=$(kubectl config current-context 2>/dev/null) || context=""
if [ -z "$context" ] || [[ $context == kind-* ]]; then
    ink "Creating kind cluster"
    cluster_name=$(ciux get clustername $DIR/..)
    ktbx create -s --name $cluster_name
else
    ink "kubectl context found: $context"
fi
ink "Install OLM"
ktbx install olm
ink "Install ArgoCD operator"
ktbx install argocd
if [ "$monitoring" = true ]; then
    ink "Install prometheus monitoring stack"
    ktbx install prometheus
fi

