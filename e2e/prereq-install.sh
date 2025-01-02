#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

kind_version_opt=""
monitoring=false

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

ktbx install kind $kind_version_opt
ktbx install kubectl
ktbx install helm
ink "Create kind cluster"
cluster_name=$(ciux get clustername $DIR/..)
ktbx create -s --name $cluster_name
ink "Install OLM"
ktbx install olm
ink "Install ArgoCD operator"
ktbx install argocd
if [ "$monitoring" = true ]; then
    ink "Install prometheus monitoring stack"
    ktbx install prometheus
fi

