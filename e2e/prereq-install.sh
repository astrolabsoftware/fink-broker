#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

kind_version_opt=""

# TODO manage cluster name inside ktbx
USER=${USER:-fink-ci}
cluster_name="$USER-$(git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]')"

# Get kind version from option -k
while getopts k: flag
do
    case "${flag}" in
        k) kind_version_opt=--kind-version=${OPTARG};;
    esac
done

ktbx install kind $kind_version_opt
ktbx install kubectl
ktbx install helm
ink "Create kind cluster"
ktbx create -s --name $cluster_name
ink "Install OLM"
ktbx install olm
ink "Install ArgoCD operator"
ktbx install argocd

