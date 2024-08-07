#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

kind_version_opt=""

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
ktbx create -s
ink "Install OLM"
ktbx install olm
ink "Install ArgoCD operator"
ktbx install argocd

