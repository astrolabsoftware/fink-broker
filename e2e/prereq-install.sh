#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

ktbx install kind
ktbx install kubectl
echo "Create kind cluster"
ktbx create -s
echo "Install OLM"
ktbx install olm
echo "Install ArgoCD operator"
ktbx install argocd

