#!/bin/bash

# Install fink-broker stack (kafka+minio)
# Based on https://min.io/docs/minio/kubernetes/upstream/index.html

# @author  Fabrice Jammes

set -euxo pipefail

CIUXCONFIG=${CIUXCONFIG:-"$HOME/.ciuxconfig"}
echo "CIUXCONFIG=${CIUXCONFIG}"
. $CIUXCONFIG

function retry {
  local n=1
  local max=5
  local delay=5
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay;
      else
        echo "The command has failed after $n attempts." >&2
        exit 1
      fi
    }
  done
}

NS=argocd

argocd login --core
kubectl config set-context --current --namespace="$NS"

# Create fink app
argocd app create fink --dest-server https://kubernetes.default.svc \
    --dest-namespace "$NS" \
    --repo https://github.com/astrolabsoftware/fink-cd.git \
    --path apps --revision "$FINK_CD_WORKBRANCH"

# Sync fink app-of-apps
argocd app sync fink

# Synk operators dependency for fink
argocd app sync strimzi minio-operator spark-operator

# TODO Try to make it simpler, try a sync-wave on Strimzi Application?
# see https://github.com/argoproj/argo-cd/discussions/16729
# and https://stackoverflow.com/questions/77750481/argocd-app-of-apps-ensuring-strimzi-child-app-health-before-kafka-app-sync
retry kubectl wait --for condition=established --timeout=60s crd/kafkas.kafka.strimzi.io \
  crd/kafkatopics.kafka.strimzi.io \
  crd/tenants.minio.min.io \
  crd/sparkapplications.sparkoperator.k8s.io \
  crd/workflows.argoproj.io

# TODO Wait for all applications to be synced (problem with spark-operator secret)

# Set fink-broker parameters
argocd app set fink-broker -p image.repository="$CIUX_IMAGE_REGISTRY" \
    -p image.name="$CIUX_IMAGE_NAME" \
    -p image.tag="$CIUX_IMAGE_TAG" \
    -p night="20200101"
argocd app sync -l app.kubernetes.io/instance=fink

# TODO Wait for kafkatopic to exist
retry kubectl wait --for condition=ready kafkatopics -n kafka  ztf-stream-sim
