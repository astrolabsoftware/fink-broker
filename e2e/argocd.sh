#!/bin/bash

# Install fink-broker stack (kafka+minio)
# Based on https://min.io/docs/minio/kubernetes/upstream/index.html

# @author  Fabrice Jammes

set -euxo pipefail

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
    --path apps --revision "$FINK_CD_WORKBRANCH" \

argocd app set fink -p fink-broker.image.repository="$CIUX_IMAGE_REGISTRY" \
    -p fink-broker.image.name="$CIUX_IMAGE_NAME" \
    -p fink-broker.image.tag="$CIUX_IMAGE_TAG"

# Sync fink app-of-apps
argocd app sync fink

# Synk operators dependency for fink
argocd app sync strimzi spark-operator

# TODO Try to make it simpler, try a sync-wave on Strimzi Application?
# see https://github.com/argoproj/argo-cd/discussions/16729
# and https://stackoverflow.com/questions/77750481/argocd-app-of-apps-ensuring-strimzi-child-app-health-before-kafka-app-sync
retry kubectl wait --for condition=established --timeout=60s crd/kafkas.kafka.strimzi.io \
  crd/kafkatopics.kafka.strimzi.io \
  crd/tenants.minio.min.io \
  crd/sparkapplications.sparkoperator.k8s.io
argocd app sync -l app.kubernetes.io/instance=fink
