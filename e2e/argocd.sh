#!/bin/bash

# Install fink-broker stack (kafka+minio)
# Based on https://min.io/docs/minio/kubernetes/upstream/index.html

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

CIUXCONFIG=${CIUXCONFIG:-"$HOME/.ciux/ciux.sh"}
. $CIUXCONFIG

function retry {
  local n=1
  local max=10
  local delay=15
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
    -p finkbroker.revision="$FINK_BROKER_WORKBRANCH"


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
echo "Use fink-broker image: $CIUX_IMAGE_URL"
if [[ "$CIUX_IMAGE_URL" =~ "-noscience" ]];
then
  valueFile=values-ci-noscience.yaml
else
  valueFile=values-ci-science.yaml
fi
argocd app set fink-broker -p image.repository="$CIUX_IMAGE_REGISTRY" \
    --values "$valueFile" \
    -p image.name="$CIUX_IMAGE_NAME" \
    -p image.tag="$CIUX_IMAGE_TAG" \
    -p log_level="DEBUG" \
    -p night="20200101"
# TODO pass parameters using a valuefile here, and not in 'argocd app create fink'
# see https://argo-cd.readthedocs.io/en/stable/user-guide/commands/argocd_app_set/

argocd app sync -l app.kubernetes.io/instance=fink

kafka_topic="ztf-stream-sim"
echo "Wait for kafkatopic $kafka_topic to exist"
retry kubectl wait --for condition=ready kafkatopics -n kafka  "$kafka_topic"

# Check if kafka namespace exists,
# if yes, it means that the e2e tests are running
if kubectl get namespace kafka; then
  echo "Retrieve kafka secrets for e2e tests"
  if [[ "$CIUX_IMAGE_URL" =~ "-noscience" ]];
  then
    FINKCONFIG="$DIR/finkconfig_noscience"
  else
    FINKCONFIG="$DIR/finkconfig"
  fi
  while ! kubectl get secret fink-producer --namespace kafka
  do
    echo "Waiting for secret/fink-producer in ns kafka"
    sleep 10
  done
  kubectl config set-context --current --namespace="spark"
  finkctl createsecrets
fi
