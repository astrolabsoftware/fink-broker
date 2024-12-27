#!/bin/bash

# Install fink-broker stack (kafka+minio)
# Based on https://min.io/docs/minio/kubernetes/upstream/index.html

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

storage="hdfs"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -S <storage>  Storage to use (hdfs or minio)
EOD
}

# Get the options
while getopts hS: c ; do
    case $c in
        h) usage ; exit 0 ;;
        S) storage="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

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
e2e_enabled="true"

argocd login --core
kubectl config set-context --current --namespace="$NS"

if [ $storage == "s3" ]
then
  hdfs_enabled="false"
  s3_enabled="true"
  online_data_prefix=""
elif [ $storage == "hdfs" ]
then
  hdfs_enabled="true"
  s3_enabled="false"
  online_data_prefix="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020///user/185"
fi


# Create fink app
argocd app create fink --dest-server https://kubernetes.default.svc \
    --dest-namespace "$NS" \
    --repo https://github.com/astrolabsoftware/fink-cd.git \
    --path apps --revision "$FINK_CD_WORKBRANCH" \
    -p s3.enabled="$s3_enabled" \
    -p hdfs.enabled="$hdfs_enabled" \
    -p spec.source.targetRevision.default="$FINK_CD_WORKBRANCH" \
    -p spec.source.targetRevision.finkbroker="$FINK_BROKER_WORKBRANCH" \
    -p spec.source.targetRevision.finkalertsimulator="$FINK_ALERT_SIMULATOR_WORKBRANCH"

# Sync fink app-of-apps
argocd app sync fink

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
    -p e2e.enabled="$e2e_enabled" \
    -p image.tag="$CIUX_IMAGE_TAG" \
    -p log_level="DEBUG" \
    -p night="20200101" \
    -p online_data_prefix="$online_data_prefix" \
    -p storage="$storage"

argocd app set fink-alert-simulator -p image.tag="$FINK_ALERT_SIMULATOR_VERSION"

# Synk operators dependency for fink
argocd app sync -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=operator
argocd app wait -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=operator

# Synk storage dependency for fink
argocd app sync -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage
argocd app wait -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage

# Sync fink-broker
argocd app sync -l app.kubernetes.io/instance=fink

if [ $e2e_enabled == "true" ]
then
  echo "Retrieve kafka secrets for e2e tests"
  while ! kubectl get secret fink-producer --namespace kafka
  do
    echo "Waiting for secret/fink-producer in ns kafka"
    sleep 10
  done
  kubectl config set-context --current --namespace="spark"
  finkctl createsecrets
  kubectl config set-context --current --namespace="argocd"
fi

argocd app wait -l app.kubernetes.io/instance=fink
