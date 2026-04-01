#!/bin/bash

# Install fink-broker stack (kafka+minio)
# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
SUFFIX="noscience"

monitoring="false"
src_dir=$DIR/..
storage="hdfs"

GITHUB_ACTIONS=${GITHUB_ACTIONS:-false}

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -s <suffix>   Suffix to use for the image (default: noscience)
  -S <storage>  Storage to use (hdfs or minio)
EOD
}

while getopts hmS:s: c ; do
    case $c in
        h) usage ; exit 0 ;;
        m) monitoring="true" ;;
        S) storage="$OPTARG" ;;
        s) SUFFIX="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ "$GITHUB_ACTIONS" == "false" ]; then
    ciux ignite --selector itest "$src_dir" --suffix "$SUFFIX"
fi

. "$DIR/../.ciux.d/ciux_itest.sh"

NS=argocd
e2e_enabled="true"

# --- CONFIGURATION WITHOUT TUNNEL ---
# Force the use of local K8s context.
# No need for 'argocd login' with password.
export ARGOCD_OPTS="--core --namespace $NS"
kubectl config set-context --current --namespace="$NS"

if [ "$storage" == "s3" ]; then
    hdfs_enabled="false"
    s3_enabled="true"
    online_data_prefix=""
elif [ "$storage" == "hdfs" ]; then
    hdfs_enabled="true"
    s3_enabled="false"
    online_data_prefix="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020///user/185"
fi

# Create fink app (Note: --core is implicit via ARGOCD_OPTS)
argocd app create fink --dest-server https://kubernetes.default.svc \
    --dest-namespace "$NS" \
    --repo https://github.com/astrolabsoftware/fink-cd.git \
    --path apps --revision "$FINK_CD_WORKBRANCH" \
    -p s3.enabled="$s3_enabled" \
    -p hdfs.enabled="$hdfs_enabled" \
    -p spec.source.targetRevision.default="$FINK_CD_WORKBRANCH" \
    -p spec.source.targetRevision.finkbroker="$FINK_BROKER_WORKBRANCH" \
    -p spec.source.targetRevision.finkalertsimulator="$FINK_ALERT_SIMULATOR_WORKBRANCH" \
    --upsert # Added to avoid error if app already exists

# Sync fink app-of-apps
argocd app sync fink

# Set fink-broker parameters
echo "Use fink-broker image: $CIUX_IMAGE_URL"
[[ "$CIUX_IMAGE_URL" =~ "-noscience" ]] && valueFile=values-ci-noscience.yaml || valueFile=values-ci-science.yaml

argocd app set fink-broker -p image.repository="$CIUX_IMAGE_REGISTRY" \
    --values "$valueFile" \
    -p e2e.enabled="$e2e_enabled" \
    -p monitoring.enabled="$monitoring" \
    -p image.tag="$CIUX_IMAGE_TAG" \
    -p log_level="DEBUG" \
    -p night="20200101" \
    -p online_data_prefix="$online_data_prefix" \
    -p storage="$storage"

argocd app set fink-alert-simulator -p image.tag="$FINK_ALERT_SIMULATOR_VERSION"

# --- OPERATORS SYNC (Fix for previous RPC issue) ---
# Using --core eliminates 'connection refused' error on 127.0.0.1
argocd app sync -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=operator
argocd app wait -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=operator --timeout 600

# Sync storage
argocd app sync -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage
argocd app wait --operation -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage --timeout 600
sleep 10
argocd app wait --health -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage --timeout 600

if [ "$e2e_enabled" == "true" ]; then
    echo "Retrieve kafka secrets for e2e tests"
    # Use kubectl directly for waiting (more reliable than shell polling)
    kubectl wait --namespace kafka --for=condition=Ready --timeout=300s pod -l app.kubernetes.io/name=kafka || true
    
    until kubectl get secret fink-producer --namespace kafka; do
        echo "Waiting for secret/fink-producer in ns kafka"
        sleep 5
    done
    
    # Switch context for finkctl
    kubectl config set-context --current --namespace="spark"
    finkctl createsecrets
    kubectl config set-context --current --namespace="$NS"
fi

# Final sync
argocd app sync -l app.kubernetes.io/part-of="fink"
argocd app wait --operation -l app.kubernetes.io/part-of="fink" --timeout 600
sleep 10
argocd app wait --health -l app.kubernetes.io/part-of="fink" --timeout 600
