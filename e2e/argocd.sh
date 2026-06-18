#!/bin/bash

# Install fink-broker stack (kafka+minio)
# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
SUFFIX="noscience"

cc="false"
monitoring="false"
src_dir=$DIR/..
storage="hdfs"

GITHUB_ACTIONS=${GITHUB_ACTIONS:-false}

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -c            Deploy with CC-IN2P3 setup (uses values-cc.yaml)
  -s <suffix>   Specify suffix ('noscience' or 'science'). Default: noscience
  -S <storage>  Storage to use (hdfs or minio)
EOD
}

# Get the options
# -s has no effect in GIHUB_ACTION mode
while getopts hcmS:s: c ; do
    case $c in
        h) usage ; exit 0 ;;
        c) cc="true" ;;
        m) monitoring="true" ;;
        S) storage="$OPTARG" ;;
        s) SUFFIX="${OPTARG:-science}" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

# Validate suffix value
if [ -n "$SUFFIX" ] && [ "$SUFFIX" != "noscience" ] && [ "$SUFFIX" != "science" ]; then
    echo "Error: suffix must be 'noscience' or 'science'"
    usage
    exit 1
fi

# Refresh ciux config if not in github actions
# Used for interactive development
if [ "$GITHUB_ACTIONS" == "false" ]; then
    ciux ignite --selector itest "$src_dir" --suffix "$SUFFIX"
fi

. "$DIR/../.ciux.d/ciux_itest.sh"

NS=argocd

if [ "$cc" == "true" ]; then
    ci_values_file="values-cc.yaml"
    e2e_enabled="false"
else
    ci_values_file="values-ci-${SUFFIX}.yaml"
    e2e_enabled="true"
fi

# --- CONFIGURATION WITHOUT TUNNEL ---
# Force the use of local K8s context.
# No need for 'argocd login' with password.
export ARGOCD_OPTS="--core --namespace $NS"
kubectl config set-context --current --namespace="$NS"

echo "Use fink-broker image: $CIUX_IMAGE_URL"

# Create fink app-of-apps with all configuration (Note: --core is implicit via ARGOCD_OPTS)
argocd app create fink --dest-server https://kubernetes.default.svc \
    --dest-namespace "$NS" \
    --repo https://github.com/astrolabsoftware/fink-cd.git \
    --path apps --revision "$FINK_CD_WORKBRANCH" \
    --values "$ci_values_file" \
    -p storage="$storage" \
    -p finkBroker.image.repository="$CIUX_IMAGE_REGISTRY" \
    -p finkBroker.image.tag="$CIUX_IMAGE_TAG" \
    -p finkBroker.monitoring.enabled="$monitoring" \
    -p finkAlertSimulator.image.tag="$FINK_ALERT_SIMULATOR_VERSION" \
    -p spec.source.targetRevision.default="$FINK_CD_WORKBRANCH" \
    -p spec.source.targetRevision.finkbroker="$FINK_BROKER_WORKBRANCH" \
    -p spec.source.targetRevision.finkalertsimulator="$FINK_ALERT_SIMULATOR_WORKBRANCH" \
    --upsert # Added to avoid error if app already exists

# Trigger the wave-ordered rollout of the app-of-apps.
# Operator (wave 0) and storage (wave 1) Applications are auto-synced, and
# sync-waves guarantee each operator is healthy (its CRDs established) before
# the matching storage custom resources (Kafka/HDFS/MinIO) are applied.
# Run async: the broker (wave 2) is synced manually further down, once the
# Kafka secret exists.
argocd app sync fink --async

# Wait for the storage Applications to be created by the app-of-apps. By the
# time they exist, sync-waves guarantee the operators are already healthy.
until [ -n "$(argocd app list -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage -o name 2>/dev/null)" ]; do
    echo "Waiting for storage Applications to be created by the app-of-apps..."
    sleep 5
done

# Wait for storage to be healthy before creating the e2e Kafka secret.
argocd app wait -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage --health --timeout 600

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

# Deploy the broker/simulator layer (wave 2) now that the Kafka secret exists,
# and wait for the whole stack to converge.
argocd app sync -l app.kubernetes.io/part-of="fink"
argocd app wait -l app.kubernetes.io/part-of="fink" --health --timeout 900
