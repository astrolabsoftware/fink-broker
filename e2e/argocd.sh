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
night=""

GITHUB_ACTIONS=${GITHUB_ACTIONS:-false}

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -c            Deploy with CC-IN2P3 setup (uses values-cc.yaml)
  -n <night>    ZTF night YYYYMMDD. With -c, defaults to the previous night
  -s <suffix>   Specify suffix ('noscience' or 'science'). Default: noscience
  -S <storage>  Storage to use (hdfs or s3)
EOD
}

# Get the options
# -s has no effect in GIHUB_ACTION mode
while getopts hcmn:S:s: c ; do
    case $c in
        h) usage ; exit 0 ;;
        c) cc="true" ;;
        m) monitoring="true" ;;
        n) night="$OPTARG" ;;
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

# Validate night value (YYYYMMDD) when provided
if [ -n "$night" ] && ! [[ "$night" =~ ^[0-9]{8}$ ]]; then
    echo "Error: night must be in YYYYMMDD format"
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
    values_file="values-cc.yaml"
    e2e_enabled="false"
    # Default to the previous night: the most recent complete ZTF observing
    # night (today's topic is still being filled). ZTF public topics are only
    # retained ~7 days, so a stale hard-coded night would already be gone.
    if [ -z "$night" ]; then
        night=$(date -u -d 'yesterday' +%Y%m%d)
        echo "No night specified, defaulting to previous night: $night"
    fi
else
    values_file="values-ci-${SUFFIX}.yaml"
    e2e_enabled="true"
fi

# Override the night only when set. The e2e values files pin their own night
# for the simulated stream, so leave them untouched unless -n was given.
night_args=()
if [ -n "$night" ]; then
    night_args+=(-p "finkBroker.night=$night")
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
    --values "$values_file" \
    -p storage="$storage" \
    ${night_args[@]+"${night_args[@]}"} \
    -p finkBroker.image.repository="$CIUX_IMAGE_REGISTRY" \
    -p finkBroker.image.tag="$CIUX_IMAGE_TAG" \
    -p finkBroker.monitoring.enabled="$monitoring" \
    -p finkAlertSimulator.image.tag="$FINK_ALERT_SIMULATOR_VERSION" \
    -p spec.source.targetRevision.default="$FINK_CD_WORKBRANCH" \
    -p spec.source.targetRevision.finkbroker="$FINK_BROKER_WORKBRANCH" \
    -p spec.source.targetRevision.finkalertsimulator="$FINK_ALERT_SIMULATOR_WORKBRANCH" \
    --upsert # Added to avoid error if app already exists

# Robust wait: let the sync operation finish, give workloads ~10s to start
# (and crash if they will), then wait for real health. A lone --health wait
# can pass on a transient Healthy before a Spark driver starts crash-looping.
wait_app() {
    argocd app wait --operation "$@" --timeout 600
    sleep 10
    argocd app wait --health "$@" --timeout 600
}

# Roll out operators (wave 0) + storage (wave 1) via sync-waves. Async so the
# Kafka secret can be created before the broker (wave 2) is synced below.
argocd app sync fink --async

# Storage Applications are created asynchronously, once the operators (and
# their CRDs) are healthy. Wait for them to exist, then for storage health.
until argocd app get kafka >/dev/null 2>&1; do
    echo "Waiting for storage Applications to be created..."
    sleep 5
done
wait_app -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage

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

# Deploy the broker/simulator layer (wave 2) now the Kafka secret exists.
argocd app sync -l app.kubernetes.io/part-of=fink
wait_app -l app.kubernetes.io/part-of=fink
