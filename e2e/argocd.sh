#!/bin/bash

# Install fink-broker stack (kafka+minio)
# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

infra="ci-noscience"
scheduled="false"
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
  -i <infra>    Target infrastructure: deploy with values-<infra>.yaml
                (e.g. 'cc', 'ci-noscience', 'ci-science'). Default: ci-noscience.
                'ci-*' infras enable the e2e alert simulator; others are
                production-like (e2e disabled). The fink-broker image variant
                (science/noscience) is derived from the infra name.
  -s            Scheduled mode: layer values-scheduled.yaml on top, deploying
                ScheduledSparkApplications that deduce the observing night at
                runtime. Without -s, one-shot SparkApplications with a pinned
                night are deployed. Mutually exclusive with -n.
  -n <night>    One-shot mode only: pin the ZTF night (YYYYMMDD). Defaults to
                the previous night on production infras. Cannot be combined
                with -s (scheduled deduces the night at runtime).
  -m            Enable monitoring.
  -S <storage>  Storage to use (hdfs or s3). Default: hdfs

Examples:
  $(basename "$0") -i cc          # CC-IN2P3, one-shot on the previous night
  $(basename "$0") -i cc -s       # CC-IN2P3, scheduled daily run
EOD
}

# Get the options
while getopts hi:mn:sS: c ; do
    case $c in
        h) usage ; exit 0 ;;
        i) infra="$OPTARG" ;;
        m) monitoring="true" ;;
        n) night="$OPTARG" ;;
        s) scheduled="true" ;;
        S) storage="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

# Scheduled mode deduces the observing night at runtime, so pinning one with -n
# is contradictory.
if [ "$scheduled" == "true" ] && [ -n "$night" ]; then
    echo "Error: -s (scheduled) and -n <night> are mutually exclusive:"
    echo "       scheduled mode deduces the observing night at runtime."
    usage
    exit 1
fi

# Validate night value (YYYYMMDD) when provided
if [ -n "$night" ] && ! [[ "$night" =~ ^[0-9]{8}$ ]]; then
    echo "Error: night must be in YYYYMMDD format"
    usage
    exit 1
fi

# Derive the fink-broker image variant (science/noscience) from the infra name;
# used by ciux to select the image below.
case "$infra" in
    *noscience*) SUFFIX="noscience" ;;
    *)           SUFFIX="science" ;;
esac

# Refresh ciux config if not in github actions
# Used for interactive development
if [ "$GITHUB_ACTIONS" == "false" ]; then
    ciux ignite --selector itest "$src_dir" --suffix "$SUFFIX"
fi

. "$DIR/../.ciux.d/ciux_itest.sh"

NS=argocd

# 'ci-*' infras run the e2e alert simulator; other (production) infras don't.
case "$infra" in
    ci-*) e2e_enabled="true" ;;
    *)    e2e_enabled="false" ;;
esac

# Layer the per-infra values, then the scheduled overlay when requested. Both
# files live in the fink-cd app-of-apps chart (apps/), resolved relative to it.
values_args=(--values "values-${infra}.yaml")
if [ "$scheduled" == "true" ]; then
    values_args+=(--values "values-scheduled.yaml")
fi

# Night handling (one-shot only). Scheduled mode passes no night: the job
# deduces it at runtime. In one-shot mode on a production infra, default to the
# previous night (the most recent complete ZTF observing night; today's topic
# is still filling, and ZTF public topics are only retained ~7 days). CI infras
# keep the night pinned by their values file unless -n overrides it.
night_args=()
if [ "$scheduled" == "false" ]; then
    if [ -z "$night" ] && [ "$e2e_enabled" == "false" ]; then
        night=$(date -u -d 'yesterday' +%Y%m%d)
        echo "No night specified, defaulting to previous night: $night"
    fi
    if [ -n "$night" ]; then
        night_args+=(-p "finkBroker.night=$night")
    fi
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
    "${values_args[@]}" \
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
# their CRDs) are healthy. `argocd app wait -l` returns immediately when its
# selector matches nothing, so guard it: wait until at least one storage app
# exists, then wait for storage health. Probe the label (not a hardcoded app
# name like `kafka`, which is optional via components.kafka), so the guard
# holds whichever storage apps are enabled.
until [ "$(argocd app list -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage -o name | wc -l)" -gt 0 ]; do
    echo "Waiting for storage Applications to be created..."
    sleep 5
done
wait_app -l app.kubernetes.io/part-of=fink,app.kubernetes.io/component=storage

# The Kafka SASL/JAAS secret mounted by the distribution SparkApplication is
# now a declarative resource of the kafka chart (fink-cd), synced in wave 1
# above, so it exists by the time the broker is synced. It used to be created
# imperatively here with `finkctl createsecrets`.

# Deploy the broker/simulator layer (wave 2) now the Kafka secret exists.
argocd app sync -l app.kubernetes.io/part-of=fink
wait_app -l app.kubernetes.io/part-of=fink
