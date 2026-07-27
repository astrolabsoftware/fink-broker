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
revision=""

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
  -r <ref>      Git revision (tag or commit sha) deployed for fink-cd,
                fink-broker and fink-alert-simulator. Default: the work
                branches resolved by ciux (the current branch when it exists
                in the dependency, else its default branch).
                All Applications are auto-synced, so a branch makes the
                running stack follow its HEAD: pin an immutable tag for
                production. The ref must exist in the three repositories.
  -m            Enable monitoring.
  -S <storage>  Storage to use (hdfs or s3). Default: hdfs

Examples:
  $(basename "$0") -i cc          # CC-IN2P3, one-shot on the previous night
  $(basename "$0") -i cc -s       # CC-IN2P3, scheduled daily run
  $(basename "$0") -i cc -s -r v3.3.0   # ... pinned on a release tag
EOD
}

# Get the options
while getopts hi:mn:r:sS: c ; do
    case $c in
        h) usage ; exit 0 ;;
        i) infra="$OPTARG" ;;
        m) monitoring="true" ;;
        n) night="$OPTARG" ;;
        r) revision="$OPTARG" ;;
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

# Revisions deployed for the three source repositories. ciux resolves a work
# branch per repository (used by CI and interactive development); -r overrides
# all of them with a single ref. Every Application is auto-synced, so a branch
# means "follow its HEAD": production runs must pin an immutable tag.
fink_cd_rev="$FINK_CD_WORKBRANCH"
fink_broker_rev="$FINK_BROKER_WORKBRANCH"
fink_alert_simulator_rev="$FINK_ALERT_SIMULATOR_WORKBRANCH"
if [ -n "$revision" ]; then
    # Fail loudly here rather than let Argo CD loop on an unresolvable
    # revision: a partially tagged release is the expected mistake. Commit
    # shas are not advertised by ls-remote, so they skip this check.
    if ! [[ "$revision" =~ ^[0-9a-f]{7,40}$ ]]; then
        for repo in fink-cd fink-broker fink-alert-simulator; do
            if ! git ls-remote --exit-code "https://github.com/astrolabsoftware/$repo.git" \
                    "refs/tags/$revision" "refs/heads/$revision" > /dev/null 2>&1; then
                echo "Error: revision '$revision' not found in astrolabsoftware/$repo"
                exit 1
            fi
        done
    fi
    fink_cd_rev="$revision"
    fink_broker_rev="$revision"
    fink_alert_simulator_rev="$revision"
fi

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
    --path apps --revision "$fink_cd_rev" \
    "${values_args[@]}" \
    -p storage="$storage" \
    ${night_args[@]+"${night_args[@]}"} \
    -p finkBroker.image.repository="$CIUX_IMAGE_REGISTRY" \
    -p finkBroker.image.tag="$CIUX_IMAGE_TAG" \
    -p finkBroker.monitoring.enabled="$monitoring" \
    -p finkAlertSimulator.image.tag="$FINK_ALERT_SIMULATOR_VERSION" \
    -p spec.source.targetRevision.default="$fink_cd_rev" \
    -p spec.source.targetRevision.finkbroker="$fink_broker_rev" \
    -p spec.source.targetRevision.finkalertsimulator="$fink_alert_simulator_rev" \
    --upsert # Added to avoid error if app already exists

# Robust wait: let the sync operation finish, give workloads ~10s to start
# (and crash if they will), then wait for real health. A lone --health wait
# can pass on a transient Healthy before a Spark driver starts crash-looping.
wait_app() {
    argocd app wait --operation "$@" --timeout 600
    sleep 10
    argocd app wait --health "$@" --timeout 600
}

# Roll out the whole stack: operators (wave 0) -> storage (wave 1) ->
# broker/simulator (wave 2). Every child Application is auto-synced, so Argo CD
# gates each wave on the health of the previous one; a synchronous sync of the
# app-of-apps is enough to order the rollout. The Kafka SASL/JAAS secret
# mounted by the distribution SparkApplication is a declarative resource of the
# kafka chart (wave 1), so it exists before the broker starts -- it used to be
# created imperatively here with `finkctl createsecrets`, which is why the
# broker layer was synced separately. The sync covers the three waves end to
# end (image pulls included on a cold cluster), hence a timeout well above the
# per-app ones above.
argocd app sync fink --timeout 1800
wait_app -l app.kubernetes.io/part-of=fink
