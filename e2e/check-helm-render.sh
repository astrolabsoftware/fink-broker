#!/bin/bash

# Render the fink-cd app-of-apps and the fink-broker chart with helm template
# and check which components each storage/HDFS/Kafka combination deploys.
# No cluster needed: this is the only CI-side guard for the switches whose
# real target (an HDFS outside the cluster) is not reachable from a runner.
#
# FINK_CD_DIR: local clone of fink-cd. Default: the fink-cd branch named like
# the current fink-broker branch when it exists, else its default branch,
# cloned in a temporary directory (same resolution as ciux in e2e/argocd.sh).

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
src_dir=$DIR/..

fink_cd_url="https://github.com/astrolabsoftware/fink-cd.git"
if [ -z "${FINK_CD_DIR:-}" ]; then
    branch=$(git -C "$src_dir" branch --show-current)
    if ! git ls-remote --exit-code "$fink_cd_url" "refs/heads/$branch" > /dev/null 2>&1; then
        branch=$(git ls-remote --symref "$fink_cd_url" HEAD | sed -n 's|^ref: refs/heads/\(.*\)\tHEAD$|\1|p')
    fi
    FINK_CD_DIR=$(mktemp -d)
    trap 'rm -rf "$FINK_CD_DIR"' EXIT
    git clone --quiet --depth 1 --branch "$branch" "$fink_cd_url" "$FINK_CD_DIR"
fi
echo "Using fink-cd at $FINK_CD_DIR ($(git -C "$FINK_CD_DIR" rev-parse --short HEAD))"

failures=0

# Names of the rendered resources of a kind, one per line.
names() {
    local kind="$1"
    yq -r "select(.kind == \"$kind\") | .metadata.name"
}

# assert <label> <expected names, space separated> <actual names, one per line>
# The yq flavours differ (mikefarah prints a '---' between documents): both
# are supported, so the script runs on a dev host as on a GitHub runner.
assert() {
    local label="$1" expected="$2" actual
    actual=$(echo "$3" | grep -v '^---$' | sort | xargs)
    expected=$(echo "$expected" | tr ' ' '\n' | sort | xargs)
    if [ "$actual" == "$expected" ]; then
        echo "ok   $label"
    else
        echo "FAIL $label"
        echo "     expected: [$expected]"
        echo "     actual:   [$actual]"
        failures=$((failures + 1))
    fi
}

# --- fink-cd app-of-apps ---------------------------------------------------

apps() {
    helm template fink "$FINK_CD_DIR/apps" "$@"
}
operators="zookeeper-operator hdfs-operator commons-operator secret-operator listener-operator"
stackable="$operators hdfs"
common="fink-broker spark-operator strimzi"
incluster_prefix="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs:8020///user/185"

# The valuesObject handed to the fink-broker chart, as YAML.
broker_values() {
    yq -r 'select(.kind == "Application" and .metadata.name == "fink-broker") | .spec.source.helm.valuesObject'
}

echo "== fink-cd: CI defaults (in-cluster HDFS, kafka, simulator)"
out=$(apps)
assert "applications" "$stackable $common kafka fink-alert-simulator" "$(echo "$out" | names Application)"
assert "hdfs.external forwarded" "false" "$(echo "$out" | broker_values | yq -r '.hdfs.external')"
assert "in-cluster prefix" "$incluster_prefix" "$(echo "$out" | broker_values | yq -r '.online_data_prefix')"

echo "== fink-cd: values-cc.yaml (external HDFS)"
out=$(apps -f "$FINK_CD_DIR/apps/values-cc.yaml")
assert "applications" "$common kafka" "$(echo "$out" | names Application)"
assert "hdfs.external forwarded" "true" "$(echo "$out" | broker_values | yq -r '.hdfs.external')"
assert "external prefix" "hdfs://ccmaster1:8020///user/185" \
    "$(echo "$out" | broker_values | yq -r '.online_data_prefix')"

echo "== fink-cd: values-cc.yaml with hdfs.external=false (back to in-cluster HDFS)"
out=$(apps -f "$FINK_CD_DIR/apps/values-cc.yaml" --set hdfs.external=false \
    --set hdfs.onlineDataPrefix="$incluster_prefix")
assert "applications" "$stackable $common kafka" "$(echo "$out" | names Application)"
assert "in-cluster prefix" "$incluster_prefix" "$(echo "$out" | broker_values | yq -r '.online_data_prefix')"
assert "datanodes sized for CC" "3" \
    "$(echo "$out" | yq -r 'select(.kind == "Application" and .metadata.name == "hdfs") | .spec.source.helm.valuesObject.dataNode.replicas')"

echo "== fink-cd: values-cc.yaml without kafka"
out=$(apps -f "$FINK_CD_DIR/apps/values-cc.yaml" --set components.kafka=false)
assert "applications" "fink-broker spark-operator" "$(echo "$out" | names Application)"

echo "== fink-cd: storage=s3"
out=$(apps --set storage=s3)
assert "applications" "$common kafka fink-alert-simulator minio-operator minio-tenant" "$(echo "$out" | names Application)"

# --- fink-broker chart -----------------------------------------------------

chart() {
    helm template fink-broker "$src_dir/chart" --set report.enabled=true "$@"
}

# Resources tied to the in-cluster HDFS stack: the pre-install Job creating
# /user/185 through the Stackable namenode, and the report which reads the
# datasets by exec-ing into the namenode pod (RBAC in the hdfs namespace).
hdfs_bound() {
    echo "$1" | yq -r 'select(.kind == "Job" or .kind == "CronJob" or .metadata.name == "fink-report") | .kind + "/" + .metadata.name'
}

echo "== fink-broker: in-cluster HDFS"
out=$(chart)
assert "hdfs-bound resources" "Job/hdfs-init CronJob/fink-broker-report ServiceAccount/fink-report Role/fink-report Role/fink-report RoleBinding/fink-report RoleBinding/fink-report" "$(hdfs_bound "$out")"

echo "== fink-broker: external HDFS"
out=$(chart --set hdfs.external=true)
assert "hdfs-bound resources" "" "$(hdfs_bound "$out")"
assert "spark applications" "fink-broker-stream2raw fink-broker-raw2science fink-broker-distribution" \
    "$(echo "$out" | names SparkApplication)"
assert "SPARK_USER kept" "185 185 185 185 185 185" \
    "$(echo "$out" | yq -r 'select(.kind == "SparkApplication") | (.spec.driver.env[], .spec.executor.env[]) | select(.name == "SPARK_USER") | .value')"

echo "== fink-broker: storage=s3"
out=$(chart --set storage=s3)
assert "hdfs-bound resources" "" "$(hdfs_bound "$out")"

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed"
    exit 1
fi
echo "All checks passed"
