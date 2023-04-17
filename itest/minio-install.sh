#!/bin/bash

# Install MinIO inside k8s
# Based on https://min.io/docs/minio/kubernetes/upstream/index.html

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

NS="minio-dev"

if kubectl apply -f $DIR/manifests/minio.yaml
then
    echo "Deploy minio"
else
    >&2 echo "ERROR: No able to deploy minio"
    exit 1
fi

kubectl rollout status deployment minio -n "$NS" --timeout=90s