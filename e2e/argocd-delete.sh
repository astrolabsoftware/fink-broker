#!/bin/bash

# Tear down the fink-broker stack: delete the ArgoCD app-of-apps and the
# storage PV/PVCs left behind by the Stackable/Strimzi StatefulSets.
#
# ArgoCD's cascade delete only removes resources it tracks from Git. The HDFS
# and Kafka PVCs come from StatefulSet volumeClaimTemplates created by the
# operators, so ArgoCD never sees them and Kubernetes does not delete them with
# the StatefulSet. We therefore delete them explicitly with kubectl.
#
# WARNING: the storage class is csi-cinder-sc-delete (reclaimPolicy=Delete), so
# deleting the PVCs destroys the underlying Cinder volumes and all HDFS/Kafka
# data. This is irreversible.
#
# @author  Fabrice Jammes

set -euxo pipefail

NS=argocd
APP="fink"
# Namespaces holding the storage PVCs created by the operators.
STORAGE_NS=("hdfs" "kafka")

# --- CONFIGURATION WITHOUT TUNNEL ---
# Force the use of local K8s context (no 'argocd login' needed).
export ARGOCD_OPTS="--core --namespace $NS"
kubectl config set-context --current --namespace="$NS"

# 1. Delete the app-of-apps. Cascade (default) removes the child Applications
#    and every resource ArgoCD tracks. foreground propagation makes the command
#    block until the cascade finishes, so the PVC cleanup below is not racing
#    against an Auto-Prune sync recreating the StatefulSets.
if argocd app get "$APP" >/dev/null 2>&1; then
    argocd app delete "$APP" --cascade --propagation-policy foreground -y
else
    echo "ArgoCD app '$APP' not found, skipping app delete"
fi

# Wait for the app to actually disappear before touching the PVCs.
until ! argocd app get "$APP" >/dev/null 2>&1; do
    echo "Waiting for app '$APP' to be fully deleted..."
    sleep 5
done

# 2. Delete the storage PVCs left behind by the StatefulSets. With
#    reclaimPolicy=Delete this also removes the PVs and the Cinder volumes.
for ns in "${STORAGE_NS[@]}"; do
    if kubectl get namespace "$ns" >/dev/null 2>&1; then
        kubectl delete pvc --all --namespace "$ns" --ignore-not-found
    else
        echo "Namespace '$ns' not found, skipping PVC delete"
    fi
done

# 3. Verify nothing is left.
echo "Remaining PVCs in storage namespaces:"
kubectl get pvc -A | grep -E "$(IFS='|'; echo "${STORAGE_NS[*]}")" || echo "  none"
echo "Remaining PVs bound to storage namespaces:"
kubectl get pv | grep -E "$(IFS='|'; echo "${STORAGE_NS[*]}")" || echo "  none"
