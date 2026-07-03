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
# Bounded waits (seconds) so the script never hangs forever and always reaches
# the PVC cleanup, which is what keeps it idempotent across partial teardowns.
APP_DELETE_TIMEOUT=300
PVC_DELETE_TIMEOUT=180

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

# Wait (bounded) for the app to actually disappear before touching the PVCs. An
# ArgoCD Application can get stuck in Terminating on a finalizer; rather than
# looping forever we warn and fall through to the PVC cleanup, which must run so
# no stale volume (with an old Kafka cluster.id) survives into the next deploy.
app_deadline=$(( SECONDS + APP_DELETE_TIMEOUT ))
while argocd app get "$APP" >/dev/null 2>&1; do
    if (( SECONDS >= app_deadline )); then
        echo "WARNING: app '$APP' still present after ${APP_DELETE_TIMEOUT}s; continuing with PVC cleanup"
        break
    fi
    echo "Waiting for app '$APP' to be fully deleted..."
    sleep 5
done

# 2. Delete the storage PVCs left behind by the StatefulSets. With
#    reclaimPolicy=Delete this also removes the PVs and the Cinder volumes.
for ns in "${STORAGE_NS[@]}"; do
    if ! kubectl get namespace "$ns" >/dev/null 2>&1; then
        echo "Namespace '$ns' not found, skipping PVC delete"
        continue
    fi

    # Force-remove any pod still mounting a PVC. While a pod mounts it, the
    # kubernetes.io/pvc-protection finalizer blocks the PVC deletion and the
    # delete below would hang. After a clean cascade there should be none; this
    # is a safety net that keeps the script robust to partial teardowns.
    kubectl delete pod --all --namespace "$ns" \
        --grace-period=0 --force --ignore-not-found --wait=false || true

    # --wait=false: return immediately; we wait explicitly (bounded) below.
    kubectl delete pvc --all --namespace "$ns" --ignore-not-found --wait=false
done

# 3. Wait (bounded) for the PVCs to actually disappear.
for ns in "${STORAGE_NS[@]}"; do
    kubectl get namespace "$ns" >/dev/null 2>&1 || continue
    pvc_deadline=$(( SECONDS + PVC_DELETE_TIMEOUT ))
    while [ -n "$(kubectl get pvc --namespace "$ns" -o name 2>/dev/null)" ]; do
        if (( SECONDS >= pvc_deadline )); then
            echo "WARNING: PVCs still present in '$ns' after ${PVC_DELETE_TIMEOUT}s:"
            kubectl get pvc --namespace "$ns"
            break
        fi
        echo "Waiting for PVCs in '$ns' to be deleted..."
        sleep 5
    done
done

# 4. Verify nothing is left.
echo "Remaining PVCs in storage namespaces:"
kubectl get pvc -A | grep -E "$(IFS='|'; echo "${STORAGE_NS[*]}")" || echo "  none"
echo "Remaining PVs bound to storage namespaces:"
kubectl get pv | grep -E "$(IFS='|'; echo "${STORAGE_NS[*]}")" || echo "  none"
