#!/bin/bash

# Tear down the fink-broker stack in reverse install order so no volume ever
# hangs in Terminating.
#
# The `fink` app-of-apps installs its children by ArgoCD sync-wave:
#   wave 0  operators   (strimzi, spark/minio operators, Stackable operators
#                        including the listener-operator, whose CSI driver runs
#                        *inside* the cluster)
#   wave 1  storage     (hdfs, kafka, minio-tenant)
#   wave 2  workloads   (fink-broker, fink-alert-simulator)
#
# Sync-waves order *sync*, not cascade *delete*, so `argocd app delete fink`
# would tear everything down at once and can kill the listener CSI driver before
# its PVs are released, leaving them stuck in Terminating forever. We instead
# delete the child Applications explicitly, highest wave first, and clean up the
# PVCs/PVs at the storage->operators boundary while the CSI drivers are still
# alive:
#   1. workloads + storage apps (wave >= STORAGE_MIN_WAVE), highest wave first
#   2. storage PVCs, then PVs   (operators/CSI still running -> clean release)
#   3. operator apps            (wave < STORAGE_MIN_WAVE)
#   4. the now-empty app-of-apps
#
# ArgoCD's cascade only removes resources it tracks from Git. The HDFS and Kafka
# PVCs come from StatefulSet volumeClaimTemplates created by the operators, so
# ArgoCD never sees them; we delete them explicitly with kubectl.
#
# WARNING: the storage class is csi-cinder-sc-delete (reclaimPolicy=Delete), so
# deleting the PVCs destroys the underlying Cinder volumes and all HDFS/Kafka
# data. This is irreversible.
#
# @author  Fabrice Jammes
#
# TODO: test the wave-ordered teardown end-to-end on the tofu-fink cluster and
#       confirm no PV is left in Terminating (listener PVs in particular).

set -euxo pipefail

NS=argocd
APP="fink"
# Child apps at this sync-wave or above are workloads/storage (own the PVCs) and
# are deleted before the PVC/PV cleanup; apps below are operators, deleted after.
STORAGE_MIN_WAVE=1
# Namespaces holding the storage PVCs created by the operators.
STORAGE_NS=("hdfs" "kafka")
# StorageClass of the Stackable listener PVs. Its CSI driver lives inside the
# fink app; if that driver is gone, nothing can remove these PVs' finalizers.
LISTENER_SC="listeners.stackable.tech"
# Bounded waits (seconds) so the script never hangs forever and always reaches
# the PVC cleanup, which is what keeps it idempotent across partial teardowns.
APP_DELETE_TIMEOUT=300
PVC_DELETE_TIMEOUT=180

# --- CONFIGURATION WITHOUT TUNNEL ---
# Force the use of local K8s context (no 'argocd login' needed).
export ARGOCD_OPTS="--core --namespace $NS"
kubectl config set-context --current --namespace="$NS"

# Distinct sync-waves among the child apps (all Applications except the parent),
# sorted descending. A missing sync-wave annotation counts as wave 0.
child_waves() {
    kubectl get applications.argoproj.io -n "$NS" \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.argocd\.argoproj\.io/sync-wave}{"\n"}{end}' 2>/dev/null \
        | awk -F '\t' -v app="$APP" '$1 != app { print ($2 == "" ? 0 : $2) + 0 }' \
        | sort -rnu
}

# Names of the child apps at a given sync-wave.
apps_at_wave() {
    local wave="$1"
    kubectl get applications.argoproj.io -n "$NS" \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.argocd\.argoproj\.io/sync-wave}{"\n"}{end}' 2>/dev/null \
        | awk -F '\t' -v app="$APP" -v w="$wave" '$1 != app && (($2 == "" ? 0 : $2) + 0) == w { print $1 }'
}

# Delete the given ArgoCD apps (cascade, foreground) and wait, bounded, for them
# to disappear. Falls through on timeout so the teardown always makes progress.
delete_apps_wait() {
    local apps=("$@")
    [ ${#apps[@]} -eq 0 ] && return 0
    local a
    for a in "${apps[@]}"; do
        if argocd app get "$a" >/dev/null 2>&1; then
            echo "Deleting ArgoCD app '$a'..."
            argocd app delete "$a" --cascade --propagation-policy foreground -y || true
        fi
    done
    local deadline=$(( SECONDS + APP_DELETE_TIMEOUT ))
    while :; do
        local remaining=()
        for a in "${apps[@]}"; do
            argocd app get "$a" >/dev/null 2>&1 && remaining+=("$a")
        done
        [ ${#remaining[@]} -eq 0 ] && break
        if (( SECONDS >= deadline )); then
            echo "WARNING: apps still present after ${APP_DELETE_TIMEOUT}s: ${remaining[*]}"
            break
        fi
        echo "Waiting for apps to be deleted: ${remaining[*]}"
        sleep 5
    done
}

# Stop the app-of-apps from re-syncing (recreating) children while we delete
# them wave by wave. Harmless if the app is already manual-sync or absent.
if argocd app get "$APP" >/dev/null 2>&1; then
    argocd app set "$APP" --sync-policy none >/dev/null 2>&1 || true
fi

# 1. Delete workload + storage apps, highest sync-wave first, so each consumer
#    stops before the storage it uses. Their operators (wave 0) stay up to
#    reconcile the deletions.
for w in $(child_waves); do
    (( w < STORAGE_MIN_WAVE )) && continue
    mapfile -t grp < <(apps_at_wave "$w")
    [ ${#grp[@]} -eq 0 ] && continue
    echo "== Deleting wave $w apps: ${grp[*]} =="
    delete_apps_wait "${grp[@]}"
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

# 4. Delete the PVs left behind by the storage namespaces.
#    reclaimPolicy=Delete deletes the PV once its PVC is gone, but that only
#    happens if a CSI driver is around to release the backing volume:
#      - csi-cinder-sc-delete PVs are handled by the cluster-level Cinder CSI
#        driver, which removes the Cinder volume and the PV on its own.
#      - listeners.stackable.tech PVs are served by the listener-operator CSI
#        driver, which is still up now (wave 0 deleted in step 5), so they
#        release cleanly here.
#    We delete every storage PV, then force-remove finalizers on any listener PV
#    still Terminating after a bounded wait (safety net for a partial teardown
#    where the operator was already gone). We never force finalizers on Cinder
#    PVs: that would orphan (leak) the underlying Cinder volume.
storage_re="$(IFS='|'; echo "${STORAGE_NS[*]}")"

# Emit "<pv-name>\t<storageClass>" for every PV whose claim is in a storage ns.
storage_pvs() {
    kubectl get pv -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.claimRef.namespace}{"\t"}{.spec.storageClassName}{"\n"}{end}' 2>/dev/null \
        | awk -v re="$storage_re" -F '\t' '$2 ~ "^(" re ")$" { print $1 "\t" $3 }'
}

pvs=$(storage_pvs | cut -f1)
if [ -n "$pvs" ]; then
    echo "Deleting PVs bound to storage namespaces:"
    echo "$pvs"
    # shellcheck disable=SC2086
    kubectl delete pv $pvs --ignore-not-found --wait=false
fi

# Wait (bounded) for the PVs to disappear; then force-remove finalizers on any
# listener PV still stuck in Terminating (its CSI driver is gone).
pv_deadline=$(( SECONDS + PVC_DELETE_TIMEOUT ))
while [ -n "$(storage_pvs)" ]; do
    if (( SECONDS >= pv_deadline )); then
        echo "PVs still present after ${PVC_DELETE_TIMEOUT}s; unsticking listener PVs:"
        storage_pvs | while IFS=$'\t' read -r pv sc; do
            if [ "$sc" = "$LISTENER_SC" ]; then
                echo "  force-removing finalizers on $pv (class $sc)"
                kubectl patch pv "$pv" --type=merge \
                    -p '{"metadata":{"finalizers":null}}' || true
            else
                echo "  WARNING: $pv (class $sc) still Terminating; not forcing" \
                     "finalizers (would leak the backing volume)"
            fi
        done
        break
    fi
    echo "Waiting for storage PVs to be deleted..."
    sleep 5
done

# 5. Delete the operator apps (wave < STORAGE_MIN_WAVE), highest wave first, now
#    that no storage volume needs their CSI drivers.
for w in $(child_waves); do
    (( w >= STORAGE_MIN_WAVE )) && continue
    mapfile -t grp < <(apps_at_wave "$w")
    [ ${#grp[@]} -eq 0 ] && continue
    echo "== Deleting operator wave $w apps: ${grp[*]} =="
    delete_apps_wait "${grp[@]}"
done

# 6. Delete the now-empty app-of-apps.
delete_apps_wait "$APP"

# 7. Verify nothing is left.
echo "Remaining PVCs in storage namespaces:"
kubectl get pvc -A | grep -E "$storage_re" || echo "  none"
echo "Remaining PVs bound to storage namespaces:"
kubectl get pv | grep -E "$storage_re" || echo "  none"
