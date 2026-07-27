# Deploying fink-broker at CC-IN2P3

This document describes how to deploy and operate the `fink-broker` stack on the
Talos Kubernetes cluster hosted at CC-IN2P3 (OpenStack). For local end-to-end
tests on Kind, see [e2e.md](e2e.md) instead.

## Overview

The whole stack is deployed by a single Argo CD *app-of-apps* named `fink`,
which lives in the [fink-cd](https://github.com/astrolabsoftware/fink-cd)
repository (`apps/`). `e2e/argocd.sh` only creates that parent Application and
waits for the rollout; everything else is declarative.

Child Applications are ordered by Argo CD sync-waves and are all auto-synced,
so Argo CD gates each wave on the health of the previous one:

| Wave | Applications | Role |
|------|--------------|------|
| -1 | `spark` namespace | Persistent namespace for the Spark jobs |
| 0 | strimzi, spark-operator, minio-operator, stackable-hadoop-operators | Operators and their CRDs |
| 1 | kafka, hdfs, minio-tenant | Storage, including the Kafka SASL/JAAS secret |
| 2 | fink-broker, fink-alert-simulator | Spark jobs (the simulator is disabled on CC) |

The CC-specific configuration lives in `fink-cd/apps/values-cc.yaml`: local
Kafka enabled, no alert simulator, HDFS sized for a real ZTF night, and the
`raw2science` executor pinned to the dedicated big-worker node pool.

## Prerequisites

### Cluster access

All operations run from the Talos bastion, which has `kubectl`, `talosctl`,
`tofu` and the OpenStack CLI configured:

```bash
ssh ccf   # talos-bastion
```

Argo CD is used in *core* mode (no API server, no `argocd login`); the scripts
set this themselves:

```bash
export ARGOCD_OPTS="--core --namespace argocd"
```

The cluster itself is provisioned out of band by the
[talos_on_openstack](https://github.com/k8s-school/talos_on_openstack) OpenTofu
project.

### ciux

`e2e/argocd.sh` resolves the fink-broker container image through `ciux`; see the
[ciux install guide](https://github.com/k8s-school/ciux#installation) and the
ignition step in [e2e.md](e2e.md).

### Kafka credentials (one-shot)

`values-cc.yaml` sets `kafka.jaas.create=false`, so the CC deployment never runs
on the `CHANGEME` default of `apps/values.yaml` — the `external` listener is a
nodeport with `scram-sha-512`. The two secrets must be created once on the
cluster; they then persist across deployments:

```bash
PW=$(openssl rand -base64 24)
kubectl -n kafka create secret generic fink-producer-password \
  --from-literal=password="$PW"
printf '// Configuration for secure kafka authentication\nKafkaClient {\n    org.apache.kafka.common.security.scram.ScramLoginModule required\n    username="fink-producer"\n    password="%s";\n};\n' "$PW" > /tmp/kafka-jaas.conf
kubectl -n spark create secret generic fink-kafka-jaas \
  --from-file=kafka-jaas.conf=/tmp/kafka-jaas.conf && rm /tmp/kafka-jaas.conf
```

If they are missing, the `KafkaUser` stays `NotReady` and the distribution
executors stay in `ContainerCreating` — a loud failure, by design.

## Pin a release

Every Application is auto-synced, so `spec.source.targetRevision` is a live
subscription: deploying from a branch makes the running pipeline follow that
branch's HEAD. Production deployments must therefore be pinned to an
**immutable** tag (never move a tag that is already deployed: Argo CD caches
resolved revisions, and a moving tag makes resyncs unpredictable).

The three source repositories share a coordinated tag scheme — cut the same tag
in `fink-cd`, `fink-broker` and `fink-alert-simulator` as described in
[release.md](release.md). `e2e/argocd.sh -r` refuses to start if the ref is
missing from any of the three.

## Deploy

```bash
git clone https://github.com/astrolabsoftware/fink-broker.git
cd fink-broker
git checkout <tag>          # keep the workspace clean: ciux derives the
                            # container image tag from the sources

# Permanent daily run, pinned on a release tag
./e2e/argocd.sh -i cc -s -r <tag>
```

Useful variants:

```bash
./e2e/argocd.sh -i cc -s                 # follow the ciux work branches (dev only)
./e2e/argocd.sh -i cc -n 20260722        # one-shot backfill of a given night
./e2e/argocd.sh -i cc -s -m              # with monitoring enabled
./e2e/argocd.sh -h                       # all options
```

`-s` (scheduled) deploys `ScheduledSparkApplication`s that deduce the observing
night at runtime; the schedule and the night offset come from
`fink-cd/apps/values.yaml` (`0 12 * * *`, `nightOffsetHours: 24` — the previous
complete ZTF night). Without `-s`, one-shot `SparkApplication`s are deployed
with the night frozen at Helm render time (defaulting to the previous night on
CC), which is what you want for a backfill or a rerun.

The script is idempotent: it upserts the `fink` Application, so re-running it
with a new `-r` is the normal way to upgrade.

## Check the deployment

```bash
export ARGOCD_OPTS="--core --namespace argocd"

# Rollout status of every component
argocd app list -l app.kubernetes.io/part-of=fink

# Spark jobs
kubectl -n spark get scheduledsparkapplications   # scheduled mode
kubectl -n spark get sparkapplications            # one-shot mode, and scheduled runs
kubectl -n spark logs -l spark-role=driver --tail=100

# Storage
kubectl -n kafka get kafka,kafkauser,kafkatopic
kubectl -n hdfs get hdfsclusters
```

`e2e/diag.sh` collects a broader diagnostic dump when something is wrong; see
also [troubleshoot.md](troubleshoot.md).

## Upgrade and rollback

An upgrade is a new coordinated tag plus a re-run of the deploy command with the
new `-r`. A rollback is the same command with the previous tag: Argo CD resyncs
the child Applications to that revision and the Spark operator restarts the
jobs.

To change a single parameter without cutting a tag (for example to raise a
memory limit during an incident), `argocd app set fink -p <key>=<value>`
works, but the change is lost at the next run of `argocd.sh` — fold it back into
`values-cc.yaml`.

## Teardown

```bash
./e2e/argocd-delete.sh
```

The stack is deleted wave by wave, highest first, with an explicit PVC/PV
cleanup between storage and operators so no volume hangs in `Terminating`.

**WARNING**: the storage class is `csi-cinder-sc-delete`
(`reclaimPolicy=Delete`), so this destroys the underlying Cinder volumes and all
HDFS/Kafka data. This is irreversible.

## Operational notes

- **Auto-sync is on for every component.** A resource deleted or edited by hand
  in the `spark`, `kafka` or `hdfs` namespaces is reverted within seconds by
  `selfHeal`. To pause it during an investigation:
  `argocd app set fink-broker --sync-policy none`.
- **Restarting a job** is done by deleting its `SparkApplication`; `selfHeal`
  recreates it identically (same night) and the operator relaunches it.
- **The container image is pinned independently** of the chart revision:
  `ciux` derives it from the hash of the workspace sources. Always deploy from
  a clean checkout of the tag, otherwise the image and the chart diverge.
