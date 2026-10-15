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
| -1 | `fink-broker` namespace | Persistent namespace for the Spark jobs and their workflows |
| 0 | strimzi, spark-operator, minio-operator, stackable-hadoop-operators | Operators and their CRDs |
| 1 | kafka, hdfs, minio-tenant | Storage, including the Kafka SASL/JAAS secret |
| 2 | fink-broker, fink-alert-simulator | Spark jobs (the simulator is disabled on CC) |

The CC-specific configuration lives in `fink-cd/apps/values-cc.yaml`: local
Kafka enabled, no alert simulator, the datasets written to the CC-IN2P3
production HDFS outside the cluster (`hdfs.external: true`, `hdfs.onlineDataPrefix` pointing there), and the
`raw2science` executor pinned to the dedicated big-worker node pool.

With `hdfs.external`, the Stackable operators and the `hdfs` Application are
not deployed, and the fink-broker chart skips the `/user/185` init Job, which
needs the in-cluster namenode. The balance report reads HDFS directly, from the
NameNode(s) listed in `hdfs.namenodes`, in both cases. Setting
`hdfs.external: false` falls back to the in-cluster HDFS, sized for a real ZTF
night by the knobs kept in the same file; `e2e/check-helm-render.sh` renders
both variants (no cluster needed).

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

### Kafka credentials

No SCRAM password lives in git: `e2e/argocd.sh` provisions the two secrets it
derives — `kafka/fink-producer-password`, read by the `KafkaUser`, and
`fink-broker/fink-kafka-jaas`, mounted by the distribution `SparkApplication` — before
creating the app-of-apps. The step is idempotent and runs on every deployment:
on CC the password is generated on the first run and reused afterwards (CI
infras get a fixed throwaway one, so both environments exercise the same path).

Read it back with:

```bash
kubectl -n kafka get secret fink-producer-password \
  -o jsonpath='{.data.password}' | base64 -d
```

Deploying without `e2e/argocd.sh` means creating both secrets by hand. If they
are missing, the `KafkaUser` stays `NotReady` and the distribution executors
stay in `ContainerCreating` — a loud failure, by design.

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

# Permanent nightly run, pinned on a release tag
./e2e/argocd.sh -i cc -s -r <tag>
```

### Site-local values

Some values are specific to the site and must stay out of git, the
repositories being public: the production HDFS hosts are not in the cluster
DNS, so `ccmaster1` (the NameNode in `hdfs.onlineDataPrefix` and
`hdfs.namenodes`) is resolved through `/etc/hosts` entries of the Spark and
report pods. They go in `apps/values-cc.local.yaml`, next to `values-cc.yaml`
in the fink-cd clone that ciux sets up beside fink-broker. The file is
git-ignored; start from the committed example, which holds everything but the
addresses:

```bash
cd ../fink-cd/apps
cp values-cc.local.yaml.example values-cc.local.yaml
# fill in the real ccmaster1 IP
```

`e2e/argocd.sh -i cc` picks it up by itself (`-l <file>` points to another
one) and hands it to Argo CD as a literal values block: it lives in the `fink`
Application, in the cluster, never in git.
The DataNodes need no entry: the HDFS client reaches them by the IP the
NameNode reports.

Useful variants:

```bash
./e2e/argocd.sh -i cc -s                 # follow the ciux work branches (dev only)
./e2e/argocd.sh -i cc -s -m              # with monitoring enabled
./e2e/argocd.sh -h                       # all options
```

`-s` deploys the nightly run: a `CronWorkflow`, `nightly`, which
submits the Argo `WorkflowTemplate` `night` every day on today's
night (UTC), fixed at submission. The schedule and the deadline come from
`fink-cd/apps/values.yaml` (`nightly.schedule: 1 0 * * *`,
`nightly.exitAt: 20:00`). Without `-s`, no night runs by itself: nights are run
with `e2e/catchup.sh` only (see [Catch up a night](#catch-up-a-night)). Both
go through the same `WorkflowTemplate`, so a night is always run the same way.

The script is idempotent: it upserts the `fink` Application, so re-running it
with a new `-r` is the normal way to upgrade.

### When a run stops

Every run stops at an absolute deadline, computed once by the `guard` step of
the workflow and handed to the three jobs as `-exit_at`:

| Run | Deadline |
|-----|----------|
| nightly | `nightly.exitAt` on the day of its night, `20:00` UTC by default |
| catch-up (`catchup.sh`) of a closed night | start of the workflow + `workflows.deadlineHours` (18 h) |
| catch-up of an open night | when the night closes, `nightCloseHour` (10:00 UTC) the next day |

A job restarted after a crash keeps the deadline of its workflow, so it aims at
the same instant instead of granting itself a fresh window. The deadline of a
nightly run derives from its night, not from the day the job (re)starts: a
restart past midnight no longer aims at the next day
([#1246](https://github.com/astrolabsoftware/fink-broker/issues/1246)). A
nightly run starting past its deadline is refused by the guard before any job
is created, so it reserves no executor; its night is then caught up.

`-exit_at` takes either a time of day in UTC (`23:00`, resolved on the day the
job starts) or a full ISO 8601 instant (`2026-08-21T20:00:00+02:00`, offset
honoured, UTC assumed when absent). The workflows always pass the second form.

## Catch up a night

A night the nightly run did not finish (crash, alerts arriving after its
deadline) is caught up without touching the nightly run:

```bash
./e2e/catchup.sh -n 20261008 -w     # prints the workflow name, waits for its end
kubectl -n fink-broker get workflows
```

The script submits the Argo `WorkflowTemplate` `night`, which runs
the three jobs of the night from the checkpoints of the nightly run: they resume
where it stopped, so only what is missing is processed and distributed. A night
already complete exits at once. A failed catch-up is resumed the same way, by
running the command again.

- **Open and closed nights.** A night is open until `nightCloseHour`
  (10:00 UTC by default) on the next day, once its late alerts are in. The
  jobs of an **open** night, today's or yesterday's, run live until the night
  closes and leave no done marker; those of a **closed** night stop as soon as
  it is caught up and mark it done. A future night is refused.
- **Restarting a failed nightly run**: delete its workflow first
  (`kubectl -n fink-broker delete workflow nightly-<id>`), which frees the
  night and stops its jobs, then `catchup.sh -n <today>`. The jobs resume from
  the checkpoints and run live until the night closes, the next morning, so
  alongside the next nightly run.
- **Deadline.** Fixed once by the guard, so a restarted job keeps it: when the
  night closes for an open night, `workflows.deadlineHours` (18 h) after the
  workflow started for a closed one.
- **One run per night.** A second run of the same night, nightly or catch-up,
  waits for the first to end; different nights run in parallel, each with the
  resources of a nightly run.
- **Re-sending a night** to the `fink_*` topics: delete its distribution
  checkpoint, `<online_data_prefix>/kafka_checkpoint/<night>`, then catch the
  night up.
- **Argo Workflows** is installed by the `fink` app-of-apps
  (`components.argoWorkflows`). At CC that Application is synced by hand:
  `argocd app sync fink` after an upgrade bringing it.

## Check the deployment

```bash
export ARGOCD_OPTS="--core --namespace argocd"

# Rollout status of every component
argocd app list -l app.kubernetes.io/part-of=fink

# Nights
kubectl -n fink-broker get cronworkflows                # nightly run (-s)
kubectl -n fink-broker get workflows                    # nightly runs and catch-ups
kubectl -n fink-broker get sparkapplications            # jobs of the running nights
kubectl -n fink-broker logs -l spark-role=driver --tail=100
kubectl -n fink-broker get cronworkflow nightly \
  -o jsonpath='{.status}'                        # last scheduled run, active ones

# Storage
kubectl -n kafka get kafka,kafkauser,kafkatopic
kubectl -n hdfs get hdfsclusters    # in-cluster HDFS only (hdfs.external=false)
```

Every object of a run is named after it: a nightly run `nightly-<id>`, a
catch-up `catchup-<night>-<id>`. Its SparkApplications are `<run>-<job>`, and
their pods `<run>-<job>-driver`, `<run>-<job>-exec-<n>`, next to the workflow
step pods `<run>-<job>-<n>`, so `kubectl get pods | grep <run>` lists a whole
run. The report pods are `report-<n>-<id>`.

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
- **Restarting a night.** The `SparkApplication`s of a night belong to its
  workflow, not to Argo CD: a job that crashes is retried by the Spark
  operator, and a workflow that failed is resumed by catching its night up
  (`e2e/catchup.sh -n <night>`), once the night is closed.
- **A night without alerts is not a failure.** The telescope does not observe
  every night. `raw2science` and `distribution` wait for their upstream data
  until the deadline, then exit *successfully* with `No alert collected for
  night <N>, nothing to do`. The workflow succeeds, no retry is triggered and
  the next night starts as usual. Before this was handled, such a
  night pinned both jobs indefinitely and blocked every following run.
- **The container image is pinned independently** of the chart revision:
  `ciux` derives it from the hash of the workspace sources. Always deploy from
  a clean checkout of the tag, otherwise the image and the chart diverge.
