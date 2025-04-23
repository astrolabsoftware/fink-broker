# 🔧 Debugging a Crashed CI Kubernetes Cluster

When a CI job crashes, the temporary Kubernetes cluster it was using is retained for **24 hours** on the CI server. This gives you time to inspect and debug the issue.

---

## 🖥️ Step 1: Access the CI Server

Connect to the server where the CI job was running:

```bash
ssh clrlsstsrv02.in2p3.fr
```

---

## 🔍 Step 2: Identify the Cluster

Temporary CI clusters are named with the prefix `fink-ci`. To list all existing clusters:

```bash
kind get clusters
```

Find the one that starts with `fink-ci`.

---

## 📁 Step 3: Retrieve the Kubeconfig

Once you've identified the cluster name, retrieve its kubeconfig to interact with it using `kubectl`:

```bash
kind get kubeconfig --name <cluster-name> > ~/.kube/config
```

Replace `<cluster-name>` with the actual name retrieved in the previous step.

---

## 🛠️ Step 4: Start Debugging

With your kubeconfig set up, begin inspecting the cluster:

- List all pods across all namespaces:

```bash
kubectl get pods -A
```

- Inspect Kafka topics (if applicable):

```bash
kubectl get kafkatopics.kafka.strimzi.io -A
```

---

## ✅ Cleanup Tip

If you're done debugging before the 24-hour window expires, consider deleting the cluster to free up resources.

```bash
ktbx delete -p fink-ci
```