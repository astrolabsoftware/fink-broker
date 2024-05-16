# Installation and execution guide for end-to-end tests of the fink-broker

This document provides instructions on how to install and run end-to-end (e2e) tests for the `fink-broker`

## Prerequisites

Before proceeding, ensure you have the following prerequisites:

- Access to a Linux workstation with 16GB+ memory and 100GB+ disk
- Go programming language installed (version 1.21.4).
- Docker installed.
- `sudo` access required

## Steps to Install and Run E2E Tests

Follow these steps to install and run the end-to-end tests:

### Checkout Code

Ensure you have the code checked out onto your local environment.

```bash
git clone github.com/astrolabsoftware/fink-broker
```

### Install ciux

Install the `ciux` tool based on the provided version.

```bash
CIUX_VERSION=v0.0.3-rc2
go install github.com/k8s-school/ciux@"$CIUX_VERSION"
```

### Ciux Project Ignition

Ignite the project using `ciux`.

```bash
# Create directory for ciux configuration
mkdir -p ~/.ciux
# Define variables below for all the following commands
export SUFFIX="noscience"
export CIUXCONFIG=$HOME/.ciux/ciux.sh
# Ignite the project
cd fink-broker
ciux ignite --selector ci $PWD --suffix "$SUFFIX"
```

### Install pre-requisites

```bash
# Create a Kubernetes cluster (Kind)
ktbx install kind
ktbx install kubectl
ktbx create -s
# Install OLM and ArgoCD operators.
ktbx install olm
ktbx install argocd
# Install Argo Workflows
ktbx install argowf
# Run ArgoCD
./e2e/argocd.sh
```

### Run Fink-Alert-Simulator

Run the Fink-Alert-Simulator.

```bash
. "$CIUXCONFIG"
"$FINK_ALERT_SIMULATOR_DIR"/argo-submit.sh
argo watch @latest
```

1.  **Install Fink-Broker Prerequisites (Spark-operator)**

Install prerequisites for Fink-Broker (JDK, Spark).

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator --namespace spark-operator \
    --create-namespace  --version 1.2.14 --set webhook.enable=true --set sparkJobNamespace=spark
```

### Run Fink-Broker

Run the Fink-Broker.

```bash
./e2e/fink-start.sh
```

### Check Results

Check the results of the tests.

```bash
./e2e/check-results.sh
```


    Promote the Fink-Broker image.

    ```bash
        . "$CIUXCONFIG"
        echo "PROMOTED_IMAGE=$CIUX_IMAGE_REGISTRY/$CIUX_IMAGE_NAME/$FINKCTL_VERSION" >> "$GITHUB_OUTPUT"
        echo "NEW_IMAGE=$CIUX_BUILD" >> "$GITHUB_OUTPUT"
    ```

## Conclusion

By following these steps, you should be able to install and run the end-to-end tests successfully.
