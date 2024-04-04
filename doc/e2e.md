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
cd fink-broker
# Variables below must be defined in all the following commands
export SUFFIX="noscience"
export CIUXCONFIG=$HOME/.ciux/ciux.sh
ciux ignite --selector ci $PWD --suffix "$SUFFIX"
```

### Create Kubernetes (Kind) Cluster

Create a Kubernetes cluster (Kind).

```bash
ktbx install kind
ktbx install kubectl
ktbx create -s
```

### Install OLM and ArgoCD Operators

Install OLM and ArgoCD operators.

```bash
ktbx install olm
ktbx install argocd
```

### Install Argo Workflows

Install Argo Workflows.

```bash
ktbx install argowf
```

### Run ArgoCD

Run ArgoCD.

```bash
./e2e/argocd.sh
```

### Run Fink-Alert-Simulator

Run the Fink-Alert-Simulator.

```bash
. "$CIUXCONFIG"
"$FINK_ALERT_SIMULATOR_DIR"/argo-submit.sh
argo watch @latest
```

7.  **Install Fink-Broker Prerequisites (JDK, Spark)**

Install prerequisites for Fink-Broker (JDK, Spark).

```bash
# commands are provided for Debian-based distribution
# use yum for RedHat-based distribution
sudo apt-get -y update
sudo apt-get -y install openjdk-8-jdk-headless
./e2e/prereq-install.sh
```

### Run Fink-Broker

Run the Fink-Broker.

```bash
./e2e/fink-start.sh -e
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