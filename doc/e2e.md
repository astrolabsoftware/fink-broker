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
cd fink-broker
```

### Install ciux

Install the `ciux` tool based on the provided version.

```bash
CIUX_VERSION=v0.0.7-rc1
go install github.com/k8s-school/ciux@"$CIUX_VERSION"
```

### Ciux Project Ignition

Ignite the project using `ciux`.

```bash
# Create directory for ciux configuration
mkdir -p ~/.ciux
# Define variables below for all the following commands
export SUFFIX="noscience"
# Ignite the project
ciux ignite --selector itest $PWD --suffix "$SUFFIX"
```

### Install pre-requisites, Fink-Alert-Simulator and Fink-Broker

```bash
# Create a Kubernetes cluster (Kind)
# Install OLM and ArgoCD operators.
./e2e/prereq-install.sh

# Run ArgoCD
./e2e/argocd.sh
```

### Check Results

Check the results of the tests.

```bash
./e2e/check-results.sh
```

## Conclusion

By following these steps, you should be able to install and run the end-to-end tests successfully.
