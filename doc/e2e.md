# Installation and Running End-to-End Tests Guide

This document provides instructions on how to install and run end-to-end (e2e) tests for the `fink-broker`

## Prerequisites

Before proceeding, ensure you have the following prerequisites:

- Access to a Linux workstation with 16GB+ memory and 100GB+ disk
- Go programming language installed (version 1.21.4).
- Docker installed.
- `sudo` accès required

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
    CIUX_VERSION=
    go install github.com/k8s-school/ciux@"$CIUX_VERSION"
    ```

1. **Ciux Project Ignition**

    Ignite the project using `ciux`.

    ```bash
    ciux ignite --selector ci --branch="$GHA_BRANCH_NAME" $PWD --suffix "${{ env.SUFFIX }}" --tmp-registry "${{ env.CI_REPO }}"
    ```

2. **Create Kubernetes (Kind) Cluster**

    Create a Kubernetes cluster (Kind).

    ```yaml
    - name: Create k8s (kind) cluster
      run: |
        # v0.20.0 does not work on self-hosted runners
        ktbx install kind --kind-version=${{ inputs.kind_version }}
        ktbx install kubectl
        # Configure private registry if needed
        ./e2e/kind-config.sh -r "${{ env.CI_REPO }}"
        ktbx create -s
    ```

3. **Install OLM and ArgoCD Operators**

    Install OLM and ArgoCD operators.

    ```yaml
    - name: Install olm and argocd operators
      run: |
        ktbx install olm
        ktbx install argocd
    ```

4. **Install Argo Workflows**

    Install Argo Workflows.

    ```yaml
    - name: Install argo-workflows (fink-alert-simulator pre-requisite)
      run: |
        ktbx install argowf
    ```

5. **Run ArgoCD**

    Run ArgoCD.

    ```yaml
    - name: Run argoCD
      run: |
        ./e2e/argocd.sh
    ```

6. **Download Image**

    Download the Docker image artifact.

    ```yaml
    - name: Download image
      uses: actions/download-artifact@v3
      with:
        name: docker-artifact
        path: artifacts
    ```

7.  **Load Container Image Inside Kind**

    Load the container image inside the Kind cluster.

    ```yaml
    - name: Load container image inside kind
      run: |
        . "$CIUXCONFIG"
        if [ -f artifacts/image.tar ]; then
          echo "Loading image from archive"
          kind load image-archive artifacts/image.tar
          docker exec -- kind-control-plane crictl image
        else
          echo "Using existing image: $CIUX_IMAGE_URL"
        fi
    ```

8.  **Run Fink-Alert-Simulator**

    Run the Fink-Alert-Simulator.

    ```yaml
    - name: Run fink-alert-simulator
      run: |
        . "$CIUXCONFIG"
        "$FINK_ALERT_SIMULATOR_DIR"/argo-submit.sh
        argo watch @latest
    ```

9.  **Install Fink-Broker Prerequisites (JDK, Spark)**

    Install prerequisites for Fink-Broker (JDK, Spark).

    ```yaml
    - name: Install fink-broker pre-requisites (JDK, Spark)
      run: |
        sudo apt-get -y update
        sudo apt-get -y install openjdk-8-jdk-headless
        ./e2e/prereq-install.sh
    ```

10. **Run Fink-Broker**

    Run the Fink-Broker.

    ```yaml
    - name: Run fink-broker
      run: |
        ./e2e/fink-start.sh -e
    ```

11. **Check Results**

    Check the results of the tests.

    ```yaml
    - name: Check results
      run: |
        ./e2e/check-results.sh
    ```

12. **Promote Fink-Broker Image**

    Promote the Fink-Broker image.

    ```yaml
    - name: Promote fink-broker image
      id: promote
      run: |
        . "$CIUXCONFIG"
        echo "PROMOTED_IMAGE=$CIUX_IMAGE_REGISTRY/$CIUX_IMAGE_NAME/$FINKCTL_VERSION" >> "$GITHUB_OUTPUT"
        echo "NEW_IMAGE=$CIUX_BUILD" >> "$GITHUB_OUTPUT"
    ```

## Conclusion

By following these steps, you should be able to install and run the end-to-end tests successfully. Ensure that all necessary configurations and dependencies are set up correctly before executing the tests.