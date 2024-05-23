# Developer guide for Kubernetes users

## Managing fink-broker images

`Fink-broker` container images for Kubernetes are based on [k8s-spark-py](https://github.com/astrolabsoftware/k8s-spark-py.git) images.

### Pre-requisites

Before proceeding, ensure you have the following prerequisites:

- Access to a Linux workstation with 16GB+ memory and 100GB+ disk
- Go programming language installed (version 1.21.4).
- Docker installed.
- `sudo` access required

### Clone the project:

```shell
git clone https://github.com/astrolabsoftware/fink-broker.git
cd fink-broker
```

### Install `ciux`.

```bash
CIUX_VERSION=v0.0.3-rc2
go install github.com/k8s-school/ciux@"$CIUX_VERSION"
```

### Configure the build

`conf.sh` contains all build parameters with inline documentation for all of them. For example, edit `SPARK_IMAGE_TAG` value in `conf.sh` in order to customize the base image used for the `fink-broker` container image.

```shell
# Work with minimal CPU/memory requirements and no science code
export SUFFIX="noscience"
# Use science code and more CPU/memory
export SUFFIX=""
```

### Build the image

```shell
./build.sh -s "$SUFFIX"
```

### Push the image to CC-IN2P3 registry

This is useful to share images among multiple users or infrastructures.

```shell
# Log in to IN2P3 registry
docker login gitlab-registry.in2p3.fr
./push-image.sh
```

## Run integration tests

It is possible to run integration tests locally, as documented in `e2e.md`. For additional details, the `e2e` section of `.github/workflows/e2e-common.yml` displays the shell commands required to perform these tasks.

## Automated build

The CI will automatically build, run integration tests, and eventually push the `fink-broker` container image inside the IN2P3 registry for each commit to the git repository.
```