# Set up fink self-hosted CI

Procedure for creating the Linux user account which runs the fink self-hosted CI, on Ubuntu, and adding it to the Docker group.

## Pre-requisites

- a Linux server with sudo access
- Docker 24.0.2+
- git 2.17.1+
- go 1.22.5+

## Steps

### List of commands

```bash
# Create user
sudo adduser fink-ci --disabled-password

# Add to Docker group
sudo usermod -aG docker fink-ci

sudo su - fink-ci
```

The user then hosts the GitHub Actions self-hosted runner (`actions-runner`) which runs
the self-hosted e2e job of `.github/workflows/e2e-common.yml`.

## Test a branch

Log into the LPC server. Go root, and then `fink-ci`. Test your branch from a checkout of it using:

```bash
BRANCH=<git-branch-name-on-fink-broker>
git clone -b ${BRANCH} https://github.com/astrolabsoftware/fink-broker.git
nohup ./fink-broker/e2e/run.sh -s science > ${BRANCH}.txt &
```

then you can follow the log:

```bash
tail -f ${BRANCH}.txt
```

and in case you want to inspect pods, go back to your username:

```
# as peloton
kind get clusters
kind get kubeconfig --name fink-ci-ci-distribute-ram-bf5747 > ~/.kube/config

kubectl get pods -A
```
