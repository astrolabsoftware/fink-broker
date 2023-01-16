#!/bin/bash

# Clone fink-alert-simulator code

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/../conf.sh

if [ -d "$FINK_ALERT_SIMULATOR_DIR" ]; then
  rm -rf "$FINK_ALERT_SIMULATOR_DIR"
fi

REPO_URL="https://github.com/astrolabsoftware/fink-alert-simulator"

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BRANCH=${GHA_BRANCH_NAME:-$GIT_BRANCH}
# Retrieve same fink-k8s branch if it exists, else use main branch
if git ls-remote --exit-code --heads "$REPO_URL" "$BRANCH"
then
    FINK_ALERT_SIMULATOR_VERSION="$BRANCH"
else
    FINK_ALERT_SIMULATOR_VERSION="master"
fi

git clone "$REPO_URL" --branch "$FINK_ALERT_SIMULATOR_VERSION" \
  --single-branch --depth=1 "$FINK_ALERT_SIMULATOR_DIR"
