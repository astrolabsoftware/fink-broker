#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

CLUSTER_NAME=$(ciux get clustername "$DIR/..")
ktbx delete -n "$CLUSTER_NAME"