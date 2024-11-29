#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

STCK_BIN="/tmp/stackable"

# TODO move to a job inside argoCD
ink "Create hdfs directory"
$DIR/mkdir.sh
