#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

STCK_BIN="/tmp/stackable"

ink "Create hdfs directory"
$DIR/mkdir.sh
