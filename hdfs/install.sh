#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

STCK_BIN="/tmp/stackable"

ink "Install Stackable operators"
curl -L -o $STCK_BIN https://github.com/stackabletech/stackable-cockpit/releases/download/stackablectl-24.7.0/stackablectl-x86_64-unknown-linux-gnu
chmod +x $STCK_BIN
$STCK_BIN operator install commons=24.7.0   secret=24.7.0 listener=24.7.0  zookeeper=24.7.0  hdfs=24.7.0

ink "Install HDFS instance"
kubectl apply -f $DIR -n default

$DIR/mkdir.sh
