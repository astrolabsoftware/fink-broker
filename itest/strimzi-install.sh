#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Install Strimzi inside k8s

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

readonly  FINKKUB=$(readlink -f "${DIR}/..")
. $FINKKUB/conf.sh

# Strimzi version
readonly STRIMZI_VERSION="0.31.1"

# Name for the Strimzi archive
readonly STRIMZI_NAME="strimzi-${STRIMZI_VERSION}"

# Strimzi install location
readonly STRIMZI_INSTALL_DIR="${HOME}/strimzi-tmp"

STRIMZI_HOME="${STRIMZI_INSTALL_DIR}/${STRIMZI_NAME}"

mkdir -p "$STRIMZI_INSTALL_DIR"

if [ ! -d "${STRIMZI_HOME}" ]
then
  readonly STRIMZI_ARCHIVE="${STRIMZI_NAME}.tar.gz"
  echo "Download and extract Strimzi ($STRIMZI_NAME)"
  curl -Lo "${STRIMZI_INSTALL_DIR}/${STRIMZI_ARCHIVE}" "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/${STRIMZI_ARCHIVE}"
  tar -C ${STRIMZI_INSTALL_DIR} -zxvf "${STRIMZI_INSTALL_DIR}/${STRIMZI_ARCHIVE}"
fi

sed -i 's/namespace: .*/namespace: kafka/' "${STRIMZI_HOME}"/install/cluster-operator/*RoleBinding*.yaml


# Install operator and CR in the same namespace
kubectl create namespace "$KAFKA_NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n "$KAFKA_NS" -f "${STRIMZI_HOME}"/install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml
kubectl apply -n "$KAFKA_NS" -f "${STRIMZI_HOME}"/install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml
kubectl apply -n "$KAFKA_NS" -f "${STRIMZI_HOME}"/install/cluster-operator/

cat << EOF | kubectl apply -n "$KAFKA_NS" -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: $KAFKA_CLUSTER
spec:
  kafka:
    replicas: 1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: tls
      - name: external
        port: 9094
        type: nodeport
        tls: false
    storage:
      type: jbod
      volumes:
      - id: 0
        type: persistent-claim
        size: 100Gi
        deleteClaim: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  zookeeper:
    replicas: 1
    storage:
      type: persistent-claim
      size: 100Gi
      deleteClaim: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF

kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=300s -n "$KAFKA_NS"


