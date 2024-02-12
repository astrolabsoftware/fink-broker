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

# Create docker image containing Fink packaged for k8s

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

readonly  FINKKUB=$(readlink -f "${DIR}/..")
. $FINKKUB/conf.sh

readonly MINIKUBE_VERSION="v1.27.0"
if minikube version | grep "minikube version: $MINIKUBE_VERSION" > /dev/null
then
  echo "Use existing minikube version==$MINIKUBE_VERSION"
else
  echo "Install minikube version==$MINIKUBE_VERSION"
  curl -Lo /tmp/minikube https://storage.googleapis.com/minikube/releases/$MINIKUBE_VERSION/minikube-linux-amd64
  chmod +x /tmp/minikube

  sudo mkdir -p /usr/local/bin/
  sudo install /tmp/minikube /usr/local/bin/
fi

minikube delete

NPROC=$(nproc)
if [ $NPROC -lt $CPUS ]; then
  EFFECTIVE_CPUS=$NPROC
else
  EFFECTIVE_CPUS=$CPUS
fi

minikube start --kubernetes-version "$K8S_VERSION" --driver=docker --cpus "$EFFECTIVE_CPUS" --cni=bridge
