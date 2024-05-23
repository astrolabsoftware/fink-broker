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

private_registry=""
# Get option -r for tmp-registry
while getopts ":r:" opt; do
  case $opt in
    r) private_registry="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [ -z "$private_registry" ]; then
  echo "Option -r not set. Using default kind configuration"
  exit 0
fi

echo "Using private registry: $private_registry"
mkdir -p $HOME/.ktbx
cat <<EOF > $HOME/.ktbx/config
kind:
  workers: 0

  # Supported only for clusters with one node
  # Certificates must be available on kind host at "/etc/docker/certs.d/{{ .PrivateRegistry }}"
  privateRegistry: "$private_registry"
EOF