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

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

. $DIR/../conf.sh

# TODO improve management of expected topics
# for example in finkctl.yaml
if [ "$SUFFIX" = "noscience" ];
then
  expected_topics="11"
else
  expected_topics="1"
fi

count=0
while ! finkctl wait topics --expected "$expected_topics" --timeout 60s -v1
do
    echo "Waiting for expected topics: $expected_topics"
    sleep 5
    kubectl get pods
    count=$((count+1))
    if [ $count -eq 10 ]; then
        echo "Timeout waiting for topics to be created"
        exit 1
    fi
done
finkctl get topics
