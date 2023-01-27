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

mkdir -p "$SPARK_INSTALL_DIR"

if [ ! -d "$SPARK_HOME" ]
then
  readonly SPARK_ARCHIVE="${SPARK_NAME}.tgz"
  echo "Download and extract Spark ($SPARK_NAME)"
  curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}" -o "${SPARK_INSTALL_DIR}/${SPARK_ARCHIVE}"
  tar -C "$SPARK_INSTALL_DIR" -xf "${SPARK_INSTALL_DIR}/${SPARK_ARCHIVE}"
fi
