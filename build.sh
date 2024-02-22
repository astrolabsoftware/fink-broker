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

# Create docker image containing fink-broker for k8s

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)


usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -s          image suffix, default to none, only 'noscience' is supported

Build image containing fink-broker for k8s
EOD
}

suffix=""
tmp_registry=""

# get the options
while getopts hr:s: c ; do
    case $c in
	    h) usage ; exit 0 ;;
            r) tmp_registry=$OPTARG ;;
	    s) suffix=$OPTARG ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

# This command avoid retrieving build dependencies if not needed
$(ciux get image --check $DIR --suffix "$suffix" --tmp-registry "$tmp_registry" --env)

if [ $CIUX_BUILD = false ];
then
    echo "Build cancelled, image $CIUX_IMAGE_URL already exists and contains current source code"
    exit 0
fi

ciux ignite --selector build $DIR --suffix "$suffix" --tmp-registry "$tmp_registry"
. $DIR/conf.sh

if [[ $suffix =~ ^noscience* ]]; then
    target="noscience"
else
    target="full"
fi

# Build image
docker image build --tag "$CIUX_IMAGE_URL" --build-arg spark_py_image="$ASTROLABSOFTWARE_FINK_SPARK_PY_IMAGE" "$DIR" --target $target

