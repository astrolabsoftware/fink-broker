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

# This will avoid overriding user ciuxconfig during a build

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -s          image suffix, default to none, only 'noscience' is supported
    -i 		Specify the survey. Default: ztf

Build image containing fink-broker for k8s
EOD
}

suffix=""
tmp_registry=""
input_survey="ztf"

# get the options
while getopts hr:i:s: c ; do
    case $c in
	    h) usage ; exit 0 ;;
            r) tmp_registry=$OPTARG ;;
	    s) suffix=$OPTARG ;;
	    i) input_survey=$OPTARG ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

ignite_msg="Run following command to prepare integration tests:"
ignite_msg="${ignite_msg}  ciux ignite --selector ci \"$DIR\" --suffix \"$suffix\""

# This command avoid retrieving build dependencies if not needed
$(ciux get image --check $DIR --suffix "$suffix" --env)

if [ $CIUX_BUILD = false ];
then
    echo "Build cancelled, image $CIUX_IMAGE_URL already exists and contains current source code"
    echo $ignite_msg
    exit 0
fi

if [[ $suffix =~ ^noscience* ]]; then
    SELECTOR="build=noscience"
else
    SELECTOR="build=science"
fi

ciux ignite --selector "$SELECTOR" $DIR --suffix "$suffix"


# TODO improve and use
# . $DIR/.ciux.d/ciuxconfig.sh
CIUXCONFIG=$(ciux get configpath --selector $SELECTOR $DIR)
echo "Sourcing ciux config from $CIUXCONFIG"
. $CIUXCONFIG

if [[ $suffix =~ ^noscience* ]]; then
    target="noscience"
    base_image="$ASTROLABSOFTWARE_FINK_FINK_DEPS_NOSCIENCE_ZTF_IMAGE"
else
    target="science"
    base_image="$ASTROLABSOFTWARE_FINK_FINK_DEPS_SCIENCE_ZTF_IMAGE"
fi

# Build image using pre-built fink-deps images
docker image build --tag "$CIUX_IMAGE_URL" --build-arg base_image="$base_image" "$DIR"


echo "Build successful"
echo $ignite_msg

