#!/usr/bin/env bash

# Push image to Docker Hub or load it inside kind

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

export CIUXCONFIG=$HOME/.ciux/ciux.build.sh
. "$CIUXCONFIG"

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -k          development mode: load image in kind
    -d          do not push image to remote registry
    -s          image suffix, default to none (i.e. science), only 'noscience' is supported

Push image to remote registry and/or load it inside kind
EOD
}

kind=false
registry=true
suffix=""

# get the options
while getopts dhks: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    k) kind=true ;;
	    d) registry=false ;;
            s) suffix=$OPTARG ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [[ $suffix =~ ^noscience* ]]; then
    SELECTOR="build=noscience"
else
    SELECTOR="build=science"
fi

CIUXCONFIG=$(ciux get configpath --selector $SELECTOR $DIR)
echo "Sourcing ciux config from $CIUXCONFIG"
. $CIUXCONFIG

if [ $kind = true ]; then
  kind load docker-image "$CIUX_IMAGE_URL"
fi
if [ $registry = true ]; then
  docker push "$CIUX_IMAGE_URL"
fi
