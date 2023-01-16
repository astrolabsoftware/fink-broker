#!/usr/bin/env bash

# Push image to Docker Hub or load it inside kind

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -k          development mode: load image in kind
    -d          do not push image to remote registry 

Push image to remote registry and/or load it inside kind
EOD
}

kind=false
registry=true

# get the options
while getopts dhk c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    k) kind=true ;;
	    d) registry=false ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ $kind = true ]; then
  kind load docker-image "$IMAGE"
fi
if [ $registry = true ]; then
  docker push "$IMAGE"
fi
