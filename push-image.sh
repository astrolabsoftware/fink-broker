#!/usr/bin/env bash

# Push image to Docker Hub or load it inside kind

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

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

CIUXCONFIG=$(ciux get configpath --selector "itest" $DIR)
echo "Sourcing ciux config from $CIUXCONFIG"
. $CIUXCONFIG

if [ $kind = true ]; then
  kind load docker-image "$CIUX_IMAGE_URL"
fi
if [ $registry = true ]; then
  if [ $NEW_IMAGE = true ]; then
    echo "Push image $PROMOTED_IMAGE"
    docker tag "$IMAGE" "$PROMOTED_IMAGE"
    docker push "$PROMOTED_IMAGE"
  else
    if which skopeo; then
      echo "skopeo is already installed"
    else
      echo "skopeo not available, cannot copy image"
      exit 1
    fi
    echo "Add image tag $PROMOTED_IMAGE to $IMAGE"
    skopeo copy docker://$IMAGE docker://$PROMOTED_IMAGE
  fi
fi
