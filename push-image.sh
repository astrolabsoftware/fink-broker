#!/usr/bin/env bash

# Push image to Docker Hub or load it inside kind

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)


usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -k          development mode: load image in kind
    -d          do not push image to remote registry

Push image to remote registry and/or load it inside kind
EOD
}

kind=false
registry=true

CIUX_BUILD=${CIUX_BUILD:-""}

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

if [ -z "$CIUX_BUILD" ]; then
  CIUXCONFIG=$(ciux get configpath --selector "itest" $DIR)
  echo "Sourcing ciux config from $CIUXCONFIG"
  . $CIUXCONFIG
fi

if [ $kind = true ]; then
  kind load docker-image "$CIUX_IMAGE_URL"
fi
if [ $registry = true ]; then
  if [ $CIUX_BUILD = true ]; then
    echo "Push image $CIUX_PROMOTED_IMAGE_URL to registry"
    docker tag "$CIUX_IMAGE_URL" "$CIUX_PROMOTED_IMAGE_URL"
    docker push "$CIUX_PROMOTED_IMAGE_URL"
  else
    if command -v skopeo >/dev/null 2>&1; then
      echo "skopeo is already installed"
    else
      echo "skopeo not available, cannot copy image"
      exit 1
    fi
    echo "Add image tag $CIUX_PROMOTED_IMAGE_URL to $CIUX_IMAGE_URL"
    skopeo copy "docker://$CIUX_IMAGE_URL" "docker://$CIUX_PROMOTED_IMAGE_URL"
  fi
fi
