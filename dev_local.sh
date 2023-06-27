#!/bin/bash

# Run fink_broker container in interactive mode inside k8s
# Local source code is mounted inside the container

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

CONTAINER="fink-broker"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message

Run $CONTAINER container in development mode

EOD
}

# Get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

echo "Running in development mode"
FINK_HOME="$DIR"
MOUNTS="-v $HOME:$HOME"
MOUNTS="$MOUNTS --volume /etc/group:/etc/group:ro -v /etc/passwd:/etc/passwd:ro"

echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
echo "   Welcome in $CONTAINER developement container"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"

docker run --net=host --name "$CONTAINER" \
  --env FINK_HOME="$FINK_HOME" \
  --env HOME="$HOME" \
  --user=$(id -u):$(id -g $USER) \
  -it $MOUNTS --rm -w "$HOME" "$IMAGE" bash
