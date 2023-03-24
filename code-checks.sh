#!/bin/bash

# Launch unit tests

# @author  Fabrice Jammes

# set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.build.sh

unittests=false
mypy=false

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -u            Run unit tests
  -m            Run mypy

Perform code analysis, unit testing on qserv code.

EOD
}

# Get the options
while getopts hum c ; do
    case $c in
        h) usage ; exit 0 ;;
        u) unittests=true ;;
        m) mypy=true ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ $unittests = true ]; then
  # Build ingest image
  $DIR/build.sh
  docker run -- "$INGEST_IMAGE" /ingest/bin/pytest.sh
fi
if [ $mypy = true ]; then
  mypy --config-file $DIR/rootfs/ingest/python/mypy.ini $DIR/rootfs/ingest/bin/replctl $DIR/rootfs/ingest/python/
fi
