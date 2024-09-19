#!/bin/bash

# Clone Fink-broker source code in a tempory directory and run e2e test
# designed to be run as a cron job.

set -euxo pipefail

export TOKEN=$(cat /home/fink-ci/.token)
export USER="fink-ci"
repo_url="https://github.com/astrolabsoftware/fink-broker.git"
tmpdir=$(mktemp -d --suffix -fink-broker-ci)
repo=$tmpdir/fink-broker
science_opt=""
cleanup_opt=""

# Set go path according to go install method
PATH=$HOME/go/bin:/usr/local/go/bin:/usr/local/bin:$PATH

branchname="master"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message
  -c            Cleanup the cluster if the tests are successful
  -s            Use the science algorithms during the tests
  -b <branch>   Branch name to clone (default: master)

Clone Fink-broker source code in a tempory directory and run e2e test
designed to be run as a cron job.

EOD
}

# Get the options
while getopts hcsb: c ; do
    case $c in
        h) usage ; exit 0 ;;
        b) branchname=$OPTARG ;;
        c) cleanup_opt="-c" ;;
        s) science_opt="-s" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

# Clone the repository
git clone --single-branch $repo_url $repo --branch $branchname

# Run fink ci in science mode
$repo/e2e/run.sh $cleanup_opt $science_opt
