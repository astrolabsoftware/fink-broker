#!/bin/bash
# Copyright 2019-2025 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
source ~/.bash_profile

SINFO="\xF0\x9F\x9B\x88"
SERROR="\xE2\x9D\x8C"
SSTOP="\xF0\x9F\x9B\x91"
SSTEP="\xF0\x9F\x96\xA7"
SDONE="\xE2\x9C\x85"

# Grab the command line arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h)
      HELP_ON_SERVICE="-h"
      shift 1
      ;;
    -s)
      SURVEY=$2
      shift 2
      ;;
    -single_package)
      SINGLE_PACKAGE=$2
      shift 2
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 1
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

__usage="
Install Fink Python dependencies from ${FINK_HOME}/deps/<survey>

Usage: $(basename $0) [OPTIONS]

Options:
  -h                Display this help
  -s                Survey name (ztf, rubin). Default is ztf.
  -single_package   If specified, install only a single package on all machines

Examples:
  # Install all ZTF dependencies at VirtualData
  ./install_python_deps.sh -s ztf

  # Install fink-utils only at CC for Rubin processing
  ./install_python_deps.sh -s rubin -single_package fink-utils==0.6
"

if [[ ${HELP_ON_SERVICE} == "-h" ]]; then
  echo -e "$__usage"
  exit 1
fi

if [[ $SURVEY == "" ]]; then
  echo -e "${SERROR} You need to specify a survey, e.g. -s ztf"
  exit 1
fi

PYTHON_VERSION=`python -c "import platform; print(platform.python_version())"`
echo -e "${SINFO} Using Python ${PYTHON_VERSION}"

function install_python_deps {
  # uninstall GitHub installation
    pip uninstall -y fink-science supernnova actsnclass actsnfink

    # Science installation
    pip install -r /tmp/requirements.txt -r /tmp/requirements-science.txt
    pip install -r /tmp/requirements-science-no-deps.txt --no-deps
}

if [[ ! ${SINGLE_PACKAGE} ]]; then
    echo -e "${SINFO} Copying requirements on all machines"
    pscp.pssh -p 12 -h ${FINK_HOME}/conf/${SURVEY}/spark_ips ${FINK_HOME}/deps/${SURVEY}/requirements*txt /tmp

    echo -e "${SINFO} Installing packages on all machines from ${FINK_HOME}/conf/${SURVEY}/spark_ips"
    pssh -p 12 -t 100000000 -o /tmp/python_deps_out/ -e /tmp/python_deps_err/ -h ${FINK_HOME}/conf/${SURVEY}/spark_ips "$(typeset -f install_python_deps); install_python_deps"
else
    pssh -p 12 -t 100000000 -o /tmp/python_deps_out/ -e /tmp/python_deps_err/ -h ${FINK_HOME}/conf/${SURVEY}/spark_ips pip install ${SINGLE_PACKAGE}
fi
