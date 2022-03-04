#!/bin/bash
# Copyright 2019-2022 AstroLab Software
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
set -e

message_help="""
Install Python dependencies for fink-broker through pip\n\n
Usage:\n
    \t./install_python_deps.sh [--astronet-token]\n\n

You need a valid GitHub token to install astronet, otherwise
it will be skipped (but you will not be able to use the T2 module).
"""

# Grab the command line arguments
TOKEN=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --astronet-token)
        TOKEN="$2"
        shift 2
        ;;
    -h)
        echo -e $message_help
        exit
        ;;
  esac
done

# Dependencies
pip install -r requirements.txt

# Installation of torch without GPU support (lighter)
pip install torch==1.6.0+cpu -f https://download.pytorch.org/whl/torch_stable.html

# Installation of astronet
if [[ $TOKEN != "" ]]; then
    pip install git+https://${TOKEN}@github.com/tallamjr/astronet.git
else
    echo "You did not provide a token for astronet -- installation skipped"
fi
