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

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

message_help="""
Install Python dependencies for fink-broker through pip
Usage:
    ./install_python_deps.sh
"""

# Dependencies
pip install --no-cache-dir -r $DIR/requirements.txt

# Fink-fat
pip install --no-dependencies git+https://github.com/FusRoman/fink-fat.git@6dcebaee620abb9fb49ea1db9c254300becbea04

# Fink_GRB
pip install --no-dependencies git+https://github.com/FusRoman/Fink_GRB.git@71e8b4d440da15911a37bc6db4908fefc94ccc6a

# Installation of torch without GPU support (lighter)
pip install --no-cache-dir torch==1.12.0+cpu -f https://download.pytorch.org/whl/torch_stable.html

# Installation of astronet
pip install --no-dependencies git+https://github.com/tallamjr/astronet.git
pip install george
pip install imbalanced-learn==0.7.0
pip install optuna==2.3.0
pip install tensorflow==2.8.0
