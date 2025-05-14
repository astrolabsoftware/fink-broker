#!/bin/bash
# Copyright 2025 AstroLab Software
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

source ~/.bash_profile

# re-download latest information
export ROCKS_CACHE_DIR="no-cache"

for OBSCODE in "T05 T08 W68 M22"; do
	fink_ssoft -s atlas -c ${FINK_HOME}/conf/ztf/fink.conf.prod -observer ${OBSCODE} --reconstruct-data > ${FINK_HOME}/broker_logs/atlas_ephemerides_${OBSCODE}.log 2>&1
done
