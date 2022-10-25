#!/bin/bash
# Copyright 2022 Fink Software
# Author: Roman Le Montagner
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
# limitations under the License

source ~/.bash_profile

# Config 
CONFIG=$FINK_HOME/conf/fink_grb.conf

NIGHT=`date +"%Y%m%d" -d "now"`

nohup fink_grb join_stream offline --night=${NIGHT} --config ${CONFIG} > $FINK_HOME/broker_logs/offline_grb_${NIGHT}.log &
