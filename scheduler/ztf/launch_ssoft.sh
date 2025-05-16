#!/bin/bash
# Copyright 2019-2024 AstroLab Software
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

CURRDATE=`date +"%Y%m"`

fink_ssoft -s ztf --update-data > ${FINK_HOME}/broker_logs/ssoft_update_ephems_$CURRDATE.log 2>&1
fink_ssoft -s ztf --link-data > ${FINK_HOME}/broker_logs/ssoft_link_ephems_$CURRDATE.log 2>&1
fink_ssoft -s ztf --run-ssoft -model HG -version ${CURRDATE} > ${FINK_HOME}/broker_logs/ssoft_HG_$CURRDATE.log 2>&1
fink_ssoft -s ztf --run-ssoft -model HG1G2 -version ${CURRDATE} > ${FINK_HOME}/broker_logs/ssoft_HG1G2_$CURRDATE.log 2>&1
fink_ssoft -s ztf --run-ssoft -model SHG1G2 -version ${CURRDATE} > ${FINK_HOME}/broker_logs/ssoft_SHG1G2_$CURRDATE.log 2>&1
#fink_ssoft -s ztf --run-ssoft -model SSHG1G2

#sudo su livy <<'EOF'
#source ~/.bashrc
#YEAR=`date +"%Y"`
#MONTH=`date +"%m"`
#/opt/hadoop-2/bin/hdfs dfs -put ssoft_SSHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
#/opt/hadoop-2/bin/hdfs dfs -put ssoft_SHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
#/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG1G2_${YEAR}.${MONTH}.parquet SSOFT/
#/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG_${YEAR}.${MONTH}.parquet SSOFT/
#EOF
#
#mv ssoft_*.parquet /spark_mongo_tmp/julien.peloton/ssoft/
