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

FOLDER=/spark_mongo_tmp/julien.peloton/ssoft/

#mv ssoft_*.parquet ${FOLDER}/

sudo su livy <<'EOF'
source ~/.bashrc
FOLDER=/spark_mongo_tmp/julien.peloton/ssoft/
YEAR=`date +"%Y"`
MONTH=`date +"%m"`
#/opt/hadoop-3/bin/hdfs dfs -put ${FOLDER}/ssoft_SSHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-3/bin/hdfs dfs -put ${FOLDER}/ssoft_SHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-3/bin/hdfs dfs -put ${FOLDER}/ssoft_HG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-3/bin/hdfs dfs -put ${FOLDER}/ssoft_HG_${YEAR}.${MONTH}.parquet SSOFT/
EOF
