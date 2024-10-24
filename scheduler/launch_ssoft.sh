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

NCORES=100
AGGREGATE="--pre_aggregate_data"
EXTRA_OPT=

# In case of trouble
#AGGREGATE=""
#EXTRA_OPT="-version 2023.12"

FINK_VERSION=`fink --version`
PYTHON_VERSION=`python -c "import platform; print(platform.python_version()[:3])"`
PYTHON_EXTRA_FILE="--py-files ${FINK_HOME}/dist/fink_broker-${FINK_VERSION}-py${PYTHON_VERSION}.egg"

spark-submit \
    --master mesos://vm-75063.lal.in2p3.fr:5050 \
    --conf spark.mesos.principal=$MESOS_PRINCIPAL \
    --conf spark.mesos.secret=$MESOS_SECRET \
    --conf spark.mesos.role=$MESOS_ROLE \
    --conf spark.executorEnv.HOME='/home/julien.peloton'\
    --driver-memory 8G --executor-memory 4G \
    --conf spark.cores.max=$NCORES --conf spark.executor.cores=2 \
    --conf spark.sql.execution.arrow.pyspark.enabled=true\
    --conf spark.kryoserializer.buffer.max=512m\
    ${PYTHON_EXTRA_FILE}\
    ${FINK_HOME}/bin/generate_ssoft.py \
    -model SSHG1G2 $AGGREGATE $EXTRA_OPT > ${FINK_HOME}/broker_logs/ssoft_SSHG1G2.log 2>&1

spark-submit \
    --master mesos://vm-75063.lal.in2p3.fr:5050 \
    --conf spark.mesos.principal=$MESOS_PRINCIPAL \
    --conf spark.mesos.secret=$MESOS_SECRET \
    --conf spark.mesos.role=$MESOS_ROLE \
    --conf spark.executorEnv.HOME='/home/julien.peloton'\
    --driver-memory 8G --executor-memory 4G \
    --conf spark.cores.max=$NCORES --conf spark.executor.cores=2 \
    --conf spark.sql.execution.arrow.pyspark.enabled=true\
    --conf spark.kryoserializer.buffer.max=512m\
    ${PYTHON_EXTRA_FILE}\
    ${FINK_HOME}/bin/generate_ssoft.py \
    -model SHG1G2 $EXTRA_OPT > ${FINK_HOME}/broker_logs/ssoft_SHG1G2.log 2>&1

spark-submit \
    --master mesos://vm-75063.lal.in2p3.fr:5050 \
    --conf spark.mesos.principal=$MESOS_PRINCIPAL \
    --conf spark.mesos.secret=$MESOS_SECRET \
    --conf spark.mesos.role=$MESOS_ROLE \
    --conf spark.executorEnv.HOME='/home/julien.peloton'\
    --driver-memory 8G --executor-memory 4G \
    --conf spark.cores.max=$NCORES --conf spark.executor.cores=2 \
    --conf spark.sql.execution.arrow.pyspark.enabled=true\
    --conf spark.kryoserializer.buffer.max=512m\
    ${PYTHON_EXTRA_FILE}\
    ${FINK_HOME}/bin/generate_ssoft.py \
    -model HG1G2 $EXTRA_OPT > ${FINK_HOME}/broker_logs/ssoft_HG1G2.log 2>&1

spark-submit \
    --master mesos://vm-75063.lal.in2p3.fr:5050 \
    --conf spark.mesos.principal=$MESOS_PRINCIPAL \
    --conf spark.mesos.secret=$MESOS_SECRET \
    --conf spark.mesos.role=$MESOS_ROLE \
    --conf spark.executorEnv.HOME='/home/julien.peloton'\
    --driver-memory 8G --executor-memory 4G \
    --conf spark.cores.max=$NCORES --conf spark.executor.cores=2 \
    --conf spark.sql.execution.arrow.pyspark.enabled=true\
    --conf spark.kryoserializer.buffer.max=512m\
    ${PYTHON_EXTRA_FILE}\
    ${FINK_HOME}/bin/generate_ssoft.py \
    -model HG $EXTRA_OPT > ${FINK_HOME}/broker_logs/ssoft_HG.log 2>&1

sudo su livy <<'EOF'
source ~/.bashrc
YEAR=`date +"%Y"`
MONTH=`date +"%m"`
/opt/hadoop-2/bin/hdfs dfs -put ssoft_SSHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-2/bin/hdfs dfs -put ssoft_SHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG_${YEAR}.${MONTH}.parquet SSOFT/
EOF

mv ssoft_*.parquet /spark_mongo_tmp/julien.peloton/ssoft/
