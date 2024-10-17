#!/bin/bash
set -e

source ~/.bash_profile

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
    -model SHG1G2 $AGGREGATE $EXTRA_OPT > ${FINK_HOME}/broker_logs/ssoft_SHG1G2.log 2>&1

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
/opt/hadoop-2/bin/hdfs dfs -put ssoft_SHG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG1G2_${YEAR}.${MONTH}.parquet SSOFT/
/opt/hadoop-2/bin/hdfs dfs -put ssoft_HG_${YEAR}.${MONTH}.parquet SSOFT/
EOF

mv ssoft_*.parquet /spark_mongo_tmp/julien.peloton/ssoft/
