#!/bin/bash
set -e

source ~/.bash_profile

source ${FINK_HOME}/conf_cluster/fink.conf.ztf_stream2raw

NCORES=8

FINK_VERSION=`fink --version`
PYTHON_VERSION=`python -c "import platform; print(platform.python_version()[:3])"`
PYTHON_EXTRA_FILE="--py-files ${FINK_HOME}/dist/fink_broker-${FINK_VERSION}-                  py${PYTHON_VERSION}.egg"

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
    --packages ${FINK_PACKAGES} --jars ${FINK_JARS} ${PYTHON_EXTRA_FILE} \
    ${FINK_HOME}/bin/index_sso_resolver.py
~                                             
