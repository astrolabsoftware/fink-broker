#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ARG base_image
FROM ${base_image}

ARG spark_uid=185
ENV spark_uid=${spark_uid}

# The base image already contains all system dependencies, Python setup,
# spark_uid, and input_survey environment variables
# Just add the fink-broker code

# Main process will run as spark_uid
ENV HOME=/home/fink
WORKDIR $HOME

ENV FINK_JARS=""
ENV FINK_PACKAGES=""

ENV PATH=$FINK_BROKER_ROOT/miniconda/bin:$PATH
ENV FINK_HOME=$HOME/fink-broker
ENV PYTHONPATH=$FINK_HOME:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip
ENV PATH=$FINK_HOME/bin:$PATH

ENV FINK_ALERT_SCHEMAS_VERSION v0.0.3

RUN git clone -c advice.detachedHead=false --depth 1 --filter=blob:none --sparse --branch "${FINK_ALERT_SCHEMAS_VERSION}" https://github.com/astrolabsoftware/fink-alert-schemas.git \
    && cd fink-alert-schemas \
    && git sparse-checkout set ztf


ADD --chown=${spark_uid} . $FINK_HOME/
