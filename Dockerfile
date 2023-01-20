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
ARG spark_image_tag
FROM gitlab-registry.in2p3.fr/astrolabsoftware/fink/spark-py:${spark_image_tag}

ARG spark_uid=185
ENV spark_uid ${spark_uid}

# Install system-dependencies and prepare spark_uid user home directory
USER root

RUN apt-get update && \
    apt install -y --no-install-recommends wget git apt-transport-https ca-certificates gnupg-agent apt-utils build-essential && \
    rm -rf /var/cache/apt/*


# Main process will run as spark_uid 
ENV HOME /home/fink
RUN mkdir $HOME && chown ${spark_uid} $HOME
USER ${spark_uid}

WORKDIR $HOME

# Install python
ARG PYTHON_VERSION=py39_4.11.0
ENV PYTHON_VERSION=$PYTHON_VERSION
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-${PYTHON_VERSION}-Linux-x86_64.sh -O $HOME/miniconda.sh \
    && bash $HOME/miniconda.sh -b -p $HOME/miniconda

ENV PATH $HOME/miniconda/bin:$PATH
ENV FINK_HOME $HOME/fink-broker
ENV PYTHONPATH $FINK_HOME:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip
ENV PATH $FINK_HOME/bin:$PATH

RUN mkdir $FINK_HOME

# Avoid re-installing Python dependencies
# when fink-broker code changes
ADD install_python_deps.sh $FINK_HOME/
ADD requirements.txt $FINK_HOME/
RUN $FINK_HOME/install_python_deps.sh

RUN git clone -c advice.detachedHead=false --depth 1 -b "latest" --single-branch https://github.com/astrolabsoftware/fink-alert-schemas.git
ADD --chown=${spark_uid} . $FINK_HOME/


