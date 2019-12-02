FROM ubuntu:16.04

ENV SPARK_VERSION "2.4.4"
ENV KAFKA_VERSION "2.2.0"
ENV USRLIBS /home/libs

WORKDIR $USRLIBS

# install base deps
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  git apt-utils software-properties-common axel vim wget bzip2 curl \
  apt-transport-https ca-certificates gnupg-agent \
  && rm -rf /var/lib/apt/lists/*

# Install docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
  && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  && apt-get update \
  && apt-get install -y docker-ce docker-ce-cli containerd.io

# Install docker-compose
RUN curl -L https://github.com/docker/compose/releases/download/1.18.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose \
  && chmod +x /usr/local/bin/docker-compose

# Install Java 8
RUN add-apt-repository -y ppa:openjdk-r/ppa \
  && apt-get -qq update \
  && apt-get install -y openjdk-8-jdk --no-install-recommends \
  && update-java-alternatives -s java-1.8.0-openjdk-amd64

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Install Apache Kafka
WORKDIR $USRLIBS

RUN wget https://www.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz -O kafka.tgz \
 && mkdir -p $USRLIBS/kafka \
 && tar -xzf $USRLIBS/kafka.tgz -C $USRLIBS/kafka --strip-components 1 \
 && rm $USRLIBS/kafka.tgz

ENV KAFKA_HOME $USRLIBS/kafka

# Install Apache Spark
RUN axel --quiet http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
  && tar -xf $USRLIBS/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
  && rm $USRLIBS/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

ENV SPARK_HOME $USRLIBS/spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARKLIB ${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.7-src.zip
ENV PYTHONPATH "${SPARKLIB}:${FINK_HOME}:$PYTHONPATH"
ENV PATH "${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN echo "spark.yarn.jars=${SPARK_HOME}/jars/*.jar" > ${SPARK_HOME}/conf/spark-defaults.conf

# Install Python
WORKDIR $USRLIBS/anaconda3

RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
 && bash $USRLIBS/anaconda3/miniconda.sh -b -p $USRLIBS/anaconda3/miniconda

ENV PATH $USRLIBS/anaconda3/miniconda/bin:$PATH

# Install the broker deps
# fink-filters and fink-science are not installed!
COPY requirements-docker.txt $USRLIBS/anaconda3/requirements-docker.txt
RUN pip install --upgrade pip setuptools wheel \
  && pip install ipython \
  && pip install -r $USRLIBS/anaconda3/requirements-docker.txt

# Install the simulator
WORKDIR /home
RUN git clone https://github.com/astrolabsoftware/fink-alert-simulator.git

ENV FINK_ALERT_SIMULATOR /home/fink-alert-simulator

# Here we assume the container will be ran with
# --v $HOST_PATH_TO/fink-package:/home/fink-package
ENV FINK_HOME /home/fink-broker
ENV FINK_SCIENCE /home/fink-science
ENV FINK_FILTERS /home/fink-filters

ENV PYTHONPATH $FINK_HOME:$FINK_ALERT_SIMULATOR:$FINK_SCIENCE:$FINK_FILTERS:$PYTHONPATH
ENV PATH $FINK_HOME/bin:$FINK_ALERT_SIMULATOR/bin:$PATH

WORKDIR $FINK_HOME
