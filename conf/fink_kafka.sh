#!/bin/bash
#
# Author: Abhishek Chauhan
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

message_help="""
Manage Fink's Kafka Server for Alert Distribution \n
USAGE:\n\n
  \t start\n
    \t\t Starts a Zookeeper and a Kafka Server \n\n
  \t stop \n
    \t\t Stops a running Kafka and Zookeeper Server \n\n
  \t -h, --help \n
    \t\t To view this help message \n\n
  \t --create-topic <TOPIC> \n
    \t\t Creates a topic named <TOPIC> if it does not already exist \n\n
"""
# Show help if no arguments is given
if [[ $1 == "" ]]; then
  echo -e $message_help
  exit 1
fi

case "$1" in
  "start")
      # Start the Zookeeper
      ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${FINK_HOME}/conf/kafka.zookeeper.properties > /dev/null 2>&1 &
      sleep 3

      # Start Kafka brokers
      ${KAFKA_HOME}/bin/kafka-server-start.sh ${FINK_HOME}/conf/kafka.server.properties > /dev/null 2>&1 &
      sleep 2

      echo "Kafka Server started"
      ;;
  "stop")
      # Stop Kafka brokers
      ${KAFKA_HOME}/bin/kafka-server-stop.sh ${FINK_HOME}/conf/kafka.server.properties > /dev/null 2>&1 &
      sleep 1

      # Stop Zookeeper
      ${KAFKA_HOME}/bin/zookeeper-server-stop.sh ${FINK_HOME}/conf/kafka.zookeeper.properties > /dev/null 2>&1 &
      sleep 1

      echo "Kafka Server stopped"
      ;;
  "--create-topic")
      if [[ $2 == "" ]]; then
        echo "$1 requires an argument (Topic Name)" >&2
        exit 1
      fi
      TOPIC=$2
      # create the topic if does not already exist
      ${KAFKA_HOME}/bin/kafka-topics.sh \
      --create \
      --if-not-exists \
      --partitions 1 \
      --replication-factor 1 \
      --topic $TOPIC \
      --zookeeper "localhost:2182"  >&1
      # Note --bootstrap-server <string of host:port> can't be used with --if-not-exists
      ;;
  "-h"|"--help")
      echo -e $message_help
      ;;
  -*)
      echo "unknown option: $1" >&2
      exit 1
      ;;
  *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
esac
