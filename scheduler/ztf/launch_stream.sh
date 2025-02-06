#!/bin/bash
# Copyright 2019-2025 AstroLab Software
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
source ~/.bash_profile

# Grab the command line arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h)
      HELP_ON_SERVICE="-h"
      shift 1
      ;;
    -c)
      conf="$2"
      shift 2
      ;;
    --poll_only)
      POLL_ONLY=true
      shift 2
      ;;
    --enrich_only)
      ENRICH_ONLY=true
      shift 2
      ;;
    --distribute_only)
      DISTRIBUTE_ONLY=true
      shift 2
      ;;
    -night)
      NIGHT="$2"
      shift 2
      ;;
    -stop_at)
      STOP_AT="$2"
      shift 2
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
done

if [[ ! $NIGHT ]]; then
  # Current night
  NIGHT=`date +"%Y%m%d"`
fi
echo "Processing night ${NIGHT}"

# Check if the conf file exists
if [[ -f $conf ]]; then
  echo "Reading custom Fink configuration file from " $conf
else
  echo "Reading default Fink conf from " ${FINK_HOME}/conf/${SURVEY}/fink.conf.prod
  conf=${FINK_HOME}/conf/${SURVEY}/fink.conf.prod
fi

if [[ ! ${STOP_AT} ]]; then
  STOP_AT='20:00 today'
fi

# stream2raw
if [[ ! ${ENRICH_ONLY} ]] && [[ ! ${DISTRIBUTE_ONLY} ]]; then
    echo "Launching stream2raw"

    LEASETIME=$(( `date +'%s' -d "${STOP_AT}"` - `date +'%s' -d 'now'` ))

    nohup ${FINK_HOME}/bin/fink start stream2raw \
        -s ztf \
        -c ${FINK_HOME}/conf/ztf/fink.conf.prod \
        -topic "ztf_${NIGHT}.*" \
        -night $NIGHT \
        -driver-memory 4g -executor-memory 2g \
        -spark-cores-max 4 -spark-executor-cores 1 \
        -exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/stream2raw_${NIGHT}.log 2>&1 &
fi

# raw2science
if [[ ! ${POLL_ONLY} ]] && [[ ! ${DISTRIBUTE_ONLY} ]]; then
    echo "Launching raw2science"
    while : ; do
        # check folder exist
        $(hdfs dfs -test -d /user/julien.peloton/online/raw/${NIGHT})
        if [[ $? == 0 ]]; then
            # check folder is not empty
            isEmpty=$(hdfs dfs -count /user/julien.peloton/online/raw/${NIGHT} | awk '{print $2}')
            if [[ $isEmpty > 0 ]]; then
                echo "Data detected."
                echo "Waiting 60 seconds for one batch to complete before launching..."
                sleep 60
                echo "Launching service"

                # LEASETIME must be computed by taking the
                # difference between now and max end (8pm CE(S)T)
                LEASETIME=$(( `date +'%s' -d "${STOP_AT}"` - `date +'%s' -d 'now'` ))

                nohup ${FINK_HOME}/bin/fink start raw2science \
                    -s ztf \
                    -c ${FINK_HOME}/conf/ztf/fink.conf.prod \
                    -driver-memory 4g -executor-memory 2g \
                    -spark-cores-max 8 -spark-executor-cores 1 \
                    -night ${NIGHT} \
                    -exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/raw2science_${NIGHT}.log 2>&1 &
                break
            fi
        fi
        DDATE=`date`
        echo "${DDATE}: no data yet. Sleeping 60 seconds..."
        sleep 60
    done
fi

# distribution
if [[ ! ${POLL_ONLY} ]] && [[ ! ${ENRICH_ONLY} ]]; then
    echo "Launching distribution"
    while true; do
        # check folder exist
        $(hdfs dfs -test -d /user/julien.peloton/online/science/${NIGHT})
        if [[ $? == 0 ]]; then
            # check folder is not empty
            isEmpty=$(hdfs dfs -count /user/julien.peloton/online/science/${NIGHT} | awk '{print $2}')
            if [[ $isEmpty > 0 ]]; then
                echo "Data detected."
                echo "Waiting 60 seconds for one batch to complete before launching..."
                sleep 60
                echo "Launching service..."
                # LEASETIME must be computed by taking the
                # difference between now and max end (8pm CEST)
                LEASETIME=$(( `date +'%s' -d "${STOP_AT}"` - `date +'%s' -d 'now'` ))

                nohup ${FINK_HOME}/bin/fink start distribute \
                    -s ztf \
                    -c ${FINK_HOME}/conf/fink.conf.prod \
                    -conf_distribution ${FINK_HOME}/conf/ztf/fink.conf.distribution \
                    -night ${NIGHT} \
                    -driver-memory 4g -executor-memory 2g \
                    -spark-cores-max 4 -spark-executor-cores 1 \
                    -exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/distribute_${NIGHT}.log 2>&1 &
                break
            fi
        fi
        DDATE=`date`
        echo "${DDATE}: no data yet. Sleeping 60 seconds..."
        sleep 60
    done
fi

