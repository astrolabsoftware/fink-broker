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
      shift 1
      ;;
    --enrich_only)
      ENRICH_ONLY=true
      shift 1
      ;;
    --distribute_only)
      DISTRIBUTE_ONLY=true
      shift 1
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

__usage="
Fink real-time operations: poll, enrich, distribute

Usage: $(basename $0) [OPTIONS]

Options:
  -h			Display this help
  -c			Path to configuration file. Default is at ${FINK_HOME}/conf/ztf/fink.conf.prod
  -night		Night to process, in the format YYYYMMDD. Default is today.
  -stop_at		Date to stop fink. Default is '20:00 today'  
  --poll_only		If specified, only poll incoming stream.
  --enrich_only		If specified, only enrich polled data.
  --distribute_only	If specified, only distribute enriched data.

Examples:
  # Launch full Fink until 20:00 today Paris time
  ./launch_stream.sh

  # It is 14:00 Paris. Poll only for 1 hour
  ./launch_stream.sh --poll_only -stop_at 15:00 today 

  # Extract science until the end of the night 
  ./launch_stream.sh --enrich_only

  # Distribute until 5pm
  ./launch_stream.sh --distribute_only -stop_at 17:00 today

  # 8:00 AM. Reprocess entirely another night in a couple of hours
  ./launch_stream.sh -night 20241231 -stop_at 10:00 today 
"


if [[ ${HELP_ON_SERVICE} == "-h" ]]; then
  echo -e "$__usage"
  exit 1
fi

if [[ ! $NIGHT ]]; then
  # Current night
  NIGHT=`date +"%Y%m%d"`
fi
echo "Processing night ${NIGHT}"

# Check if the conf file exists
if [[ -f $conf ]]; then
  echo "Reading custom Fink configuration file from " $conf
else
  echo "Reading default Fink conf from " ${FINK_HOME}/conf/ztf/fink.conf.prod
  conf=${FINK_HOME}/conf/ztf/fink.conf.prod
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
        -c ${conf} \
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
                    -c ${conf} \
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
                    -c ${conf} \
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

