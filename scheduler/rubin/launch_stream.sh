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

SINFO="\xF0\x9F\x9B\x88"
SERROR="\xE2\x9D\x8C"
SSTOP="\xF0\x9F\x9B\x91"
SSTEP="\xF0\x9F\x96\xA7"
SDONE="\xE2\x9C\x85"

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
    --check_offsets)
      CHECK_OFFSET=true
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
  -h                Display this help
  -c                Path to configuration file. Default is at ${FINK_HOME}/conf/rubin/fink.conf.prod
  -night            Night to process, in the format YYYYMMDD. Default is today.
  -stop_at          Date to stop fink. Default is '20:00 today'
  --poll_only       If specified, only poll incoming stream. [NOT AVAILABLE YET]
  --enrich_only     If specified, only enrich polled data.
  --distribute_only If specified, only distribute enriched data.
  --check_offset    If specified, check offsets and exit (compatible only with --poll_only)

Examples:
  # Launch full Fink until 20:00 today Paris time
  ./launch_stream.sh

  # It is 14:00 Paris. Poll only for 1 hour
  ./launch_stream.sh --poll_only -stop_at 15:00 today

  # Check Kafka offsets
  ./launch_stream.sh --poll_only --check_offset

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
echo -e "${SINFO} Processing night ${NIGHT}"

# Check if the conf file exists
if [[ -f $conf ]]; then
  echo -e "${SINFO} Reading custom Fink configuration file from " $conf
else
  echo -e "${SINFO} Reading default Fink conf from " ${FINK_HOME}/conf/rubin/fink.conf.prod
  conf=${FINK_HOME}/conf/rubin/fink.conf.prod
fi

if [[ ! ${STOP_AT} ]]; then
  echo -e "${SINFO} No ending time specified, stopping at 20:00 today Paris time."
  STOP_AT='20:00 today'
fi

# stream2raw
if [[ ! ${ENRICH_ONLY} ]] && [[ ! ${DISTRIBUTE_ONLY} ]]; then
  # Force fetch schema
  ${FINK_HOME}/bin/fink start fetch_schema -s rubin

  if [[ ${CHECK_OFFSET} == true ]]; then
    CHECK_OFFSET="--check_offsets"
  fi

  nohup fink start stream2raw \
    -s rubin \
    -c ${conf} \
    -conf_stream2raw ${FINK_HOME}/conf/rubin/fink.conf.stream2raw \
    -night ${NIGHT} \
    -stop_at "`date +'%Y-%m-%d %H:%M' -d "${STOP_AT}"`" ${CHECK_OFFSET} > ${FINK_HOME}/broker_logs/stream2raw_${NIGHT}.log 2>&1 &
fi

# raw2science
if [[ ! ${POLL_ONLY} ]] && [[ ! ${DISTRIBUTE_ONLY} ]]; then
    echo -e "${SINFO} Launching raw2science"
    while : ; do
        # check folder exist
        $(hdfs dfs -test -d /user/fink/online/raw/${NIGHT})
        if [[ $? == 0 ]]; then
            # check folder is not empty
            isEmpty=$(hdfs dfs -count /user/fink/online/raw/${NIGHT} | awk '{print $2}')
            if [[ $isEmpty > 0 ]]; then
                echo -e "${SINFO} Data detected."
                echo -e "${SINFO} Waiting 60 seconds for one batch to complete before launching..."
                sleep 60
                echo -e "${SINFO} Launching raw2science service"

                # LEASETIME must be computed by taking the
                # difference between now and max end (8pm CE(S)T)
                LEASETIME=$(( `date +'%s' -d "${STOP_AT}"` - `date +'%s' -d 'now'` ))

                nohup ${FINK_HOME}/bin/fink start raw2science \
                    -s rubin \
                    -c ${conf} \
                    -driver-memory 4g -executor-memory 2g \
                    -spark-cores-max 24 -spark-executor-cores 1 \
                    -night ${NIGHT} \
                    -exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/raw2science_${NIGHT}.log 2>&1 &
                break
            fi
        fi
        DDATE=`date`
        echo -e "${SINFO} ${DDATE}: no data yet. Sleeping 60 seconds..."
        sleep 60
    done
fi

# distribution
if [[ ! ${POLL_ONLY} ]] && [[ ! ${ENRICH_ONLY} ]]; then
    echo -e "${SINFO} Launching distribution"
    while true; do
        # check folder exist
        $(hdfs dfs -test -d /user/fink/online/science/${NIGHT})
        if [[ $? == 0 ]]; then
            # check folder is not empty
            isEmpty=$(hdfs dfs -count /user/fink/online/science/${NIGHT} | awk '{print $2}')
            if [[ $isEmpty > 0 ]]; then
                echo -e "${SINFO} Data detected."
                echo -e "${SINFO} Waiting 60 seconds for one batch to complete before launching..."
                sleep 60
                echo -e "${SINFO} Launching distribution service..."
                # LEASETIME must be computed by taking the
                # difference between now and max end (8pm CEST)
                LEASETIME=$(( `date +'%s' -d "${STOP_AT}"` - `date +'%s' -d 'now'` ))

                nohup ${FINK_HOME}/bin/fink start distribute \
                    -s rubin \
                    -c ${conf} \
                    -conf_distribution ${FINK_HOME}/conf/rubin/fink.conf.distribution \
                    -night ${NIGHT} \
                    -driver-memory 4g -executor-memory 2g \
                    -spark-cores-max 4 -spark-executor-cores 1 \
                    -exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/distribute_${NIGHT}.log 2>&1 &
                break
            fi
        fi
        DDATE=`date`
        echo -e "${SINFO} ${DDATE}: no data yet. Sleeping 60 seconds..."
        sleep 60
    done
fi

