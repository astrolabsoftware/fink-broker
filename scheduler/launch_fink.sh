#!/bin/bash

# user:julien.peloton
# Launch the broker at 10pm CE(S)T (9pm UTC) on day D
# to collect and process the night D+1.
# Usually, ZTF observations start around 3am CE(S)T on D+1 (2am UTC).
# Broker will be up from 10pm day D to 9pm day D+1 CE(S)T.
# Then database operations take place between 9pm and 10pm CE(S)T, and another night starts.

source ~/.bash_profile

# Next day
#YEAR=`date +"%Y"`
#MONTH=`date +"%m"`
#DAY=`date +"%d" -d "now + 1 days"`
#NIGHT=${YEAR}${MONTH}${DAY}
NIGHT=`date +"%Y%m%d" -d "now + 1 days"`

# 23 hours lease
LEASETIME=82800

# stream2raw
nohup fink start stream2raw -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_stream2raw --topic "ztf_${NIGHT}.*" --exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/stream2raw_${NIGHT}.log 2>&1 &

# raw2science
nohup ${FINK_HOME}/science_service.sh > ${FINK_HOME}/broker_logs/raw2science_${NIGHT}.log 2>&1 &

# disrtribute
nohup ${FINK_HOME}/distribution_service.sh > ${FINK_HOME}/broker_logs/distribute_${NIGHT}.log 2>&1 &
