#!/bin/bash

# user:julien.peloton
# Launch the broker at 11.45pm CE(S)T (9.45pm UTC) on day D
# to collect and process the night D+1.
# Usually, ZTF observations start around 3am CE(S)T on D+1 (2am UTC).
# Broker will be up from 11.45pm day D to 8pm day D+1 CE(S)T.
# Then database operations take place between 8pm and 9pm CE(S)T, and another night starts.

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`

# 20 hours lease
LEASETIME=72900

# stream2raw
nohup fink start stream2raw \
    -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_stream2raw \
    --topic "ztf_${NIGHT}.*" \
    --night $NIGHT \
    --exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/stream2raw_${NIGHT}.log 2>&1 &

# raw2science
nohup ${FINK_HOME}/scheduler/science_service.sh > ${FINK_HOME}/broker_logs/raw2science_${NIGHT}.log 2>&1 &

# disrtribute
nohup ${FINK_HOME}/scheduler/distribution_service.sh > ${FINK_HOME}/broker_logs/distribute_${NIGHT}.log 2>&1 &
