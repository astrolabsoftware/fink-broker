#!/bin/bash

# nohup ${ROOTPATH}/science_service.sh > ${ROOTPATH}/broker_logs/raw2science_${NIGHT}.log 2>&1 &

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}
#YEAR=`date +"%Y"`
#MONTH=`date +"%m"`
#DAY=`date +"%d" -d "now + 1 days"`
#DAY=`date +"%d"`

while true; do
    #NIGHT=${YEAR}${MONTH}${DAY}

    $(hdfs dfs -test -d /user/julien.peloton/current/raw/year=${YEAR}/month=${MONTH}/day=${DAY})
    if [[ $? == 0 ]]; then
        echo "Launching service"
        #echo "fink start raw2science -c conf_cluster/fink.conf.ztf_raw2science --night ${NIGHT} --exit_after ${LEASETIME}"

        # LEASETIME must be computed by taking the difference between now and max end (9pm CE(S)T)
        LEASETIME=$(( `date +'%s' -d '21:00 today'` - `date +'%s' -d 'now'` ))

        ${FINK_HOME}/bin/fink start raw2science -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_raw2science --night ${NIGHT} --exit_after ${LEASETIME} #> ${FINK_HOME}/raw2science_${NIGHT}.log 2>&1 &
        exit
    fi
    DDATE=`date`
    echo "${DDATE}: no data yet. Sleeping..."
    sleep 300
done
