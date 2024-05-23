#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`

while true; do
    $(hdfs dfs -test -d /user/julien.peloton/online/science/${NIGHT})
    if [[ $? == 0 ]]; then
        echo "Launching service"

        # LEASETIME must be computed by taking the difference between now and max end (5pm CEST)
        LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))

        ${FINK_HOME}/bin/fink start distribution \
            -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_distribute \
            -conf_distribution ${FINK_HOME}/conf_cluster/fink.conf.distribution_cluster \
            --night ${NIGHT} \
            --exit_after ${LEASETIME}
        exit
    fi
    DDATE=`date`
    echo "${DDATE}: no data yet. Sleeping..."
    sleep 300
done
