#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`

while true; do
    # check folder exist
    $(hdfs dfs -test -d /user/julien.peloton/online/science/${NIGHT})
    if [[ $? == 0 ]]; then
        # check folder is not empty
        isEmpty=$(hdfs dfs -count /user/julien.peloton/online/science/${NIGHT} | awk '{print $2}')
        if [[ $isEmpty > 0 ]]; then
	    echo "Data detected. Waiting 300 seconds for one batch to complete before launching..."
	    sleep 300
            echo "Launching service..."
            # LEASETIME must be computed by taking the difference between now and max end (5pm CEST)
            LEASETIME=$(( `date +'%s' -d '20:00 today'` - `date +'%s' -d 'now'` ))

            ${FINK_HOME}/bin/fink start distribution \
		-s ztf \
                -c ${FINK_HOME}/conf/fink.conf.prod \
                -conf_distribution ${FINK_HOME}/conf/ztf/fink.conf.distribution_cluster \
                -night ${NIGHT} \
		-driver-memory 4g -executor-memory 2g \
		-spark-cores-max 4 -spark-executor-cores 1 \
                -exit_after ${LEASETIME}
            exit
        fi
    fi
    DDATE=`date`
    echo "${DDATE}: no data yet. Sleeping 300 seconds..."
    sleep 300
done
