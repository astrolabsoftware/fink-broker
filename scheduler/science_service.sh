#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now + 1 days"`

while true; do
    # check folder exist
    $(hdfs dfs -test -d /user/julien.peloton/online/raw/${NIGHT})
    if [[ $? == 0 ]]; then
	# check folder is not empty
	isEmpty=$(hdfs dfs -count /user/julien.peloton/online/raw/${NIGHT} | awk '{print $2}')
	if [[ $isEmpty > 0 ]]; then
            echo "Launching service"

            # LEASETIME must be computed by taking the difference between now and max end (5pm CE(S)T)
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))

            ${FINK_HOME}/bin/fink start raw2science \
                -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_raw2science \
                --night ${NIGHT} \
                --exit_after ${LEASETIME}
            exit
        fi
    fi
    DDATE=`date`
    echo "${DDATE}: no data yet. Sleeping..."
    sleep 300
done
