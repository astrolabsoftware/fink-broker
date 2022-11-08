#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d" -d "now"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

# same entries as in the .conf
ZTF_ONLINE="/user/julien.peloton/online" # online_ztf_data_prefix
GCN_ONLINE="/user/julien.peloton/fink_grb/gcn_storage" # online_gcn_data_prefix
ZTFXGRB_OUTPUT="/user/julien.peloton/fink_grb/gcn_x_ztf" # online_grb_data_prefix

HDFS_HOME="/opt/hadoop-2/bin/"

while true; do

     LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))
     echo $LEASETIME

     $(hdfs dfs -test -d ${ZTF_ONLINE}/science/year=${YEAR}/month=${MONTH}/day=${DAY})
     if [[ $? == 0 ]]; then
        $(hdfs dfs -test -d ${GCN_ONLINE}/raw/year=${YEAR}/month=${MONTH}/day=${DAY})
        if [[ $? == 0 ]]; then
            echo "Launching service"

            # LEASETIME must be computed by taking the difference between now and max end
            LEASETIME=$(( `date +'%s' -d '17:00 today'` - `date +'%s' -d 'now'` ))

            nohup fink_grb join_stream online --config ${FINK_HOME}/conf_cluster/fink_grb.conf --night ${NIGHT} --exit_after ${LEASETIME} > ${FINK_HOME}/broker_logs/join_stream_${YEAR}${MONTH}${DAY}.log 2>&1
            break
        fi
     fi
     if [[ $LEASETIME -le 0 ]]
     then
        echo "exit scheduler, no data for this night."
        break
     fi
     DDATE=`date`
     echo "${DDATE}: no data yet. Sleeping..."
     sleep 5
done


# Removing the _spark_metadata and grb_checkpoint directories are important. The next time the stream begins
# will not work if these two directories exists.
$(hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/grb/_spark_metadata)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb/_spark_metadata"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb/_spark_metadata
fi

$(hdfs dfs -test -d ${ZTFXGRB_OUTPUT}/grb_checkpoint)
if [[ $? == 0 ]]; then
   echo "hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb_checkpoint"
   hdfs dfs -rm -r ${ZTFXGRB_OUTPUT}/grb_checkpoint
fi

echo "Exit science2grb properly"
exit
