#!/bin/bash

source ~/.bash_profile

NIGHT=`date +"%Y%m%d"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

#YEAR=`date +"%Y"`
#MONTH=`date +"%m"`
#DAY=`date +"%d"`

#NIGHT=${YEAR}${MONTH}${DAY}

$(hdfs dfs -test -d /user/julien.peloton/current/science/year=${YEAR}/month=${MONTH}/day=${DAY})
if [[ $? == 0 ]]; then
    echo "merge_and_clean"
    fink start merge -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_high --night ${NIGHT} > ${FINK_HOME}/broker_logs/merge_and_clean_${NIGHT}.log 2>&1

    echo "science_archival"
    fink start science_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/science_archival_${NIGHT}.log 2>&1

    echo "Update index tables"
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table jd_objectId > ${FINK_HOME}/broker_logs/index_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table pixel_jd > ${FINK_HOME}/broker_logs/index_pixel_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table class_jd_objectId > ${FINK_HOME}/broker_logs/index_class_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table upper_objectId_jd > ${FINK_HOME}/broker_logs/index_upper_objectId_jd_${NIGHT}.log 2>&1

    echo "Push TNS candidates"
    fink start push_to_tns -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --tns_folder ${FINK_HOME}/tns_logs > ${FINK_HOME}/broker_logs/tns_${NIGHT}.log 2>&1

    exit
fi
