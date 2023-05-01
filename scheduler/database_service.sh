#!/bin/bash
# Copyright 2019-2022 AstroLab Software
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

NIGHT=`date +"%Y%m%d"`
YEAR=${NIGHT:0:4}
MONTH=${NIGHT:4:2}
DAY=${NIGHT:6:2}

$(hdfs dfs -test -d /user/julien.peloton/online/science/year=${YEAR}/month=${MONTH}/day=${DAY})
if [[ $? == 0 ]]; then
    echo "merge_and_clean"
    fink start merge -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_high --night ${NIGHT} > ${FINK_HOME}/broker_logs/merge_and_clean_${NIGHT}.log 2>&1

    echo "science_archival"
    fink start science_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/science_archival_${NIGHT}.log 2>&1

    echo "Update index tables"
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table jd_objectId > ${FINK_HOME}/broker_logs/index_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table pixel128_jd_objectId > ${FINK_HOME}/broker_logs/index_pixel128_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table pixel4096_jd_objectId > ${FINK_HOME}/broker_logs/index_pixel4096_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table pixel131072_jd_objectId > ${FINK_HOME}/broker_logs/index_pixel131072_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table class_jd_objectId > ${FINK_HOME}/broker_logs/index_class_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table upper_objectId_jd > ${FINK_HOME}/broker_logs/index_upper_objectId_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table ssnamenr_jd > ${FINK_HOME}/broker_logs/index_ssnamenr_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table uppervalid_objectId_jd > ${FINK_HOME}/broker_logs/index_uppervalid_objectId_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table tracklet_objectId > ${FINK_HOME}/broker_logs/index_tracklet_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table tns_jd_objectId --tns_folder ${FINK_HOME}/tns_logs > ${FINK_HOME}/broker_logs/index_tns_jd_objectId_${NIGHT}.log 2>&1

    echo "Call to Fink-Fat"
    fink_fat associations candidates --config $FINK_HOME/conf/fink_fat.conf --night ${YEAR}-${MONTH}-${DAY} --verbose > ${FINK_HOME}/broker_logs/fink_fat_association_${NIGHT}.log 2>&1
    fink_fat solve_orbit candidates local --config $FINK_HOME/conf/fink_fat.conf --verbose > ${FINK_HOME}/broker_logs/fink_fat_solve_orbit_${NIGHT}.log 2>&1

    echo "Push SSO candidates to HBase"
    fink start index_sso_cand_archival -c $FINK_HOME/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/index_sso_cand_archival_${NIGHT}.log 2>&1

    # echo "Push object tables to HBase"
    # fink start object_archival -c $FINK_HOME/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/object_archival_${NIGHT}.log 2>&1

    echo "Push TNS candidates"
    fink start push_to_tns -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --tns_folder ${FINK_HOME}/tns_logs > ${FINK_HOME}/broker_logs/tns_${NIGHT}.log 2>&1

    echo "Push Anomaly candidates"
    fink start anomaly_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/science_archival_${NIGHT}.log 2>&1

    echo "Update statistics"
    fink start stats -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/stats_${NIGHT}.log 2>&1

fi

# Check if data is on the archive
# If yes, delete temp ones
$(hdfs dfs -test -d /user/julien.peloton/archive/science/year=${YEAR}/month=${MONTH}/day=${DAY})
if [[ $? == 0 ]]; then
  hdfs dfs -rm -r /user/julien.peloton/online
fi
