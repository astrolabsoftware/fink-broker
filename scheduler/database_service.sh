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

$(hdfs dfs -test -d /user/julien.peloton/online/science/${NIGHT})
if [[ $? == 0 ]]; then
    echo "Download latest TNS data"
    fink start tns_resolver -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --tns_folder ${FINK_HOME}/tns_logs --tns_raw_output /spark_mongo_tmp/julien.peloton > ${FINK_HOME}/broker_logs/tns_resolver_${NIGHT}.log 2>&1

    echo "merge_and_clean"
    fink start merge -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_high --night ${NIGHT} > ${FINK_HOME}/broker_logs/merge_and_clean_${NIGHT}.log 2>&1

    echo "science_archival"
    fink start science_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/science_archival_${NIGHT}.log 2>&1

    echo "images_archival"
    fink start images_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/images_archival_${NIGHT}.log 2>&1

    echo "Update index tables"
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table jd_objectId > ${FINK_HOME}/broker_logs/index_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table pixel128_jdstarthist_objectId > ${FINK_HOME}/broker_logs/index_pixel128_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table class_jd_objectId > ${FINK_HOME}/broker_logs/index_class_jd_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table upper_objectId_jd > ${FINK_HOME}/broker_logs/index_upper_objectId_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table ssnamenr_jd > ${FINK_HOME}/broker_logs/index_ssnamenr_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table uppervalid_objectId_jd > ${FINK_HOME}/broker_logs/index_uppervalid_objectId_jd_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table tracklet_objectId > ${FINK_HOME}/broker_logs/index_tracklet_objectId_${NIGHT}.log 2>&1
    fink start index_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --index_table tns_jd_objectId --tns_folder ${FINK_HOME}/tns_logs > ${FINK_HOME}/broker_logs/index_tns_jd_objectId_${NIGHT}.log 2>&1

fi

# Check if data is on the archive
# If yes, delete temp ones
$(hdfs dfs -test -d /user/julien.peloton/archive/science/year=${YEAR}/month=${MONTH}/day=${DAY})
if [[ $? == 0 ]]; then
  # Remove data path
  hdfs dfs -rm -r /user/julien.peloton/online/raw/${NIGHT}
  hdfs dfs -rm -r /user/julien.peloton/online/science/${NIGHT}

  # Remove checkpoints
  hdfs dfs -rm -r /user/julien.peloton/online/raw_checkpoint/${NIGHT}
  hdfs dfs -rm -r /user/julien.peloton/online/science_checkpoint/${NIGHT}
  hdfs dfs -rm -r /user/julien.peloton/online/kafka_checkpoint/${NIGHT}

  # Remove checkpoints for fink-mm
  hdfs dfs -rm -r /user/julien.peloton/fink_mm/gcn_x_ztf/online/_spark_metadata
  hdfs dfs -rm -r /user/julien.peloton/fink_mm/gcn_x_ztf/online_checkpoint
  hdfs dfs -rm -r /user/julien.peloton/fink_mm/gcn_x_ztf/mm_distribute_checkpoint

fi
