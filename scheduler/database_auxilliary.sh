#!/bin/bash
# Copyright 2024 AstroLab Software
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

$(hdfs dfs -test -d /user/julien.peloton/archive/science/year=${YEAR}/month=${MONTH}/day=${DAY})
if [[ $? == 0 ]]; then
    echo "Push TNS candidates"
    fink start push_to_tns -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} --tns_folder ${FINK_HOME}/tns_logs > ${FINK_HOME}/broker_logs/tns_${NIGHT}.log 2>&1

    echo "Push Anomaly candidates"
    fink start anomaly_archival -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/anomaly_detection_${NIGHT}.log 2>&1

    echo "Push Active Learning loop candidates"
    fink start al_loop -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/al_loop_${NIGHT}.log 2>&1

    echo "Push Hostless candidates"
    fink start hostless -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/hostless_${NIGHT}.log 2>&1

    echo "Send Dwarf AGN candidates"
    fink start dwarf_agn -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/dwarf_agn_${NIGHT}.log 2>&1

    echo "Send Known TDE candidates"
    fink start known_tde -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/known_tde_${NIGHT}.log 2>&1

    echo "Update statistics"
    fink start stats -c ${FINK_HOME}/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/stats_${NIGHT}.log 2>&1

    echo "Call to Fink-Fat"
    fink_fat associations candidates --config $FINK_HOME/conf/fink_fat.conf --night ${YEAR}-${MONTH}-${DAY} --verbose > ${FINK_HOME}/broker_logs/fink_fat_association_${NIGHT}.log 2>&1
    fink_fat solve_orbit candidates local --config $FINK_HOME/conf/fink_fat.conf --verbose > ${FINK_HOME}/broker_logs/fink_fat_solve_orbit_${NIGHT}.log 2>&1

    echo "Push SSO candidates to HBase"
    fink start index_sso_cand_archival -c $FINK_HOME/conf_cluster/fink.conf.ztf_nomonitoring_hbase --night ${NIGHT} > ${FINK_HOME}/broker_logs/index_sso_cand_archival_${NIGHT}.log 2>&1

fi

