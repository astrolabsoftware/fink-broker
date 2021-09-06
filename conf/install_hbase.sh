#!/bin/bash
# Copyright 2019-2020 AstroLab Software
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
set -e

HBASE_VERSION=2.2.7

wget https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz
tar -zxvf hbase-${HBASE_VERSION}-bin.tar.gz
rm hbase-${HBASE_VERSION}-bin.tar.gz

cp conf/hbase-site.xml hbase-${HBASE_VERSION}/conf/

hbase-${HBASE_VERSION}/bin/start-hbase.sh

# create a fake table for tests
# source conf/fink.conf.hbase
# echo -e "create '$SCIENCE_DB_NAME', 'i', 'd', 'b'" | hbase-${HBASE_VERSION}/bin/hbase shell -n
