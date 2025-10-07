#!/bin/bash
# Copyright 2019-2025 AstroLab Software
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
TABLE_PREFIX="rubin"

STANDARD_TABLES=(
        "${TABLE_PREFIX}.diaObject"
        "${TABLE_PREFIX}.ssObject"
        "${TABLE_PREFIX}.diaSource_static"
        "${TABLE_PREFIX}.diaSource_sso"
        "${TABLE_PREFIX}.cutouts"
        "${TABLE_PREFIX}.pixel128"
        "${TABLE_PREFIX}.tns_resolver"
	"${TABLE_PREFIX}.sso_resolver"
)

for TABLE_NAME in $STANDARD_TABLES; do
        echo "Processing table $TABLE_NAME"
	COMMAND="disable '${TABLE_NAME}'"
	echo -e $COMMAND | /opt/hbase/bin/hbase shell -n

	COMMAND="drop '${TABLE_NAME}'"
	echo -e $COMMAND | /opt/hbase/bin/hbase shell -n
done

