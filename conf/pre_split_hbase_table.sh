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
SINFO="\xF0\x9F\x9B\x88"
SSTOP="\xF0\x9F\x9B\x91"
SDONE="\xE2\x9C\x85"

TABLE_PREFIX="rubin"

pre_split_nth_digits() {
        # Define start and stop boundaries
        START=$1
        STOP=$2
        INCREMENT=$3
        NDIGITS=$4

        # Initialize an empty array for SPLITS
        SPLITS=()

        # Loop through the range and populate the SPLITS array with two-digit numbers
        for ((i=$START; i<=$STOP; $INCREMENT)); do
            # Format the number to two digits
            SPLIT=$(printf "${NDIGITS}" $i)
            SPLITS+=($SPLIT)
        done

        # Convert the array to a comma-separated string
        SPLITS_STRING=$(IFS=,; echo "${SPLITS[*]}")

        # return split and number of partitions
        echo "$SPLITS_STRING" "${#SPLITS[@]}"
}

pre_split_alphabetical() {
        # Array for letters
	letters=($(echo {a..z} | tr ' ' '\n' | sed 's/^/"/;s/$/"/'))

        # Convert the array to a comma-separated string
        SPLITS_STRING=$(IFS=,; echo "${letters[*]}")

        # return split and number of partitions
        echo "$SPLITS_STRING" "${#letters[@]}"
}


STANDARD_TABLES=(
	"${TABLE_PREFIX}.diaObject"
	"${TABLE_PREFIX}.mpc_orbits"
	"${TABLE_PREFIX}.diaSource_static"
	"${TABLE_PREFIX}.diaSource_sso"
	"${TABLE_PREFIX}.cutouts"
	"${TABLE_PREFIX}.pixel128"
	"${TABLE_PREFIX}.tns_resolver"
        "${TABLE_PREFIX}.sso_resolver"
	"${TABLE_PREFIX}.statistics"
)

COLFAMILIES=(
        "{NAME => 'r', COMPRESSION => 'LZ4'}, {NAME => 'f', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}, {NAME => 'f', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}, {NAME => 'f', COMPRESSION => 'LZ4'}"
	"{NAME => 'f', COMPRESSION => 'LZ4'}"
        "{NAME => 'r', COMPRESSION => 'LZ4'}"
)


for ((index=0; index<${#STANDARD_TABLES[@]}; index++)); do
	TABLE_NAME=${STANDARD_TABLES[index]}
	COLFAMILY=${COLFAMILIES[index]}
        echo -e "$SINFO Processing table $TABLE_NAME"
	echo -e "$SINFO Options: ${COLFAMILY}"
        if echo -e "list" | /opt/hbase/bin/hbase shell -n | grep ${TABLE_NAME}; then
                echo -e "$SSTOP Table $TABLE_NAME already exists -- not creating a new one."
        else
                echo -e "$SDONE $TABLE_NAME does not exists -- creating a new one"

                if [[ $TABLE_NAME == "${TABLE_PREFIX}.pixel128" ]]; then
                        # pixel128 has a different splitting
                        output=$(pre_split_nth_digits 1000 199999 "i+=1000" \'%03d\')
		elif [[ $TABLE_NAME == "${TABLE_PREFIX}.diaSource_sso" ]]; then
                        # diaSource_sso has 100 partitions, based on year [YY]YY
                        output=$(pre_split_nth_digits 1 99 "i++" \'%02d\')
		elif [[ $TABLE_NAME == "${TABLE_PREFIX}.mpc_orbits" ]]; then
                        # diaObject has 100 partitions, based on year [YY]YY
                        output=$(pre_split_nth_digits 1 99 "i++" \'%02d\')
		elif [[ $TABLE_NAME == "${TABLE_PREFIX}.tns_resolver" ]]; then
                        # diaObject has 100 partitions, based on year [YY]YY
                        output=$(pre_split_alphabetical)
		elif [[ $TABLE_NAME == "${TABLE_PREFIX}.statistics" ]]; then
                        # diaSource_sso has 100 partitions, without salt defined yet
                        output=$(pre_split_nth_digits 1 99 "i++" \'%02d\')
                else
                        # Default splitting
                        output=$(pre_split_nth_digits 1 999 "i++" \'%03d\')
                fi
                read -r SPLIT_POINTS NPARTS <<< "$output"
                echo -e "$SINFO Number of regions: $((NPARTS + 1))"
                COMMAND="create '${TABLE_NAME}', ${COLFAMILY}, SPLITS=> [$SPLIT_POINTS]"

                # Create the table
                echo -e $COMMAND | /opt/hbase/bin/hbase shell -n
        fi
done

