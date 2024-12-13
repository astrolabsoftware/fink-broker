#!/bin/bash
# Copyright 2019-2024 AstroLab Software
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
source /home/julien.peloton/.bash_profile

message_help="./check_results <--night NIGHT> <--telegram>"

# Grab the command line arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h)
        echo -e "$message_help"
        exit
        ;;
    --night)
        NIGHT="$2"
        shift 2
        ;;
    --telegram)
        TELEGRAM=true
        shift 1
        ;;
  esac
done

broadcastMessage () {
    if [[ $TELEGRAM == true ]]; then
        curl -X POST -d 'chat_id=@fink_bot_error' -d "text=$1" https://api.telegram.org/bot"$FINK_TG_TOKEN"/sendMessage
    else
        echo "$1"
    fi
}

if [[ -z "$NIGHT" ]]; then
    NIGHT=$(date +"%Y%m%d")
fi

FILES=$(/usr/bin/ls "$FINK_HOME"/broker_logs/*"${NIGHT}"*)

if [[ -z "$FILES" ]]; then
    broadcastMessage "Nothing found for $NIGHT"
    exit
fi

newline=$'\n'
MESSAGE=""
for file in $FILES; do
    isPythonProblem=$(grep -n Traceback "$file")
    if [[ $isPythonProblem != "" ]]; then
        MESSAGE+="SPARK FAILURE: $(basename $file) $newline"
    fi

    isHdfsProblem=$(grep -n "Name node is in safe mode" "$file")
    if [[ $isHdfsProblem != "" ]]; then
        MESSAGE+="HDFS FAILURE: $(basename $file) $newline"
    fi

    isHdfsProblem=$(grep -n "Dead nodes" "$file")
    if [[ $isHdfsProblem != "" ]]; then
        MESSAGE+="HDFS FAILURE: $(basename $file) $newline"
    fi

    isHbaseProblem=$(grep -n "RetriesExhaustedWithDetailsException" "$file")
    if [[ $isHbaseProblem != "" ]]; then
        MESSAGE+="HBase FAILURE: $(basename $file) $newline"
        hasProcessed=$(grep -n "alerts pushed to HBase" "$file")
        if [[ $hasProcessed != "" ]]; then
            MESSAGE+="|--> PUSHED OK $newline"
        fi
        hasProcessed=$(grep -n "Output Path is null in commitJob" "$file")
        if [[ $hasProcessed != "" ]]; then
            MESSAGE+="|--> PUSHED OK $newline"
        fi
    fi
done

if [[ -z "$MESSAGE" ]]; then
    broadcastMessage "All OK for $NIGHT"
else
    broadcastMessage "$MESSAGE"
fi
