#!/bin/bash
# Copyright 2026 AstroLab Software
# Author: Fabrice Jammes
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

# For each ZTF night: alerts in the topic, alerts that arrived after a cut-off
# hour (UTC), and the basic:raw count of the Fink statistics API.
#
# ZTF keeps writing to a night's topic after the end of the observations, so a
# run stopping at a fixed hour misses the late alerts. kafka-get-offsets.sh
# --time gives, per partition, the first offset whose timestamp is at or after
# the cut: nothing is consumed.
#
# Run it from a host allowed to read the ZTF Kafka (the one running the
# broker): elsewhere the TCP port answers but the client hangs. The topics are
# kept about two weeks.
#
# Needs: kafka-get-offsets.sh (Kafka >= 3.0, bin/ of the Kafka distribution),
# GNU date, curl and python3.
#
# Usage: ./ztf_late_alerts.sh [CUT_HH:MM] NIGHT...
#   KAFKA_BIN=/opt/kafka/bin ./ztf_late_alerts.sh 18:00 20261003 20261004
set -euo pipefail

KAFKA_BIN=${KAFKA_BIN:-bin}
SERVER=${SERVER:-public.alerts.ztf.uw.edu:9092}
API=https://api.ztf.fink-portal.org/api/v1/statistics

cut="18:00"
if [[ "${1:-}" =~ ^[0-9]{2}:[0-9]{2}$ ]]; then cut="$1"; shift; fi

offsets() {  # offsets() TOPIC TIME -> "partition offset" lines
    "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$SERVER" \
        --topic "$1" --time "$2" 2>/dev/null | awk -F: '{print $2, $3}'
}

printf "%-9s %11s %22s %15s\n" night topic "after ${cut} UTC" "basic:raw (API)"
for night in "$@"; do
    topic="ztf_${night}_programid1"
    cut_ms=$(( $(date -u -d "${night:0:4}-${night:4:2}-${night:6:2} $cut" +%s) * 1000 ))
    # End offsets, and offsets of the first message at or after the cut.
    # A partition with no message after the cut counts with its end offset.
    read -r total before < <(
        awk 'NR==FNR {end[$1]=$2; next} $2!="" {at[$1]=$2}
             END {for (p in end) {t+=end[p]; b+=(p in at) ? at[p] : end[p]}
                  print t+0, b+0}' \
            <(offsets "$topic" -1) <(offsets "$topic" "$cut_ms"))
    raw=$(curl -s -m 30 -X POST "$API" -H 'Content-Type: application/json' \
        -d "{\"date\":\"$night\",\"columns\":\"basic:raw\"}" |
        python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["basic:raw"] if d else "-")')
    printf "%-9s %11d %22d %15s\n" "$night" "$total" "$(( total - before ))" "$raw"
done
