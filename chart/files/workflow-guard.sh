#!/bin/bash

# First step of the WorkflowTemplate catching a night up: refuse a night that
# cannot be caught up, else print the absolute deadline of the run (ISO 8601,
# UTC) on stdout, where the next steps read it.
#
# Usage: workflow-guard.sh NIGHT DEADLINE_HOURS CLOSE_HOUR
#
# Only a closed night is accepted, i.e. from CLOSE_HOUR UTC on the next day
# (the jobs' -night_close_hour): its late alerts are in, and the daily run,
# which processes today's night, can no longer write the same checkpoints.
#
# The deadline is computed once, here, so a restarted job aims at the same
# instant instead of granting itself a fresh window.
#
# GUARD_NOW_EPOCH: current time, for tests.

set -euo pipefail

night="${1:-}"
deadline_hours="$2"
close_hour="$3"
now="${GUARD_NOW_EPOCH:-$(date +%s)}"

if ! [[ "$night" =~ ^[0-9]{8}$ ]]; then
    echo "Malformed night '$night', expected YYYYMMDD" >&2
    exit 1
fi

# Closing time: CLOSE_HOUR UTC on the day after the night
midnight=$(TZ=UTC date -d "${night:0:4}-${night:4:2}-${night:6:2} 00:00:00" +%s)
closes_at=$((midnight + 86400 + close_hour * 3600))
if [ "$now" -lt "$closes_at" ]; then
    echo "Night $night is still open until" \
        "$(date -u -d "@$closes_at" +%Y-%m-%dT%H:%M:%SZ): catch it up once closed" >&2
    exit 1
fi

date -u -d "@$((now + deadline_hours * 3600))" +%Y-%m-%dT%H:%M:%SZ
