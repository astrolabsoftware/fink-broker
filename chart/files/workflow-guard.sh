#!/bin/bash

# First step of the WorkflowTemplate running a night: refuse a night that
# cannot be run in this mode, else print the absolute deadline of the run
# (ISO 8601, UTC) on stdout, where the next steps read it.
#
# Usage: workflow-guard.sh MODE NIGHT DEADLINE_HOURS CLOSE_HOUR EXIT_AT
#
# MODE:
#   catchup  a night submitted by hand (e2e/catchup.sh). Only a closed night is
#            accepted, i.e. from CLOSE_HOUR UTC on the next day: its late
#            alerts are in, and the nightly run, which processes today's night,
#            can no longer write the same checkpoints. Deadline: now +
#            DEADLINE_HOURS.
#   nightly  the run submitted by the CronWorkflow. Only today's night (UTC) is
#            accepted, processed live as its alerts arrive. Deadline: EXIT_AT
#            (HH:MM, UTC) on the day of the night.
#
# The deadline is computed once, here, so a restarted job aims at the same
# instant instead of granting itself a fresh window.
#
# GUARD_NOW_EPOCH: current time, for tests.

set -euo pipefail

mode="${1:-}"
night="${2:-}"
deadline_hours="$3"
close_hour="$4"
exit_at="$5"
now="${GUARD_NOW_EPOCH:-$(date +%s)}"

iso() {
    date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ
}

if ! [[ "$night" =~ ^[0-9]{8}$ ]]; then
    echo "Malformed night '$night', expected YYYYMMDD" >&2
    exit 1
fi
midnight=$(TZ=UTC date -d "${night:0:4}-${night:4:2}-${night:6:2} 00:00:00" +%s)

case "$mode" in
    catchup)
        # Closing time: CLOSE_HOUR UTC on the day after the night
        closes_at=$((midnight + 86400 + close_hour * 3600))
        if [ "$now" -lt "$closes_at" ]; then
            echo "Night $night is still open until $(iso "$closes_at"):" \
                "catch it up once closed" >&2
            exit 1
        fi
        iso $((now + deadline_hours * 3600))
        ;;
    nightly)
        today=$(date -u -d "@$now" +%Y%m%d)
        if [ "$night" != "$today" ]; then
            echo "A nightly run only runs today's night, $today, not $night:" \
                "catch it up instead" >&2
            exit 1
        fi
        deadline=$(TZ=UTC date -d "${night:0:4}-${night:4:2}-${night:6:2} $exit_at" +%s)
        if [ "$now" -ge "$deadline" ]; then
            echo "Nightly run of $night started past its deadline $(iso "$deadline")" >&2
            exit 1
        fi
        iso "$deadline"
        ;;
    *)
        echo "Unknown mode '$mode', expected catchup or nightly" >&2
        exit 1
        ;;
esac
