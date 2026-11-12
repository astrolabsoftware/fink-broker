#!/bin/bash

# First step of the WorkflowTemplate running a night: refuse a night that
# cannot be run in this mode, else decide once how its jobs run:
# - state: open (live, until the deadline, no done marker) or closed (stop as
#   soon as caught up, leave a done marker)
# - deadline: absolute end of the run (ISO 8601, UTC)
# Both are written to $GUARD_OUTPUT_DIR/{state,deadline}, read by Argo as the
# output parameters of the step, and printed on stdout for the logs.
#
# Usage: workflow-guard.sh MODE NIGHT DEADLINE_HOURS CLOSE_HOUR EXIT_AT
#
# MODE:
#   catchup  a night submitted by hand (e2e/catchup.sh). A night is open until
#            CLOSE_HOUR UTC on the next day, once its late alerts are in:
#            - an open night, today's or yesterday's, e.g. to restart a failed
#              nightly run: open. Deadline: when the night closes.
#            - a closed night: closed. Deadline: now + DEADLINE_HOURS, a
#              ceiling.
#            A run of the same night, nightly or catch-up, waits on the
#            per-night mutex of the WorkflowTemplate.
#   nightly  the run submitted by the CronWorkflow. Only today's night (UTC) is
#            accepted, open. Deadline: EXIT_AT (HH:MM, UTC) on the day of the
#            night.
#
# Both are decided once, here: a restarted job aims at the same instant and
# keeps the same behaviour, instead of deciding anew from the time it restarts.
#
# GUARD_NOW_EPOCH: current time, for tests.
# GUARD_OUTPUT_DIR: where the outputs are written. Default: /tmp/guard

set -euo pipefail

mode="${1:-}"
night="${2:-}"
deadline_hours="$3"
close_hour="$4"
exit_at="$5"
now="${GUARD_NOW_EPOCH:-$(date +%s)}"
output_dir="${GUARD_OUTPUT_DIR:-/tmp/guard}"

iso() {
    date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ
}

# accept STATE DEADLINE_EPOCH
accept() {
    mkdir -p "$output_dir"
    echo "$1" > "$output_dir/state"
    iso "$2" > "$output_dir/deadline"
    echo "$1 $(iso "$2")"
}

if ! [[ "$night" =~ ^[0-9]{8}$ ]]; then
    echo "Malformed night '$night', expected YYYYMMDD" >&2
    exit 1
fi
midnight=$(TZ=UTC date -d "${night:0:4}-${night:4:2}-${night:6:2} 00:00:00" +%s)

case "$mode" in
    catchup)
        if [ "$now" -lt "$midnight" ]; then
            echo "Night $night has not started yet" >&2
            exit 1
        fi
        # Closing time: CLOSE_HOUR UTC on the day after the night
        closes_at=$((midnight + 86400 + close_hour * 3600))
        if [ "$now" -lt "$closes_at" ]; then
            accept open "$closes_at"
        else
            accept closed $((now + deadline_hours * 3600))
        fi
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
        accept open "$deadline"
        ;;
    *)
        echo "Unknown mode '$mode', expected catchup or nightly" >&2
        exit 1
        ;;
esac
