#!/bin/bash

# Unit test of chart/files/workflow-guard.sh, the first step of the
# WorkflowTemplate running a night, for both modes: catchup and nightly. The
# current time is pinned with GUARD_NOW_EPOCH.

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
guard=$DIR/../chart/files/workflow-guard.sh

stderr=$(mktemp)
trap 'rm -f "$stderr"' EXIT

failures=0

# expect <label> <now, UTC> <mode> <night> <expected rc> <expected stdout>
#        [<pattern expected in stderr>]
# Catch-ups: nights closed at 10:00 UTC the next day; an open night runs until
# it closes, a closed one for 18 hours.
# Nightly runs: stop at 20:00 UTC.
expect() {
    local label="$1" now="$2" mode="$3" night="$4" rc_expected="$5" out_expected="$6"
    local err_expected="${7:-}" out rc
    out=$(GUARD_NOW_EPOCH=$(date -u -d "$now" +%s) \
        bash "$guard" "$mode" "$night" 18 10 20:00 2> "$stderr") && rc=0 || rc=$?
    if [ "$rc" == "$rc_expected" ] && [ "$out" == "$out_expected" ] &&
        { [ -z "$err_expected" ] || grep -q -- "$err_expected" "$stderr"; }; then
        echo "ok   $label"
    else
        echo "FAIL $label"
        echo "     expected: rc=$rc_expected [$out_expected] $err_expected"
        echo "     actual:   rc=$rc [$out] $(cat "$stderr")"
        failures=$((failures + 1))
    fi
}

expect "closed night: deadline is now + 18 h" \
    2026-10-09T12:00:00Z catchup 20261008 0 "2026-10-10T06:00:00Z"
expect "night closed from 10:00 UTC the next day" \
    2026-10-09T10:00:00Z catchup 20261008 0 "2026-10-10T04:00:00Z"
expect "yesterday's night, still open: caught up until it closes" \
    2026-10-09T09:00:00Z catchup 20261008 0 "2026-10-09T10:00:00Z"
expect "yesterday's night, a minute before it closes" \
    2026-10-09T09:59:00Z catchup 20261008 0 "2026-10-09T10:00:00Z"
expect "today's night: caught up live until it closes" \
    2026-10-09T12:00:00Z catchup 20261009 0 "2026-10-10T10:00:00Z"
expect "today's night, even past the nightly deadline" \
    2026-10-09T23:59:00Z catchup 20261009 0 "2026-10-10T10:00:00Z"
expect "today's night, just after the nightly run starts" \
    2026-10-09T00:01:00Z catchup 20261009 0 "2026-10-10T10:00:00Z"
expect "future night" 2026-10-09T12:00:00Z catchup 20261010 1 "" \
    "Night 20261010 has not started yet"
expect "malformed night" 2026-10-09T12:00:00Z catchup 2026-10-07 1 ""
expect "empty night" 2026-10-09T12:00:00Z catchup "" 1 ""


expect "nightly: today's night stops at 20:00 UTC" \
    2026-10-09T00:01:00Z nightly 20261009 0 "2026-10-09T20:00:00Z"
expect "nightly: a restart keeps the same deadline" \
    2026-10-09T19:59:00Z nightly 20261009 0 "2026-10-09T20:00:00Z"
expect "nightly: starting at the deadline" 2026-10-09T20:00:00Z nightly 20261009 1 "" \
    "past its deadline 2026-10-09T20:00:00Z"
expect "nightly: yesterday's night" 2026-10-09T00:01:00Z nightly 20261008 1 "" \
    "only runs today's night, 20261009"
expect "nightly: future night" 2026-10-09T00:01:00Z nightly 20261010 1 "" \
    "only runs today's night, 20261009"
expect "nightly: malformed night" 2026-10-09T00:01:00Z nightly 2026-10-09 1 "" \
    "Malformed night"
expect "unknown mode" 2026-10-09T12:00:00Z oneoff 20261008 1 "" \
    "Unknown mode 'oneoff'"

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed"
    exit 1
fi
echo "All checks passed"
