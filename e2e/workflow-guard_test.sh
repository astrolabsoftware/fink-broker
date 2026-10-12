#!/bin/bash

# Unit test of chart/files/workflow-guard.sh, the first step of the
# WorkflowTemplate catching a night up. The current time is pinned with
# GUARD_NOW_EPOCH.

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
guard=$DIR/../chart/files/workflow-guard.sh

stderr=$(mktemp)
trap 'rm -f "$stderr"' EXIT

failures=0

# expect <label> <now, UTC> <night> <expected rc> <expected stdout>
# Deadline after 18 hours, nights closed at 10:00 UTC the next day.
expect() {
    local label="$1" now="$2" night="$3" rc_expected="$4" out_expected="$5" out rc
    out=$(GUARD_NOW_EPOCH=$(date -u -d "$now" +%s) bash "$guard" "$night" 18 10 \
        2> "$stderr") && rc=0 || rc=$?
    if [ "$rc" == "$rc_expected" ] && [ "$out" == "$out_expected" ]; then
        echo "ok   $label"
    else
        echo "FAIL $label"
        echo "     expected: rc=$rc_expected [$out_expected]"
        echo "     actual:   rc=$rc [$out] $(cat "$stderr")"
        failures=$((failures + 1))
    fi
}

expect "closed night: deadline is now + 18 h" \
    2026-10-09T12:00:00Z 20261008 0 "2026-10-10T06:00:00Z"
expect "night closed from 10:00 UTC the next day" \
    2026-10-09T10:00:00Z 20261008 0 "2026-10-10T04:00:00Z"
expect "night still open the next morning" 2026-10-09T09:59:00Z 20261008 1 ""
expect "today's night is open" 2026-10-09T12:00:00Z 20261009 1 ""
expect "future night" 2026-10-09T12:00:00Z 20261010 1 ""
expect "malformed night" 2026-10-09T12:00:00Z 2026-10-07 1 ""
expect "empty night" 2026-10-09T12:00:00Z "" 1 ""

expect "refusal says when the night closes" 2026-10-09T12:00:00Z 20261009 1 ""
if ! grep -q "2026-10-10T10:00:00Z" "$stderr"; then
    echo "FAIL refusal message: $(cat "$stderr")"
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed"
    exit 1
fi
echo "All checks passed"
