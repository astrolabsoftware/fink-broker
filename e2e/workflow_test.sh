#!/bin/bash

# Unit test of e2e/workflow.sh --catchup-done: it must take the workflow of the
# CI night as the only Workflow of the namespace, and fail with the list of
# Workflows when there is not exactly one. kubectl is replaced by a stub
# printing $STUB_WORKFLOWS, as "<name>\t<night>\t<phase>" lines.

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

stubdir=$(mktemp -d)
stderr=$(mktemp)
trap 'rm -rf "$stubdir" "$stderr"' EXIT

cat > "$stubdir/kubectl" << 'EOF'
#!/bin/bash
if [ "$1 $2" = "get workflows" ]; then
  printf '%s' "$STUB_WORKFLOWS"
  exit 0
fi
echo "kubectl stub: unexpected call: $*" 1>&2
exit 1
EOF
chmod +x "$stubdir/kubectl"

failures=0

# expect_failure <label> <workflows> <pattern expected in stderr>...
expect_failure() {
  local label="$1" workflows="$2" rc pattern ok=true
  shift 2
  STUB_WORKFLOWS="$workflows" PATH="$stubdir:$PATH" \
    "$DIR/workflow.sh" --catchup-done > /dev/null 2> "$stderr" && rc=0 || rc=$?
  if [ "$rc" -eq 0 ]; then
    ok=false
  fi
  for pattern in "$@"; do
    grep -qF -- "$pattern" "$stderr" || ok=false
  done
  if $ok; then
    echo "ok   $label"
  else
    echo "FAIL $label (rc=$rc)"
    sed 's/^/     /' "$stderr"
    failures=$((failures + 1))
  fi
}

expect_failure "no workflow" "" \
  "expected exactly one Workflow" "before --catchup-open and --nightly"

expect_failure "two workflows" \
  "$(printf 'catchup-20200101-a\t20200101\tSucceeded\nnightly-x\t20261123\tRunning\n')" \
  "expected exactly one Workflow" \
  "catchup-20200101-a" "20200101" "Succeeded" \
  "nightly-x" "20261123" "Running"

# One workflow: it is taken as the CI night, the next kubectl call (not
# stubbed, so it fails) reads its night argument
expect_failure "one workflow is the CI night" \
  "$(printf 'catchup-20200101-a\t20200101\tRunning\n')" \
  "unexpected call: get workflow -n fink-broker catchup-20200101-a"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "All tests passed"
