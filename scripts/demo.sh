#!/usr/bin/env bash
# make demo: PLAN 11 steps 1-11 with the scripted edit that forces conflicts in cycle 2.
# SF=0.01 bash scripts/demo.sh also works. Timings go to runs/<utc>-sf<SF>/steps.json.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh
init_run

# Cycle 2 is triggered without waiting; the edits land once its transform stage has started,
# which is after the extract snapshot, so apply finds version conflicts.
cycle_with_edits() {
  local out id
  out=$($RP cycle trigger --mode incremental)
  echo "$out"
  id=$(printf '%s\n' "$out" | sed -n 's/^cycle \([0-9][0-9]*\) .*/\1/p' | head -n 1)
  [ -n "$id" ] || { echo "no cycle id in the trigger output" >&2; return 1; }
  $RP cycle wait-stage --cycle "$id" --stage transform
  $RP workload edit --count 50 --kvi --seed 7
  $RP cycle wait --cycle "$id"
}
check() { $RP check --cycles 1-3 --expect-conflicts-in 2 2>&1 | tee "$RUN_DIR/check.txt"; }

prepare
step "cycle 1 full" cycle_full
step "workload start" workload_start
step "cycle 2 incremental with edits" cycle_with_edits
step "cycle 3 incremental" cycle_incremental
step "workload stop" workload_stop
step "check" check
step "report" report
echo "done: $RUN_DIR/timings.md"
