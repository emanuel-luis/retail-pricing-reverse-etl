#!/usr/bin/env bash
# make heavy SF=10: the demo steps with the workload running through every cycle, docker stats
# sampled every 5 s into stats.jsonl, and no injected edits.
set -euo pipefail
cd "$(dirname "$0")/.."
SF="${SF:-10}"
. scripts/lib.sh
init_run

check() { $RP check --cycles 1-3 2>&1 | tee "$RUN_DIR/check.txt"; }

prepare
step "workload start" workload_start
bash scripts/sample-stats.sh "$RUN_DIR/stats.jsonl" 5 &
SAMPLER=$!
trap 'kill "$SAMPLER" 2>/dev/null || true' EXIT
step "cycle 1 full" cycle_full
step "cycle 2 incremental" cycle_incremental
step "cycle 3 incremental" cycle_incremental
kill "$SAMPLER" 2>/dev/null || true
step "workload stop" workload_stop
step "check" check
step "report" report
echo "done: $RUN_DIR/timings.md"
