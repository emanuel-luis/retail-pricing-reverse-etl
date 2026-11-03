# Shared by demo.sh and heavy.sh (sourced, never run). Runs from the repo root.
#
# Every step is one function holding one-liners, so a step can be replayed by hand in Git Bash
# (no make there): copy its command lines, or source this file and call the function.
# `step NAME FUNCTION` times a step and appends it to $RUN_DIR/steps.json.

export MSYS_NO_PATHCONV=1  # Git Bash would rewrite container paths such as /var/lib/...
SF="${SF:-1}"
RP="uv run retail-pricing"
# --profile on the command line replaces COMPOSE_PROFILES, so the core profile is named again.
ALL_PROFILES="--profile core --profile workload"
STEPS=""

init_run() {
  RUN_DIR="runs/$(date -u +%Y%m%dT%H%M%SZ)-sf${SF}"
  mkdir -p "$RUN_DIR"
  echo "run folder: $RUN_DIR"
}

now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

# step "name" function: run the function in a subshell with errexit (a plain `if` would switch
# errexit off inside it), record the time and ok flag, and stop the script on failure.
step() {
  local name="$1" t0 rc
  shift
  echo "==> $name"
  t0=$(now_ms)
  set +e
  ( set -e; "$@" )
  rc=$?
  set -e
  STEPS="${STEPS:+$STEPS,}{\"step\":\"$name\",\"ms\":$(( $(now_ms) - t0 )),\"ok\":$([ "$rc" -eq 0 ] && echo true || echo false)}"
  printf '[%s]\n' "$STEPS" > "$RUN_DIR/steps.json"
  [ "$rc" -eq 0 ] || { echo "step '$name' failed (exit $rc)" >&2; exit "$rc"; }
}

env_file() { [ -f .env ] || { cp .env.example .env; echo "created .env from .env.example (local-only values)"; }; }
data_all() { $RP data all --sf "$SF"; }
stack_up() { docker compose up -d --build --wait; }
oltp_load() { $RP oltp load --sf "$SF"; }
control_init() { $RP control init; }
ch_init() { $RP ch init; }
s3_init() { $RP s3 init; }
nifi_deploy() { $RP nifi deploy --env "${DEPLOY_ENV:-local}"; }
cycle_full() { $RP cycle trigger --mode full --wait; }
cycle_incremental() { $RP cycle trigger --mode incremental --wait; }
workload_start() {
  docker compose $ALL_PROFILES build dbt-runner
  docker compose $ALL_PROFILES up -d --wait workload
}
workload_stop() { docker compose $ALL_PROFILES stop workload; }
report() { $RP report --run-dir "$RUN_DIR"; }

# Steps 1-8 of PLAN 11: everything up to the first cycle.
prepare() {
  step "env file" env_file
  step "data all" data_all
  step "stack up" stack_up
  step "oltp load" oltp_load
  step "control init" control_init
  step "ch init" ch_init
  step "s3 init" s3_init
  step "nifi deploy" nifi_deploy
}
