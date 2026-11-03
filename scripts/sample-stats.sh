#!/usr/bin/env bash
# Append one JSON line per container from `docker stats --no-stream` every N seconds.
# Usage: sample-stats.sh OUT_FILE [SECONDS]. Stop it with kill (TERM): the trap also stops the
# sleep or docker stats in flight, so nothing outlives the sampler. report reads the peak memory.
set -uo pipefail
out="${1:?usage: sample-stats.sh OUT_FILE [SECONDS]}"
every="${2:-5}"
child=""
trap '[ -n "$child" ] && kill "$child" 2>/dev/null; exit 0' TERM INT
while :; do
  docker stats --no-stream --format '{{json .}}' >> "$out" &
  child=$!
  wait "$child" || true
  sleep "$every" &
  child=$!
  wait "$child" || true
done
