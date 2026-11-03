#!/usr/bin/env bash
# Append one JSON line per container from `docker stats --no-stream` every N seconds.
# Usage: sample-stats.sh OUT_FILE [SECONDS]. Stop it with kill; report reads the peak memory.
set -uo pipefail
out="${1:?usage: sample-stats.sh OUT_FILE [SECONDS]}"
every="${2:-5}"
while :; do
  docker stats --no-stream --format '{{json .}}' >> "$out" || true
  sleep "$every"
done
