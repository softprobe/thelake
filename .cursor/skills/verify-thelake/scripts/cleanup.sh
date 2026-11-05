#!/usr/bin/env bash
# Stop only the process recorded by launch.sh. Evidence files stay.
set -euo pipefail
RUN_DIR="${VERIFY_THELAKE_RUN_DIR:-/tmp/verify-thelake-run}"
PID_FILE="$RUN_DIR/thelake.pid"
if [[ ! -f "$PID_FILE" ]]; then
  exit 0
fi
PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  for _ in $(seq 1 20); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" || true
  fi
fi
rm -f "$PID_FILE"
echo "stopped $PID"
