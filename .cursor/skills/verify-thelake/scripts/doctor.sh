#!/usr/bin/env bash
set -euo pipefail
RUN_DIR="${VERIFY_THELAKE_RUN_DIR:-/tmp/verify-thelake-run}"
PORT="${VERIFY_THELAKE_PORT:-18090}"
PID_FILE="$RUN_DIR/thelake.pid"
if [[ ! -f "$PID_FILE" ]]; then
  echo "no pid file"
  exit 1
fi
PID="$(cat "$PID_FILE")"
if ! kill -0 "$PID" 2>/dev/null; then
  echo "pid $PID is not running"
  exit 1
fi
BODY="$(curl -sf "http://127.0.0.1:${PORT}/health")"
python3 -c 'import json,sys; body=json.loads(sys.argv[1]); assert body.get("status")=="ok", body' "$BODY"
echo "doctor ok pid=$PID port=$PORT"
echo "$BODY"
