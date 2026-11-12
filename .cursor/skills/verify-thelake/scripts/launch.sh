#!/usr/bin/env bash
# Start one thelake process for verification. Refuses a port this run did not start.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
RUN_DIR="${VERIFY_THELAKE_RUN_DIR:-/tmp/verify-thelake-run}"
EVIDENCE="${VERIFY_THELAKE_EVIDENCE:-/tmp/verify-thelake-evidence}"
PORT="${VERIFY_THELAKE_PORT:-18090}"
mkdir -p "$RUN_DIR" "$EVIDENCE"
PID_FILE="$RUN_DIR/thelake.pid"
LOG_FILE="$EVIDENCE/thelake.log"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running pid $(cat "$PID_FILE")"
  exit 0
fi
if (command -v ss >/dev/null 2>&1 && ss -ltn | awk '{print $4}' | grep -q ":${PORT}$") || \
   (command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1); then
  echo "port ${PORT} is in use by a process this script did not start" >&2
  exit 1
fi
cd "$ROOT"
export THELAKE_CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
export CARGO_HOME="${CARGO_HOME:-$THELAKE_CACHE_ROOT/cargo}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$THELAKE_CACHE_ROOT/target}"
export PATH="$CARGO_HOME/bin:$HOME/.cargo/bin:$PATH"
export DUCKDB_DOWNLOAD_LIB="${DUCKDB_DOWNLOAD_LIB:-1}"
DUCKDB_LIB_DIR="$(find "$CARGO_TARGET_DIR/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -n "$DUCKDB_LIB_DIR" ]]; then
  case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
    *) export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
  esac
fi
export CONFIG_FILE="${CONFIG_FILE:-$ROOT/config.yaml}"
export SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-dev-admin}"
export SOFTPROBE_ASSERTION_HMAC_SECRET="${SOFTPROBE_ASSERTION_HMAC_SECRET:-dev-assertion}"
export SOFTPROBE_LISTEN_ADDR="127.0.0.1:${PORT}"
export SOFTPROBE_GRPC_DISABLE=1
nohup cargo run --locked --bin thelake >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
for _ in $(seq 1 180); do
  if curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null; then
    echo "ready http://127.0.0.1:${PORT}"
    exit 0
  fi
  if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "process exited before ready" >&2
    tail -n 40 "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 2
done
echo "timed out waiting for /health" >&2
exit 1
