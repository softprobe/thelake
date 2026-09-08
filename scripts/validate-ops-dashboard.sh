#!/usr/bin/env bash
# Ensure Softprobe (detached) + seed path, then validate every thelake-ops query.
# Usage (from thelake/): ./scripts/validate-ops-dashboard.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
CONFIG="${THELAKE_CONFIG:-$STATE_DIR/config.yaml}"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
RUNTIME_BIN="${SOFTPROBE_BIN:-$ROOT/dist/softprobe-runtime}"
SOFTPROBE_URL_HOST="${SOFTPROBE_URL:-http://127.0.0.1:8090}"
OPS_KEY="${SOFTPROBE_OPS_API_KEY:-local-ops-key}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
DEMO_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"

mkdir -p "$STATE_DIR"

if [[ ! -x "$RUNTIME_BIN" ]]; then
  echo "ERROR: missing $RUNTIME_BIN — build with: make build-release (or cargo build --release)" >&2
  exit 1
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: missing $CONFIG — run: make grafana-up  (once) to provision the manual stack" >&2
  exit 1
fi

# Compaction panels need TWCS waves; keep waves capped for stability.
python3 - "$CONFIG" <<'PY'
from pathlib import Path
import re
import sys
p = Path(sys.argv[1])
text = p.read_text()
text2 = re.sub(
    r"(maintenance:\n  enabled: )false",
    r"\1true",
    text,
    count=1,
)
# Prefer short interval so validator can observe waves.
text2 = re.sub(
    r"(interval_seconds: )\d+",
    r"\g<1>60",
    text2,
    count=1,
)
if text2 != text:
    p.write_text(text2)
    print(f"updated {p}: maintenance.enabled=true interval_seconds=60")
else:
    print(f"config OK: {p}")
PY

our_softprobe_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(tr -d '[:space:]' <"$PID_FILE" || true)"
  [[ -n "${pid:-}" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$cmd" == *softprobe-runtime* ]] || return 1
  return 0
}

start_softprobe_detached() {
  export CONFIG_FILE="$CONFIG"
  export SOFTPROBE_GRPC_DISABLE="${SOFTPROBE_GRPC_DISABLE:-1}"
  export RUST_LOG="${RUST_LOG:-info}"
  # Resolve libduckdb like grafana-manual-up.sh
  local target_dir duck_lib
  target_dir="${CARGO_TARGET_DIR:-$ROOT/target}"
  duck_lib="$(find "${target_dir}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
  if [[ -z "${duck_lib}" && -f "$ROOT/dist/libduckdb.dylib" ]]; then
    duck_lib="$ROOT/dist"
  fi
  if [[ -n "${duck_lib}" ]]; then
    case "$(uname -s)" in
      Darwin) export DYLD_LIBRARY_PATH="${duck_lib}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" ;;
      *) export LD_LIBRARY_PATH="${duck_lib}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
    esac
  fi
  : >>"$LOG"
  echo "===== validate-ops-dashboard $(date -u +%FT%TZ) =====" >>"$LOG"
  if command -v setsid >/dev/null 2>&1; then
    setsid "$RUNTIME_BIN" >>"$LOG" 2>&1 &
    echo $! >"$PID_FILE"
  else
    perl -e '
      use strict; use warnings;
      my ($bin, $log, $pidfile) = @ARGV;
      exit 0 if fork;
      require POSIX; POSIX::setsid();
      exit 0 if fork;
      if (open my $fh, ">", $pidfile) { print {$fh} "$$\n"; close $fh; }
      open STDOUT, ">>", $log or die $!;
      open STDERR, ">&STDOUT";
      open STDIN, "<", "/dev/null";
      exec $bin or die $!;
    ' "$RUNTIME_BIN" "$LOG" "$PID_FILE"
  fi
  local ok=0 pid
  for _ in $(seq 1 40); do
    if [[ -f "$PID_FILE" ]]; then
      pid="$(tr -d '[:space:]' <"$PID_FILE" || true)"
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        ok=1
        break
      fi
    fi
    sleep 0.25
  done
  [[ "$ok" == 1 ]] || { echo "ERROR: Softprobe detach failed"; tail -40 "$LOG"; exit 1; }
}

if our_softprobe_running; then
  echo "==> stopping existing Softprobe so config changes take effect"
  kill "$(tr -d '[:space:]' <"$PID_FILE")" 2>/dev/null || true
  sleep 2
fi
# Stop any leftover on :8090
if curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1; then
  echo "==> :8090 still responds — attempting pkill softprobe-runtime"
  pkill -f softprobe-runtime 2>/dev/null || true
  sleep 2
fi
if curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1; then
  echo "ERROR: Softprobe still listening on :8090; refuse to validate against stale process" >&2
  exit 1
fi
echo "==> starting Softprobe (detached) with $CONFIG"
start_softprobe_detached

echo "==> waiting for /ready"
ok=0
for _ in $(seq 1 90); do
  if curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1; then
    ok=1
    break
  fi
  if ! our_softprobe_running; then
    echo "ERROR: Softprobe exited during startup" >&2
    tail -50 "$LOG" >&2 || true
    exit 1
  fi
  sleep 1
done
[[ "$ok" == 1 ]] || { echo "ERROR: Softprobe not ready"; tail -50 "$LOG"; exit 1; }

# Pause OTel demo during validation so Softprobe is not flooded while we seed
# and query ops PromQL. Unpause afterward only if we paused.
DEMO_PAUSED_BY_US=0
if command -v docker >/dev/null 2>&1; then
  if docker ps --format '{{.Names}} {{.Status}}' 2>/dev/null | grep -q 'otel-collector.*Up'; then
    if ! docker ps --format '{{.Names}} {{.Status}}' 2>/dev/null | grep -q 'otel-collector.*Paused'; then
      echo "==> pausing otel-collector + load-generator for stable validation"
      docker pause otel-collector load-generator 2>/dev/null || true
      DEMO_PAUSED_BY_US=1
    fi
  fi
fi

echo "==> waiting briefly for maintenance / first export"
sleep 25

export SOFTPROBE_URL="$SOFTPROBE_URL_HOST"
export SOFTPROBE_OPS_API_KEY="$OPS_KEY"
export SOFTPROBE_API_KEY="$API_KEY"
chmod +x "$ROOT/scripts/validate-ops-dashboard-queries.py"
set +e
python3 "$ROOT/scripts/validate-ops-dashboard-queries.py" "$@"
rc=$?
set -e

if [[ "$DEMO_PAUSED_BY_US" == "1" ]]; then
  echo "==> unpausing otel-collector + load-generator"
  docker unpause otel-collector load-generator 2>/dev/null || true
fi
exit "$rc"
