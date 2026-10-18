#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
EXPLORER_DIR="$ROOT/packages/thelake-explorer"

command -v node >/dev/null 2>&1 || { echo "ERROR: node is required for Explorer UI E2E." >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "ERROR: cargo is required for Explorer UI E2E." >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required for Explorer UI E2E." >&2; exit 1; }
PORT="${THELAKE_EXPLORER_E2E_PORT:-18090}"
BACKEND="http://127.0.0.1:$PORT"
if curl -fsS "$BACKEND/ready" >/dev/null 2>&1; then
  echo "ERROR: Explorer E2E backend port $PORT is already serving a process." >&2
  exit 1
fi

ONLINE_E2E="${THELAKE_EXPLORER_E2E_ONLINE:-0}"
E2E_DB_PORT="5432"
if [[ "$ONLINE_E2E" == "1" ]]; then
  command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is required for online evaluation E2E." >&2; exit 1; }
  if [[ -z "${GOOGLE_API_KEY:-}" && -n "${GEMINI_API_KEY:-}" ]]; then export GOOGLE_API_KEY="$GEMINI_API_KEY"; fi
  [[ -n "${GOOGLE_API_KEY:-}" ]] || { echo "ERROR: Set GOOGLE_API_KEY (or GEMINI_API_KEY) for live Gemini agent and evaluator calls." >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "ERROR: Start Docker before online evaluation E2E." >&2; exit 1; }
  LISA_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  export LISA_THELAKE_TOKEN="$LISA_TOKEN"
  export THELAKE_QUICKSTART_INSTANCE_ID="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
  read -r E2E_DB_PORT < <(python3 - <<'PY'
import socket
for port in range(55440, 55460):
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
            print(port)
            break
        except OSError:
            continue
else:
    raise SystemExit("No free localhost port in 55440-55459.")
PY
)
  export THELAKE_QUICKSTART_DB_PORT="$E2E_DB_PORT"
  if [[ "$(uname -s)" == "Linux" ]]; then
    export THELAKE_QUICKSTART_AGENT_NETWORK=host
  fi
fi

if [[ ! -x "$BROWSER_DIR/node_modules/.bin/playwright" ]]; then
  npm --prefix "$BROWSER_DIR" ci --no-audit
fi
if [[ ! -x "$EXPLORER_DIR/node_modules/.bin/vite" ]]; then
  npm --prefix "$EXPLORER_DIR" ci --no-audit
fi

THELAKE_CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$THELAKE_CACHE_ROOT/target}"
TMP_DIR=""
E2E_STACK_STATE=""
ARTIFACT_DIR=""
if [[ "$ONLINE_E2E" == "1" ]]; then
  stack_json="$(python3 scripts/test_stack.py start-compose \
    --suite explorer-online \
    --compose-file "$ROOT/examples/quickstart/compose.yaml" \
    --service postgres \
    --published-port postgres:5432 \
    --wait)"
  E2E_STACK_STATE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["state_file"])' "$stack_json")"
  ARTIFACT_DIR="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["artifact_dir"])' "$stack_json")"
  TMP_DIR="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["scratch_dir"])' "$stack_json")"
  E2E_DB_PORT="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["ports"]["postgres:5432"][0].rsplit(":",1)[1])' "$stack_json")"
else
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/thelake-explorer-e2e.XXXXXX")"
  ARTIFACT_DIR="$ROOT/target/test-artifacts/explorer-ui/$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p "$ARTIFACT_DIR"
fi
mkdir -p "$TMP_DIR/warehouse"
WORKSPACE_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SCHEMA="explorer_e2e_$(python3 -c 'import os; print(os.getpid())')"
SERVER_PID=""
LISA_PID=""
LISA_PORT="${THELAKE_EXPLORER_E2E_LISA_PORT:-18082}"
LISA_TOKEN=""

cleanup() {
  local status=$?
  if [[ -n "$LISA_PID" ]] && kill -0 "$LISA_PID" 2>/dev/null; then
    kill "$LISA_PID" 2>/dev/null || true
    wait "$LISA_PID" 2>/dev/null || true
  fi
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "$E2E_STACK_STATE" ]]; then
    python3 "$ROOT/scripts/test_stack.py" stop --state-file "$E2E_STACK_STATE" || status=$?
  fi
  if [[ "${THELAKE_E2E_KEEP_ARTIFACTS:-0}" == "1" ]]; then
    echo "Explorer E2E logs and temporary data kept at $TMP_DIR" >&2
  else
    rm -rf "$TMP_DIR"
  fi
  echo "Explorer E2E artifacts retained at $ARTIFACT_DIR" >&2
  exit "$status"
}
trap cleanup EXIT

if [[ "$ONLINE_E2E" == "1" ]]; then
  echo "==> Starting isolated Postgres for Lisa's local Gemini evaluator..."
  echo "==> Calibrating Lisa's local evaluator with paired policy cases..."
  LISA_RUN_EVALUATION_LIVE=1 uv run --project "$ROOT/lisa" --extra test pytest -q \
    "$ROOT/lisa/tests/test_evaluation_live.py" -k "pair_judge_calibration and connected_itinerary"
fi

cat > "$TMP_DIR/config.yaml" <<EOF
server:
  host: 127.0.0.1
  port: $PORT
  max_body_size: 104857600
query:
  max_connections: 1
ingest:
  flush_interval_seconds: 0
maintenance:
  enabled: false
  metadata_enabled: false
ducklake:
  metadata_path: "host=127.0.0.1 port=$E2E_DB_PORT dbname=ducklake user=ducklake password=ducklake"
  data_path: "$TMP_DIR/warehouse/"
  catalog_alias: softprobe
  metadata_schema: $SCHEMA
  workspace_scope_mode: shared
  extension_path: "$ROOT/target/ducklake-extension/ducklake.duckdb_extension"
session_summary:
  reducer_interval_ms: 100
EOF

echo "==> Building thelake and its embedded Explorer assets..."
make ducklake-extension
make build
THELAKE_BIN="$CARGO_TARGET_DIR/debug/thelake"
if [[ ! -x "$THELAKE_BIN" ]]; then
  echo "ERROR: built thelake binary not found at $THELAKE_BIN" >&2
  exit 1
fi

echo "==> Starting thelake with a temporary DuckLake and anonymous test workspace..."
DUCKDB_VERSION="$(awk '$1 == "duckdb_version" { print $2; exit }' "$ROOT/scripts/ducklake-extension.lock")"
DUCKDB_LIB_DIR="$(find "$CARGO_TARGET_DIR/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -path "*/$DUCKDB_VERSION/*" -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -n "$DUCKDB_LIB_DIR" ]]; then
  case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="$DUCKDB_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
    *) export LD_LIBRARY_PATH="$DUCKDB_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
  esac
fi
if [[ "$ONLINE_E2E" == "1" ]]; then
  export LISA_PUBLIC_URL="http://127.0.0.1:$LISA_PORT/v1"
fi

LISA_THELAKE_TOKEN="$LISA_TOKEN" \
CONFIG_FILE="$TMP_DIR/config.yaml" \
SOFTPROBE_LOCAL_ANONYMOUS=1 \
THELAKE_DEFAULT_WORKSPACE_ID="$WORKSPACE_ID" \
SOFTPROBE_LISTEN_ADDR="127.0.0.1:$PORT" \
SOFTPROBE_GRPC_DISABLE=1 \
RUST_LOG="${RUST_LOG:-info}" \
  "$THELAKE_BIN" >"$ARTIFACT_DIR/thelake.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 180); do
  if curl -fsS "$BACKEND/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$ARTIFACT_DIR/thelake.log" >&2
    echo "ERROR: thelake exited before becoming ready." >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  cat "$ARTIFACT_DIR/thelake.log" >&2
  echo "ERROR: thelake did not become ready within 180 seconds." >&2
  exit 1
fi

if [[ "$ONLINE_E2E" == "1" ]]; then
  LISA_WORKSPACE_ID="$WORKSPACE_ID" \
  LISA_DATA_DIR="$TMP_DIR/lisa-workspaces" \
  LISA_THELAKE_API_URL="$BACKEND" \
  LISA_THELAKE_TOKEN="$LISA_TOKEN" \
  LISA_LOCAL_ANONYMOUS=1 \
  LISA_QA_CRON="* * * * *" \
  LISA_SLACK_BOT_TOKEN="" LISA_SLACK_CHANNEL_ID="" \
    uv run --project "$ROOT/lisa" --extra openhands uvicorn lisa.app:app \
      --host 127.0.0.1 --port "$LISA_PORT" >"$ARTIFACT_DIR/lisa.log" 2>&1 &
  LISA_PID=$!
  lisa_ready=0
  for _ in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$LISA_PORT/health" >/dev/null 2>&1; then
      lisa_ready=1
      break
    fi
    if ! kill -0 "$LISA_PID" 2>/dev/null; then
      cat "$ARTIFACT_DIR/lisa.log" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ "$lisa_ready" != "1" ]]; then
    cat "$ARTIFACT_DIR/lisa.log" >&2
    echo "ERROR: Lisa did not become ready." >&2
    exit 1
  fi
fi

export THELAKE_E2E_URL="$BACKEND"
export THELAKE_API_PROXY="$BACKEND"

if [[ "$(uname -s)" == "Linux" && "${CI:-}" == "true" ]]; then
  (cd "$BROWSER_DIR" && npx playwright install --with-deps chromium)
else
  (cd "$BROWSER_DIR" && npx playwright install chromium)
fi

cd "$BROWSER_DIR"
npx playwright test --config playwright.explorer.config.ts "$@" 2>&1 | tee "$ARTIFACT_DIR/playwright.log"
