#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
EXPLORER_DIR="$ROOT/packages/thelake-explorer"

command -v node >/dev/null 2>&1 || { echo "ERROR: node is required for Explorer UI E2E." >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "ERROR: cargo is required for Explorer UI E2E." >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required for Explorer UI E2E." >&2; exit 1; }
ONLINE_E2E="${THELAKE_EXPLORER_E2E_ONLINE:-0}"
E2E_DB_PORT=""
if [[ "$ONLINE_E2E" == "1" ]]; then
  command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is required for online evaluation E2E." >&2; exit 1; }
  if [[ -z "${GOOGLE_API_KEY:-}" && -n "${GEMINI_API_KEY:-}" ]]; then export GOOGLE_API_KEY="$GEMINI_API_KEY"; fi
  [[ -n "${GOOGLE_API_KEY:-}" ]] || { echo "ERROR: Set GOOGLE_API_KEY (or GEMINI_API_KEY) for live Gemini agent and evaluator calls." >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "ERROR: Start Docker before online evaluation E2E." >&2; exit 1; }
  LISA_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  export LISA_THELAKE_TOKEN="$LISA_TOKEN"
  export THELAKE_QUICKSTART_INSTANCE_ID="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
  export THELAKE_QUICKSTART_DB_PORT=0
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
ARTIFACT_DIR="$ROOT/target/test-artifacts/explorer-ui/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$ARTIFACT_DIR"
WORKSPACE_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SCHEMA="explorer_e2e_$(python3 -c 'import os; print(os.getpid())')"
SERVER_PID=""
PORT_FILE=""
PORT=""
BACKEND=""
LISA_PORT=""
LISA_TOKEN=""

cleanup() {
  local status=$?
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "$E2E_STACK_STATE" ]]; then
    python3 "$ROOT/scripts/test_stack.py" stop --state-file "$E2E_STACK_STATE" || status=$?
  fi
  if [[ -z "$E2E_STACK_STATE" ]]; then
    rm -rf "$TMP_DIR"
  fi
  echo "Explorer E2E artifacts retained at $ARTIFACT_DIR" >&2
  exit "$status"
}
trap cleanup EXIT

if [[ "$ONLINE_E2E" == "1" ]]; then
  COMPOSE_FILE="$ROOT/examples/quickstart/compose.yaml"
  POSTGRES_SERVICE="postgres"
  STACK_SUITE="explorer-online"
else
  COMPOSE_FILE="$ROOT/tests/compose.test.yaml"
  POSTGRES_SERVICE="explorer-postgres"
  STACK_SUITE="explorer-ui"
fi
stack_json="$(python3 scripts/test_stack.py start-compose \
  --suite "$STACK_SUITE" \
  --artifact-dir "$ARTIFACT_DIR" \
  --compose-file "$COMPOSE_FILE" \
  --service "$POSTGRES_SERVICE" \
  --published-port "$POSTGRES_SERVICE:5432" \
  --wait)"
E2E_STACK_STATE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["state_file"])' "$stack_json")"
TMP_DIR="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["scratch_dir"])' "$stack_json")"
E2E_DB_PORT="$(python3 -c 'import json,sys; s=json.loads(sys.argv[1]); k=s["services"][0]+":5432"; print(s["ports"][k][0].rsplit(":",1)[1])' "$stack_json")"
mkdir -p "$TMP_DIR/warehouse"

cat > "$TMP_DIR/config.yaml" <<EOF
server:
  host: 127.0.0.1
  port: 0
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
PORT_FILE="$TMP_DIR/service-ports.json"
service_args=(
  --port-file "$PORT_FILE"
  --thelake-log "$ARTIFACT_DIR/thelake.log"
  --lisa-log "$ARTIFACT_DIR/lisa.log"
  --thelake-bin "$THELAKE_BIN"
  --config-file "$TMP_DIR/config.yaml"
  --workspace-id "$WORKSPACE_ID"
  --lisa-token "$LISA_TOKEN"
  --lisa-data-dir "$TMP_DIR/lisa-workspaces"
)
if [[ "$ONLINE_E2E" == "1" ]]; then service_args+=(--online); fi
RUST_LOG="${RUST_LOG:-info}" python3 scripts/run-explorer-e2e-services.py "${service_args[@]}" &
SERVER_PID=$!

for _ in $(seq 1 30); do
  [[ -s "$PORT_FILE" ]] && break
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: Explorer E2E services exited before assigning ports." >&2
    exit 1
  fi
  sleep 0.1
done
if [[ ! -s "$PORT_FILE" ]]; then
  echo "ERROR: Explorer E2E services did not assign ports." >&2
  exit 1
fi
PORT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["thelake"])' "$PORT_FILE")"
BACKEND="http://127.0.0.1:$PORT"
if [[ "$ONLINE_E2E" == "1" ]]; then
  LISA_PORT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lisa"])' "$PORT_FILE")"
fi

ready=0
for _ in $(seq 1 180); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$ARTIFACT_DIR/thelake.log" >&2
    echo "ERROR: thelake exited before becoming ready." >&2
    exit 1
  fi
  if curl -fsS "$BACKEND/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  cat "$ARTIFACT_DIR/thelake.log" >&2
  echo "ERROR: thelake did not become ready within 180 seconds." >&2
  exit 1
fi

if [[ "$ONLINE_E2E" == "1" ]]; then
  lisa_ready=0
  for _ in $(seq 1 90); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      cat "$ARTIFACT_DIR/lisa.log" >&2
      echo "ERROR: Lisa exited before becoming ready." >&2
      exit 1
    fi
    if curl -fsS "http://127.0.0.1:$LISA_PORT/health" >/dev/null 2>&1; then
      lisa_ready=1
      break
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
