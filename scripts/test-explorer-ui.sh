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

ONLINE_E2E="${THELAKE_EXPLORER_E2E_ONLINE:-0}"
E2E_COMPOSE_PROJECT=""
E2E_RUNNER_PORT=""
E2E_DB_PORT="5432"
if [[ "$ONLINE_E2E" == "1" ]]; then
  command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is required for online evaluation E2E." >&2; exit 1; }
  if [[ -z "${GOOGLE_API_KEY:-}" && -n "${GEMINI_API_KEY:-}" ]]; then export GOOGLE_API_KEY="$GEMINI_API_KEY"; fi
  [[ -n "${GOOGLE_API_KEY:-}" ]] || { echo "ERROR: Set GOOGLE_API_KEY (or GEMINI_API_KEY) for live Gemini agent and evaluator calls." >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "ERROR: Start Docker before online evaluation E2E." >&2; exit 1; }
  E2E_COMPOSE_PROJECT="thelake-explorer-e2e-$$"
  read -r E2E_DB_PORT E2E_RUNNER_PORT < <(python3 - <<'PY'
import socket
chosen = []
for start, end in ((55440, 55460), (18100, 18120)):
    selected = None
    for port in range(start, end):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
                selected = port
                break
            except OSError:
                continue
    if selected is None:
        raise SystemExit(f"No free localhost port in {start}-{end - 1}.")
    chosen.append(selected)
print(*chosen)
PY
)
  export THELAKE_QUICKSTART_DB_PORT="$E2E_DB_PORT"
  export THELAKE_QUICKSTART_RUNNER_PORT="$E2E_RUNNER_PORT"
  if [[ "$(uname -s)" == "Linux" ]]; then
    export THELAKE_QUICKSTART_AGENT_NETWORK=host
  fi
  export THELAKE_EVALUATION_RUNNER_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
  export THELAKE_E2E_COMPOSE_PROJECT="$E2E_COMPOSE_PROJECT"
  export THELAKE_E2E_LAKE_PORT="$PORT"
fi

if [[ ! -x "$BROWSER_DIR/node_modules/.bin/playwright" ]]; then
  npm --prefix "$BROWSER_DIR" ci --no-audit
fi
if [[ ! -x "$EXPLORER_DIR/node_modules/.bin/vite" ]]; then
  npm --prefix "$EXPLORER_DIR" ci --no-audit
fi

THELAKE_CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$THELAKE_CACHE_ROOT/target}"
BACKEND="http://127.0.0.1:$PORT"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/thelake-explorer-e2e.XXXXXX")"
mkdir -p "$TMP_DIR/warehouse"
WORKSPACE_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SCHEMA="explorer_e2e_$(python3 -c 'import os; print(os.getpid())')"
SERVER_PID=""

cleanup() {
  if [[ -n "$E2E_COMPOSE_PROJECT" ]]; then
      GOOGLE_API_KEY="${GOOGLE_API_KEY:-}" \
      THELAKE_EVALUATION_RUNNER_TOKEN="${THELAKE_EVALUATION_RUNNER_TOKEN:-}" \
      THELAKE_QUICKSTART_DB_PORT="$E2E_DB_PORT" \
      THELAKE_QUICKSTART_RUNNER_PORT="$E2E_RUNNER_PORT" \
      docker compose --project-name "$E2E_COMPOSE_PROJECT" \
        --file "$ROOT/examples/quickstart/compose.yaml" down --remove-orphans -v >/dev/null 2>&1 || true
  fi
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "${THELAKE_E2E_KEEP_ARTIFACTS:-0}" == "1" ]]; then
    echo "Explorer E2E logs and temporary data kept at $TMP_DIR" >&2
  else
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

if [[ "$ONLINE_E2E" == "1" ]]; then
  echo "==> Starting isolated real Gemini evaluation runner..."
  docker compose --project-name "$E2E_COMPOSE_PROJECT" \
    --file "$ROOT/examples/quickstart/compose.yaml" \
    up --build --detach postgres evaluation-runner
  db_ready=0
  for _ in $(seq 1 60); do
    if docker compose --project-name "$E2E_COMPOSE_PROJECT" \
      --file "$ROOT/examples/quickstart/compose.yaml" \
      exec --no-TTY postgres pg_isready -U ducklake -d ducklake >/dev/null 2>&1; then
      db_ready=1
      break
    fi
    sleep 1
  done
  if [[ "$db_ready" != "1" ]]; then
    docker compose --project-name "$E2E_COMPOSE_PROJECT" \
      --file "$ROOT/examples/quickstart/compose.yaml" logs postgres >&2
    echo "ERROR: isolated E2E Postgres did not become ready." >&2
    exit 1
  fi
  runner_ready=0
  for _ in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$E2E_RUNNER_PORT/health" >/dev/null 2>&1; then
      runner_ready=1
      break
    fi
    sleep 1
  done
  if [[ "$runner_ready" != 1 ]]; then
    docker compose --project-name "$E2E_COMPOSE_PROJECT" \
      --file "$ROOT/examples/quickstart/compose.yaml" logs evaluation-runner >&2
    echo "ERROR: live evaluator runner did not become ready." >&2
    exit 1
  fi
  export THELAKE_EVALUATION_RUNNER_URL="http://127.0.0.1:$E2E_RUNNER_PORT/v1/evaluate"
fi

if curl -fsS "$BACKEND/ready" >/dev/null 2>&1; then
  echo "ERROR: Explorer E2E backend port $PORT is already serving a process." >&2
  exit 1
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
CONFIG_FILE="$TMP_DIR/config.yaml" \
SOFTPROBE_LOCAL_ANONYMOUS=1 \
THELAKE_DEFAULT_WORKSPACE_ID="$WORKSPACE_ID" \
SOFTPROBE_LISTEN_ADDR="127.0.0.1:$PORT" \
SOFTPROBE_GRPC_DISABLE=1 \
THELAKE_EVALUATION_RUNNER_URL="${THELAKE_EVALUATION_RUNNER_URL:-}" \
THELAKE_EVALUATION_RUNNER_TOKEN="${THELAKE_EVALUATION_RUNNER_TOKEN:-}" \
RUST_LOG="${RUST_LOG:-info}" \
  "$THELAKE_BIN" >"$TMP_DIR/thelake.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 180); do
  if curl -fsS "$BACKEND/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$TMP_DIR/thelake.log" >&2
    echo "ERROR: thelake exited before becoming ready." >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  cat "$TMP_DIR/thelake.log" >&2
  echo "ERROR: thelake did not become ready within 180 seconds." >&2
  exit 1
fi

export THELAKE_E2E_URL="$BACKEND"
export THELAKE_API_PROXY="$BACKEND"

if [[ "$(uname -s)" == "Linux" && "${CI:-}" == "true" ]]; then
  (cd "$BROWSER_DIR" && npx playwright install --with-deps chromium)
else
  (cd "$BROWSER_DIR" && npx playwright install chromium)
fi

cd "$BROWSER_DIR"
npx playwright test --config playwright.explorer.config.ts "$@"
