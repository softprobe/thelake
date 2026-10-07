#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
EXPLORER_DIR="$ROOT/packages/thelake-explorer"

command -v node >/dev/null 2>&1 || { echo "ERROR: node is required for Explorer UI E2E." >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "ERROR: cargo is required for Explorer UI E2E." >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required for Explorer UI E2E." >&2; exit 1; }

if [[ ! -x "$BROWSER_DIR/node_modules/.bin/playwright" ]]; then
  npm --prefix "$BROWSER_DIR" ci --no-audit
fi
if [[ ! -x "$EXPLORER_DIR/node_modules/.bin/vite" ]]; then
  npm --prefix "$EXPLORER_DIR" ci --no-audit
fi

THELAKE_CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$THELAKE_CACHE_ROOT/target}"
PORT="${THELAKE_EXPLORER_E2E_PORT:-18090}"
BACKEND="http://127.0.0.1:$PORT"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/thelake-explorer-e2e.XXXXXX")"
mkdir -p "$TMP_DIR/warehouse"
WORKSPACE_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SCHEMA="explorer_e2e_$(python3 -c 'import os; print(os.getpid())')"
SERVER_PID=""

cleanup() {
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
  metadata_path: "host=127.0.0.1 port=5432 dbname=ducklake user=ducklake password=ducklake"
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
