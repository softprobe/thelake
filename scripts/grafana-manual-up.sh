#!/usr/bin/env bash
# Start host Softprobe + pinned Grafana + OpenTelemetry Demo (Astronomy Shop)
# as the live OTLP traffic source.
# Usage (from repo root): ./scripts/grafana-manual-up.sh
# Teardown: ./scripts/grafana-manual-down.sh  (or: make grafana-down)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
COMPOSE_FILE="$ROOT/tests/compat/grafana/docker-compose.manual.yml"
GRAFANA_COMPOSE_IMAGE="${GRAFANA_COMPOSE_IMAGE:-$(python3 - "$ROOT/docs/compat/references.v0.yaml" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(
    r"(?ms)^\s+grafana:\s*\n\s+image:\s*([^\s#]+)\s*\n\s+tag:\s*[\"']?([^\s\"']+).*?\n\s+digest:\s*[\"']?(sha256:[0-9a-fA-F]{64})",
    text,
)
if not match:
    raise SystemExit("canonical Grafana manifest entry is missing an immutable digest")
print(f"{match.group(1)}@{match.group(3)}")
PY
)}"
export GRAFANA_COMPOSE_IMAGE
OVERLAY_DIR="$ROOT/tests/compat/grafana/otel-demo"
COLLECTOR_EXTRAS="$OVERLAY_DIR/otelcol-config-extras.yml"
COMPOSE_SOFTPROBE="$OVERLAY_DIR/compose.softprobe.yaml"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
CONFIG="$STATE_DIR/config.yaml"
GRAFANA_AUTH_MOCK_PORT="${GRAFANA_AUTH_MOCK_PORT:-18080}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
SOFTPROBE_URL_HOST="${SOFTPROBE_LISTEN:-http://127.0.0.1:8090}"
PG_HOST="${GRAFANA_PG_HOST:-127.0.0.1}"
PG_PORT="${GRAFANA_PG_HOST_PORT:-5434}"
PG_SCHEMA="${GRAFANA_PG_SCHEMA:-grafana_manual}"
ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-local-dev-admin-key}"
TENANT_ID="${GRAFANA_TENANT_ID:-local-dev-tenant}"
TENANT_SCHEMA="${GRAFANA_TENANT_SCHEMA:-${PG_SCHEMA}_local_dev_tenant}"

# Official Astronomy Shop pin (https://github.com/open-telemetry/opentelemetry-demo).
OTEL_DEMO_TAG="${OTEL_DEMO_TAG:-3.0.0}"
CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
DEMO_DIR="${OTEL_DEMO_DIR:-$CACHE_ROOT/otel-demo/$OTEL_DEMO_TAG}"
# Shared with tests/compat/grafana/browser/query_features.ts (H-04 catalog expr).
HISTOGRAM_BUCKET_RATE_EXPR_FILE="$ROOT/tests/compat/grafana/browser/catalog_gates/histogram_bucket_rate.expr"
DEMO_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"
STORE_URL="${OTEL_DEMO_STORE_URL:-http://127.0.0.1:8080}"

mkdir -p "$STATE_DIR/data/$TENANT_ID" "$STATE_DIR/data/_thelake_ops" "$STATE_DIR/cache" "$STATE_DIR/postgres"

port_busy() {
  local port="$1"
  ss -ltn 2>/dev/null | grep -qE ":${port}\\s" || return 1
}

if [[ -z "${SOFTPROBE_AUTH_URL:-}" ]] && [[ "$GRAFANA_AUTH_MOCK_PORT" == "18080" ]] && port_busy 18080; then
  if ! curl -sf -X POST "http://127.0.0.1:18080/validate" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1; then
    echo "WARN: :18080 busy with non-auth service; falling back GRAFANA_AUTH_MOCK_PORT=18085"
    GRAFANA_AUTH_MOCK_PORT=18085
  fi
fi
AUTH_URL="${SOFTPROBE_AUTH_URL:-http://127.0.0.1:${GRAFANA_AUTH_MOCK_PORT}/validate}"

our_softprobe_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "${pid:-}" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$cmd" == *softprobe-runtime* ]] || return 1
  return 0
}

demo_compose() {
  # shellcheck disable=SC2086
  (cd "$DEMO_DIR" && \
    DEMO_VERSION="$OTEL_DEMO_TAG" \
    IMAGE_VERSION="$OTEL_DEMO_TAG" \
    OTEL_COLLECTOR_CONFIG_EXTRAS="$COLLECTOR_EXTRAS" \
    $COMPOSE -p "$DEMO_PROJECT" \
      --env-file .env \
      -f compose.yaml \
      -f "$COMPOSE_SOFTPROBE" \
      "$@")
}

ensure_otel_demo_checkout() {
  if [[ -f "$DEMO_DIR/compose.yaml" ]]; then
    echo "==> OpenTelemetry Demo $OTEL_DEMO_TAG already at $DEMO_DIR"
    return 0
  fi
  echo "==> cloning OpenTelemetry Demo $OTEL_DEMO_TAG → $DEMO_DIR"
  mkdir -p "$(dirname "$DEMO_DIR")"
  rm -rf "$DEMO_DIR"
  git clone --depth 1 --branch "$OTEL_DEMO_TAG" \
    https://github.com/open-telemetry/opentelemetry-demo.git "$DEMO_DIR"
}

print_ready() {
  cat <<EOF

Grafana is ready for manual inspection (live Astronomy Shop traffic).

  Grafana:     http://127.0.0.1:3000  (admin / admin)
  Dashboards:  Astronomy Shop → GOLD overview + per-service boards
               Softprobe PromQL → capability smoke boards
               thelake ops → self-monitoring (datasource Softprobe Prometheus · ops)
  Softprobe:   $SOFTPROBE_URL_HOST  (Bearer $API_KEY; ops: local-ops-key → thelake-ops)
  DuckLake:    Postgres 19 catalog on $PG_HOST:$PG_PORT (schema $PG_SCHEMA)
  Parquet:     $STATE_DIR/data/
  Store UI:    $STORE_URL
  Demo pin:    $OTEL_DEMO_TAG  ($DEMO_DIR)
  Softprobe log: $LOG

Astronomy Shop boards monitor live multi-language demo services.
PromQL boards cover Softprobe's declared query subset.

Teardown: make grafana-down
EOF
}

wait_for_demo_metrics() {
  echo "==> waiting for Softprobe to see demo metrics"
  local ok=0
  local body=""
  for _ in $(seq 1 90); do
    body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
      "$SOFTPROBE_URL_HOST/api/v1/label/__name__/values" 2>/dev/null || true)"
    if [[ -n "$body" ]] && [[ "$body" != *'"data":[]'* ]] && [[ "$body" == *'"status":"success"'* ]]; then
      # Prefer evidence of multi-service / spanmetrics / http server metrics.
      if echo "$body" | grep -Eqi 'http_|traces_span|rpc_|process_|otelcol_|calls|duration'; then
        ok=1
        break
      fi
      # Any non-empty name list after collector is up is enough to proceed.
      if echo "$body" | grep -q '"data":\[.'; then
        ok=1
        break
      fi
    fi
    sleep 2
  done
  if [[ "$ok" != 1 ]]; then
    echo "ERROR: no metrics appeared in Softprobe after starting OTel Demo." >&2
    echo "  last /api/v1/label/__name__/values: ${body:-<empty>}" >&2
    echo "  collector: docker logs otel-collector 2>&1 | tail -40" >&2
    exit 1
  fi
  echo "==> Softprobe metric names: $(echo "$body" | head -c 400)…"

  # Require real scrape continuity — lookback of one sample draws flat Grafana lines.
  # Prefer a counter that moves under load (not k6_vus, which can be constant).
  echo "==> waiting for non-identical Prom samples (live scrapes)"
  local vary=0
  local end start payload changes q
  for _ in $(seq 1 90); do
    end="$(date +%s)"
    start="$((end - 600))"
    for q in \
      'k6_http_reqs' \
      'http_server_request_duration_count' \
      'traces_span_metrics_calls' \
      'demo_ad_served_total' \
      'k6_iterations'
    do
      payload="$(curl -sf -m 30 -H "Authorization: Bearer $API_KEY" \
        -H "X-Scope-OrgID: $TENANT_ID" \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        --data-urlencode "query=$q" \
        --data "start=$start&end=$end&step=15" \
        "$SOFTPROBE_URL_HOST/api/v1/query_range" 2>/dev/null || true)"
      printf '%s' "$payload" > /tmp/thelake-grafana-prom-live.json
      changes="$(python3 - <<'PY'
import json
try:
    d = json.load(open("/tmp/thelake-grafana-prom-live.json"))
except Exception:
    print(0)
    raise SystemExit
rows = (d.get("data") or {}).get("result") or []
best = 0
for s in rows:
    vals = [float(v) for _, v in (s.get("values") or [])]
    ch = sum(1 for a, b in zip(vals, vals[1:]) if a != b)
    best = max(best, ch)
print(best)
PY
)"
      if [[ "${changes:-0}" -ge 2 ]]; then
        vary=1
        echo "==> live scrapes OK ($q value changes=$changes)"
        break 2
      fi
    done
    sleep 5
  done
  if [[ "$vary" != 1 ]]; then
    echo "ERROR: Prom series stayed flat (lookback of a single scrape). Ingest is not continuous." >&2
    echo "  collector: docker logs otel-collector 2>&1 | tail -60" >&2
    echo "  softprobe: tail -60 $LOG" >&2
    exit 1
  fi

  wait_for_histogram_bucket_rates
}

# rate() on classic _bucket needs ≥2 raw samples per series in the window.
# Expr is shared with browser H-04 (catalog_gates/histogram_bucket_rate.expr).
wait_for_histogram_bucket_rates() {
  local bucket_q
  if [[ ! -f "$HISTOGRAM_BUCKET_RATE_EXPR_FILE" ]]; then
    echo "ERROR: missing H-04 expr file: $HISTOGRAM_BUCKET_RATE_EXPR_FILE" >&2
    exit 1
  fi
  bucket_q="$(tr -d '\n' <"$HISTOGRAM_BUCKET_RATE_EXPR_FILE")"
  echo "==> waiting for classic histogram bucket rates (H-04)"
  local bucket_rate_ok=0
  local end start payload
  for _ in $(seq 1 60); do
    end="$(date +%s)"
    start="$((end - 600))"
    payload="$(curl -sf -m 30 -H "Authorization: Bearer $API_KEY" \
      -H "X-Scope-OrgID: $TENANT_ID" \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode "query=$bucket_q" \
      --data "start=$start&end=$end&step=15" \
      "$SOFTPROBE_URL_HOST/api/v1/query_range" 2>/dev/null || true)"
    printf '%s' "$payload" > /tmp/thelake-grafana-prom-bucket-rate.json
    if python3 - <<'PY'
import json
try:
    d = json.load(open("/tmp/thelake-grafana-prom-bucket-rate.json"))
except Exception:
    raise SystemExit(1)
rows = (d.get("data") or {}).get("result") or []
ok = sum(1 for s in rows if "le" in ((s.get("metric") or {}))) >= 3
raise SystemExit(0 if ok else 1)
PY
    then
      bucket_rate_ok=1
      echo "==> histogram bucket rates OK ($bucket_q)"
      break
    fi
    sleep 5
  done
  if [[ "$bucket_rate_ok" != 1 ]]; then
    echo "ERROR: classic histogram bucket rate query stayed empty (need ≥2 samples/series in 5m)." >&2
    echo "  query: $bucket_q" >&2
    echo "  collector: docker logs otel-collector 2>&1 | tail -60" >&2
    exit 1
  fi
}

wait_for_demo_logs() {
  echo "==> waiting for Softprobe Loki labels in the live Explore window"
  local ok=0
  local body=""
  local end_ns start_ns
  for _ in $(seq 1 90); do
    end_ns="$(python3 -c 'import time; print(int(time.time()*1e9))')"
    start_ns="$((end_ns - 3600 * 1000000000))"
    body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
      -H "X-Scope-OrgID: $TENANT_ID" \
      "$SOFTPROBE_URL_HOST/loki/api/v1/labels?start=$start_ns&end=$end_ns" 2>/dev/null || true)"
    if [[ -n "$body" ]] && [[ "$body" == *'"status":"success"'* ]] \
      && [[ "$body" == *'"data":['* ]] && [[ "$body" != *'"data":[]'* ]]; then
      ok=1
      break
    fi
    sleep 2
  done
  if [[ "$ok" != 1 ]]; then
    echo "ERROR: no Loki labels appeared in the last hour after starting OTel Demo." >&2
    echo "  last /loki/api/v1/labels: ${body:-<empty>}" >&2
    echo "  collector: docker logs otel-collector 2>&1 | tail -40" >&2
    exit 1
  fi
  echo "==> Softprobe Loki labels (live window): $(echo "$body" | head -c 400)…"
}

# Reuse if Softprobe + Grafana + demo collector already healthy *and* ingest is live (disabled if GRAFANA_REUSE_STACK=0).
if [[ "${GRAFANA_REUSE_STACK:-1}" == "1" ]] \
  && our_softprobe_running \
  && curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1 \
  && curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1 \
  && docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -q true; then
  # Flat lookback lines mean the collector is timing out — do not claim "already up".
  end_now="$(date +%s)"
  start_now="$((end_now - 600))"
  live_changes=0
  for test_q in 'k6_http_reqs' 'demo_ad_served_total' 'k6_iterations' 'http_server_request_duration_count'; do
    ch="$(curl -sf -m 20 -H "Authorization: Bearer $API_KEY" \
      -H "X-Scope-OrgID: $TENANT_ID" \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-urlencode "query=$test_q" \
      --data "start=$start_now&end=$end_now&step=15" \
      "$SOFTPROBE_URL_HOST/api/v1/query_range" 2>/dev/null \
      | python3 -c 'import sys,json
try:
 d=json.load(sys.stdin); r=(d.get("data") or {}).get("result") or []; best=0
 for s in r:
  vals=[float(v) for _,v in (s.get("values") or [])]
  best=max(best, sum(1 for a,b in zip(vals,vals[1:]) if a!=b))
 print(best)
except Exception:
 print(0)' || echo 0)"
    if [[ "${ch:-0}" -ge 1 ]]; then
      live_changes="$ch"
      break
    fi
  done
  if [[ "${live_changes:-0}" -ge 1 ]]; then
    end_ns="$(python3 -c 'import time; print(int(time.time()*1e9))')"
    start_ns="$((end_ns - 3600 * 1000000000))"
    loki_body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
      -H "X-Scope-OrgID: $TENANT_ID" \
      "$SOFTPROBE_URL_HOST/loki/api/v1/labels?start=$start_ns&end=$end_ns" 2>/dev/null || true)"
    if [[ -n "$loki_body" ]] && [[ "$loki_body" == *'"status":"success"'* ]] \
      && [[ "$loki_body" == *'"data":['* ]] && [[ "$loki_body" != *'"data":[]'* ]]; then
      wait_for_histogram_bucket_rates
      echo "already up (owned Softprobe pid=$(cat "$PID_FILE") + otel-collector, live Prom + Loki OK)."
      print_ready
      exit 0
    fi
    echo "already up with live Prom but Loki labels empty in the last hour; rebuilding stack."
  fi
  echo "already up but Prom series are flat (changes=${live_changes:-0}); rebuilding stack for live ingest."
fi

if port_busy 8090 && ! our_softprobe_running; then
  echo "ERROR: :8090 is in use by another process. Stop it or make grafana-down first." >&2
  exit 1
fi
if port_busy 3000 && ! curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
  echo "ERROR: :3000 is in use but not our Grafana. Free the port or set a different mapping." >&2
  exit 1
fi
if port_busy "$PG_PORT" && ! docker inspect -f '{{.State.Running}}' thelake-grafana-postgres 2>/dev/null | grep -q true; then
  echo "ERROR: :$PG_PORT is in use by another process. Stop it or set GRAFANA_PG_HOST_PORT." >&2
  exit 1
fi
if port_busy 8080; then
  echo "WARN: :8080 busy — Astronomy Shop UI may fail to bind (ENVOY_PORT). Softprobe ingest can still work." >&2
fi

if [[ -f "$PID_FILE" ]]; then
  old="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old:-}" ]] && kill -0 "$old" 2>/dev/null; then
    cmd="$(ps -p "$old" -o args= 2>/dev/null || true)"
    if [[ "$cmd" == *softprobe-runtime* ]]; then
      kill "$old" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$old" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$old" 2>/dev/null || true
    fi
  fi
  rm -f "$PID_FILE"
fi

reset_grafana_state() {
  THELAKE_GRAFANA_STATE_DIR="$STATE_DIR" $COMPOSE -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  rm -rf "$STATE_DIR/data" "$STATE_DIR/cache"
  if [[ -d "$STATE_DIR/postgres" ]]; then
    docker run --rm -v "$STATE_DIR/postgres:/data" alpine sh -c 'rm -rf /data/* /data/.[!.]* /data/..?*' >/dev/null 2>&1 || true
  fi
  mkdir -p "$STATE_DIR/data/$TENANT_ID" "$STATE_DIR/data/_thelake_ops" "$STATE_DIR/cache" "$STATE_DIR/postgres"
}

reset_grafana_state

echo "==> building softprobe-runtime (release; AC-S3)"
if [[ -f "$ROOT/Makefile" ]] && grep -q '^build-release:' "$ROOT/Makefile"; then
  make -C "$ROOT" build-release
else
  cargo build -q --release --bin softprobe-runtime
fi

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$CACHE_ROOT/target}"
RUNTIME_BIN="$ROOT/dist/softprobe-runtime"
if [[ ! -x "$RUNTIME_BIN" ]]; then
  RUNTIME_BIN="${CARGO_TARGET_DIR}/release/softprobe-runtime"
fi
if [[ ! -x "$RUNTIME_BIN" ]]; then
  RUNTIME_BIN="$ROOT/target/release/softprobe-runtime"
fi
if [[ ! -x "$RUNTIME_BIN" ]]; then
  echo "ERROR: missing $RUNTIME_BIN (expected release binary)" >&2
  exit 1
fi

echo "==> starting Grafana + auth-mock + Postgres 19"
THELAKE_GRAFANA_STATE_DIR="$STATE_DIR" GRAFANA_PG_HOST_PORT="$PG_PORT" GRAFANA_AUTH_MOCK_PORT="$GRAFANA_AUTH_MOCK_PORT" \
  $COMPOSE -f "$COMPOSE_FILE" up -d

echo "==> waiting for auth-mock"
auth_ok=0
for _ in $(seq 1 40); do
  if curl -sf -X POST "$AUTH_URL" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1; then
    auth_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$auth_ok" != 1 ]]; then
  echo "ERROR: auth-mock did not become ready at $AUTH_URL" >&2
  exit 1
fi

echo "==> waiting for Postgres on $PG_HOST:$PG_PORT"
pg_ok=0
for _ in $(seq 1 60); do
  if docker inspect -f '{{.State.Health.Status}}' thelake-grafana-postgres 2>/dev/null | grep -q healthy; then
    pg_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$pg_ok" != 1 ]]; then
  echo "ERROR: Postgres did not become healthy on $PG_HOST:$PG_PORT" >&2
  exit 1
fi

# TWCS default on for ops compaction panels. Browser CI sets
# THELAKE_MAINTENANCE_ENABLED=false — open-day waves under demo load starve
# fresh k6_http_reqs (I-02/I-03), matching main's Grafana SLO stance.
case "${THELAKE_MAINTENANCE_ENABLED:-true}" in
  0|false|FALSE|no|NO|off|OFF) MAINTENANCE_ENABLED=false ;;
  *) MAINTENANCE_ENABLED=true ;;
esac

cat >"$CONFIG" <<EOF
server:
  port: 8090
  host: "0.0.0.0"
  max_body_size: 104857600
  worker_threads: null

object_store:
  region: "us-east-1"
  endpoint: null

query:
  max_connections: 16
  cache_dir: "$STATE_DIR/cache"

# Demo: TWCS on by default (ops panels). Override with THELAKE_MAINTENANCE_ENABLED.
# Keep merge light so open-day waves cannot peg CPU under Grafana refresh.
maintenance:
  enabled: ${MAINTENANCE_ENABLED}
  target_file_size_bytes: 67108864
  interval_seconds: 300
  metadata_enabled: true
  metadata_interval_seconds: 300
  max_snapshot_age_seconds: 60
  remove_orphan_files_enabled: true
  remove_orphan_older_than_seconds: 60
  open_day_file_cap: 64
  max_waves_per_table: 1
  max_compacted_files_per_wave: 16
  closed_day_max_compacted_files: 128
  closed_day_max_waves: 2
  max_merge_file_size_bytes: 8388608

ducklake:
  catalog_type: "postgres"
  metadata_path: "host=$PG_HOST port=$PG_PORT dbname=ducklake user=ducklake password=ducklake"
  data_path: "$STATE_DIR/data/"
  catalog_alias: "softprobe"
  metadata_schema: "$PG_SCHEMA"
  data_inlining_row_limit: 0
  writer_pool_size: 4

dropdown_catalog:
  enabled: false

# Self-monitoring ops lake (Design 2) — Grafana Softprobe Prometheus · ops.
self_monitoring:
  enabled: true
  export_interval_seconds: 15
  ops_metadata_schema: thelake_ops
  ops_data_path: "$STATE_DIR/data/_thelake_ops/"
EOF

TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
DUCKDB_LIB_DIR="$(find "${TARGET_DIR}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -z "${DUCKDB_LIB_DIR}" ]]; then
  DUCKDB_LIB_DIR="$(find "${ROOT}/target/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
fi
if [[ -z "${DUCKDB_LIB_DIR}" && -f "$ROOT/dist/libduckdb.so" ]]; then
  DUCKDB_LIB_DIR="$ROOT/dist"
fi
if [[ -z "${DUCKDB_LIB_DIR}" && -f "$ROOT/dist/libduckdb.dylib" ]]; then
  DUCKDB_LIB_DIR="$ROOT/dist"
fi
if [[ -z "${DUCKDB_LIB_DIR}" ]]; then
  echo "ERROR: libduckdb not found under ${TARGET_DIR}/duckdb-download (build with DUCKDB_DOWNLOAD_LIB=1?)" >&2
  exit 1
fi
case "$(uname -s)" in
  Darwin)
    export DYLD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
    # SIP often strips DYLD_* for child processes; @executable_path next to staged dylib is reliable.
    if [[ -f "$ROOT/dist/libduckdb.dylib" ]] && command -v install_name_tool >/dev/null 2>&1; then
      install_name_tool -add_rpath @executable_path "$RUNTIME_BIN" 2>/dev/null || true
    fi
    ;;
  *) export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
esac

echo "==> starting Softprobe on :8090"
export CONFIG_FILE="$CONFIG"
export SOFTPROBE_AUTH_URL="$AUTH_URL"
export SOFTPROBE_ADMIN_API_KEY="$ADMIN_API_KEY"
export SOFTPROBE_GRPC_DISABLE=1
export RUST_LOG="${RUST_LOG:-info}"
: >"$LOG"
# Detach from the launcher process group so Softprobe survives when Make/CI
# shells exit (Cursor agent shells tear down the whole tree otherwise).
# Linux: setsid. Darwin: double-fork + setsid-equivalent via perl.
start_softprobe_detached() {
  if command -v setsid >/dev/null 2>&1; then
    setsid "$RUNTIME_BIN" >>"$LOG" 2>&1 &
    echo $! >"$PID_FILE"
    return
  fi
  # macOS: no setsid; perl double-fork orphans the runtime from Make/agent shells.
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
  local ok=0
  for _ in $(seq 1 40); do
    if [[ -f "$PID_FILE" ]]; then
      local pid
      pid="$(tr -d "[:space:]" <"$PID_FILE" || true)"
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        ok=1
        break
      fi
    fi
    sleep 0.25
  done
  if [[ "$ok" != 1 ]]; then
    echo "ERROR: Softprobe did not appear after detach start" >&2
    tail -40 "$LOG" >&2 || true
    exit 1
  fi
}
start_softprobe_detached
disown || true

echo "==> waiting for Softprobe /ready"
ok=0
for _ in $(seq 1 60); do
  if curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 0.5
done
if [[ "$ok" != 1 ]]; then
  echo "ERROR: Softprobe did not become ready; log: $LOG" >&2
  tail -40 "$LOG" >&2 || true
  exit 1
fi

echo "==> provisioning tenant $TENANT_ID (Postgres catalog)"
tenant_payload="$(TENANT_ID="$TENANT_ID" TENANT_SCHEMA="$TENANT_SCHEMA" TENANT_DATA_PATH="$STATE_DIR/data/$TENANT_ID/" python3 - <<'PY'
import json, os
print(json.dumps({
    "tenantId": os.environ["TENANT_ID"],
    "storageHints": {
        "ducklakeMetadataSchema": os.environ["TENANT_SCHEMA"],
        "ducklakeDataPath": os.environ["TENANT_DATA_PATH"],
        "gcsBucket": "warehouse",
    },
}))
PY
)"
tenant_http="$(curl -sS -o /tmp/thelake-grafana-tenant-provision.json -w '%{http_code}' \
  -X POST "$SOFTPROBE_URL_HOST/v1/tenants" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d "$tenant_payload" || true)"
if [[ "$tenant_http" != "200" && "$tenant_http" != "201" ]]; then
  echo "ERROR: tenant provisioning returned HTTP ${tenant_http:-curl-fail}" >&2
  cat /tmp/thelake-grafana-tenant-provision.json >&2 || true
  exit 1
fi

# Prefer typed hot columns for Prom/Grafana selectors before demo traffic.
# shellcheck source=scripts/lib/apply-prom-hot-labels.sh
source "$ROOT/scripts/lib/apply-prom-hot-labels.sh"
apply_prom_hot_labels "$SOFTPROBE_URL_HOST" "$API_KEY"

echo "==> waiting for Grafana"
graf_ok=0
for _ in $(seq 1 180); do
  if curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
    graf_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$graf_ok" != 1 ]]; then
  echo "ERROR: Grafana did not become ready on :3000" >&2
  exit 1
fi

ensure_otel_demo_checkout

echo "==> starting OpenTelemetry Demo $OTEL_DEMO_TAG (minimal, Softprobe backend)"
demo_compose up --pull missing --remove-orphans --detach
demo_compose restart otel-collector >/dev/null 2>&1 || true

wait_for_demo_metrics
wait_for_demo_logs
print_ready
