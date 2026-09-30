#!/usr/bin/env bash
# Seeded LLM/agent performance bench: local lake + agent ingest + session APIs.
# make bench-llm-seeded
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SEED_DIR="${SEED_DIR:-${HOME}/data/thelake-seed/scrubbed/northwind}"
PORT="${PORT:-38190}"
DURATION="${DURATION:-60}"
IDLE_SECS="${IDLE_SECS:-60}"
IDLE_COOLDOWN_SECS="${IDLE_COOLDOWN_SECS:-15}"
SESSION_QPS="${SESSION_QPS:-2}"
LLM_INTERVAL_MS="${LLM_INTERVAL_MS:-500}"
API_TOKEN="${API_TOKEN:-test-token}"
REPORT_JSON="${REPORT_JSON:-/tmp/thelake-bench-llm-seeded.json}"
TMP_CONFIG="/tmp/thelake-bench-llm-seeded.yaml"
LOG="/tmp/thelake-bench-llm-seeded.log"
# Local Bearer → default lake (docs/compat/auth.md); required for /v1/* without assertion JWT.
export SOFTPROBE_DEFAULT_TENANT_KEY="${SOFTPROBE_DEFAULT_TENANT_KEY:-llm-bench}"
export SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-llm-bench-admin}"

if [[ ! -d "${SEED_DIR}" ]]; then
  echo "SEED_DIR missing: ${SEED_DIR}" >&2
  echo "Dump+scrub outside git first (see ~/ops/thelake-seed/README.md)." >&2
  echo "Or set SEED_DIR to a scrubbed seed tree." >&2
  exit 1
fi

make setup

# MinIO credentials for local object store (same as make test / stress).
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

# Dedicated schema/warehouse under /tmp to avoid clobbering default warehouse.
WAREHOUSE="/tmp/thelake-bench-llm-warehouse"
SCHEMA="thelake_bench_llm"
rm -rf "${WAREHOUSE}"
mkdir -p "${WAREHOUSE}"

# Prod-aligned knobs (from production deploy generated config), local paths only.
# Do not inherit local config.yaml maintenance.interval_seconds=60 / missing session_summary.
python3 - <<PY
from pathlib import Path
text = f"""
server:
  port: ${PORT}
  host: "127.0.0.1"
  max_body_size: 104857600
  worker_threads: null
object_store:
  region: "us-east-1"
  endpoint: "http://127.0.0.1:9000"
query:
  max_connections: 10
  cache_dir: "/tmp/thelake-bench-llm-cache"
ingest:
  flush_interval_seconds: 2
  buffer_size_mb: 256
  write_timeout_seconds: 60
async_jobs:
  lease_ttl_seconds: 120
  heartbeat_seconds: 30
session_summary:
  reducer_interval_ms: 10000
  rebuild_interval_ms: 86400000
  max_sessions_per_reduce: 1000
  max_reduce_span_seconds: 604800
self_monitoring:
  enabled: true
  export_interval_seconds: 60
maintenance:
  enabled: true
  interval_seconds: 3600
  metadata_enabled: true
  reader_safety_grace_seconds: 300
ducklake:
  metadata_path: "host=127.0.0.1 port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "${WAREHOUSE}/"
  catalog_alias: "softprobe"
  metadata_schema: "${SCHEMA}"
  data_inlining_row_limit: 500
  writer_pool_size: 4
"""
Path("${TMP_CONFIG}").write_text(text.lstrip() + "\\n")
print("wrote", "${TMP_CONFIG}", "(prod-aligned knobs, local store)")
PY

# Drop prior PG schema if present
docker exec ducklake-postgres psql -U ducklake -d ducklake -c \
  "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;" >/dev/null 2>&1 || true

echo "Loading seed from ${SEED_DIR}…"
SEED_FORCE=1 CONFIG_FILE="${TMP_CONFIG}" SEED_DIR="${SEED_DIR}" \
  SEED_TENANT_ID="${SOFTPROBE_DEFAULT_TENANT_KEY}" \
  ./scripts/seed-lake-from-parquet.sh

echo "Starting thelake on :${PORT}…"
export LD_LIBRARY_PATH="${HOME}/.cache/thelake/target/duckdb-download/x86_64-unknown-linux-gnu/1.5.5:${LD_LIBRARY_PATH:-}"
BIN="${HOME}/.cache/thelake/target/release/thelake"
if [[ ! -x "${BIN}" ]]; then
  echo "release thelake missing at ${BIN}; building…" >&2
  make build-release
fi
SOFTPROBE_DEFAULT_TENANT_KEY="${SOFTPROBE_DEFAULT_TENANT_KEY}" \
SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY}" \
  CONFIG_FILE="${TMP_CONFIG}" \
  nohup "${BIN}" >"${LOG}" 2>&1 &
WRAPPER_PID=$!
cleanup() { kill "${WRAPPER_PID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ "${i}" -eq 60 ]]; then
    echo "thelake failed to start" >&2
    tail -100 "${LOG}" >&2 || true
    exit 1
  fi
done

# Resolve the listening thelake PID (never trust cargo wrapper).
THELAKE_PID="$(
  python3 - <<PY
import sys
sys.path.insert(0, "scripts/perf")
from process_cpu import resolve_pid
pid = resolve_pid(base_url="http://127.0.0.1:${PORT}")
print(pid or "")
PY
)"
if [[ -z "${THELAKE_PID}" ]]; then
  THELAKE_PID="${WRAPPER_PID}"
  echo "WARN: port PID discovery failed; falling back to wrapper pid=${THELAKE_PID}" >&2
fi
echo "thelake pid=${THELAKE_PID}"

echo "Provisioning scope ${SOFTPROBE_DEFAULT_TENANT_KEY}…"
# Hints must match the bench CONFIG_FILE warehouse (isolated or shared).
PROVISION_SCHEMA="${SCHEMA}"
PROVISION_DATA_PATH="${WAREHOUSE}/"
curl -sf -X POST "http://127.0.0.1:${PORT}/v1/tenants" \
  -H "Authorization: Bearer ${SOFTPROBE_ADMIN_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"${SOFTPROBE_DEFAULT_TENANT_KEY}\",\"storageHints\":{\"ducklakeMetadataSchema\":\"${PROVISION_SCHEMA}\",\"ducklakeDataPath\":\"${PROVISION_DATA_PATH}\"}}" \
  >/tmp/thelake-bench-provision.json \
  || { echo "provision failed"; cat /tmp/thelake-bench-provision.json 2>/dev/null; tail -40 "${LOG}"; exit 1; }
cat /tmp/thelake-bench-provision.json
echo

SESSION_FILE="${SEED_DIR}/SESSION_IDS.txt"
FROM_TO=()
if [[ -f "${SEED_DIR}/MANIFEST.json" ]]; then
  eval "$(python3 - <<PY
import json
from pathlib import Path
m=json.loads(Path("${SEED_DIR}/MANIFEST.json").read_text())
tr=m.get("time_range") or {}
fr=tr.get("from"); to=tr.get("to")
if fr and to:
    print(f'FROM_TO=(--from "{fr}" --to "{to}")')
else:
    print('FROM_TO=()')
PY
)"
fi

echo "Running bench_llm_load.py…"
python3 scripts/perf/bench_llm_load.py \
  --base-url "http://127.0.0.1:${PORT}" \
  --api-token "${API_TOKEN}" \
  --duration "${DURATION}" \
  --idle-secs "${IDLE_SECS}" \
  --idle-cooldown-secs "${IDLE_COOLDOWN_SECS}" \
  --thelake-pid "${THELAKE_PID}" \
  --session-qps "${SESSION_QPS}" \
  --llm-interval-ms "${LLM_INTERVAL_MS}" \
  --session-id-file "${SESSION_FILE}" \
  --report-json "${REPORT_JSON}" \
  "${FROM_TO[@]+"${FROM_TO[@]}"}"

echo "Report: ${REPORT_JSON}"
