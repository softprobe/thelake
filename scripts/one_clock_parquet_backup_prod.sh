#!/usr/bin/env bash
# Dump one or more production DuckLake catalogs to one-clock Parquet.
#
# Requires:
#   - kubectl access to production + GCS HMAC (from softprobe-gcs-hmac)
#   - DuckDB ≥ 1.5.5 (thelake/.tools/duckdb or DUCKDB_BIN)
#   - Local port-forward to in-cluster Postgres (started by this script)
#
# Usage:
#   ./scripts/one_clock_parquet_backup_prod.sh              # all known workspaces
#   ./scripts/one_clock_parquet_backup_prod.sh sp-llm ws-myworkspace-mtyxusmz-2t77yn
#
# Output: ~/src/arex/data/workspaces/<name>/{table}/data.parquet + MANIFEST.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
THELAKE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${OUT_DIR:-${HOME}/src/arex/data/workspaces}"
PF_LOCAL_PORT="${PF_LOCAL_PORT:-15432}"
NAMESPACE="${NAMESPACE:-production}"

# workspace_dir|postgres_schema|gcs_data_path
ALL_WORKSPACES=(
  "sp-llm|sp_llm|gs://softprobe-datalake-ducklake/sp-llm-ducklake/"
  "ws-e2e-alpha-1789239311-mtyqw0vv-fwix0g|ws_e2e_alpha_1789239311_mtyqw0vv_fwix0g|gs://softprobe-datalake-ducklake/workspaces/ws-e2e-alpha-1789239311-mtyqw0vv-fwix0g/"
  "ws-e2e-beta-1789239500-mtyr02fk-f6qzk5|ws_e2e_beta_1789239500_mtyr02fk_f6qzk5|gs://softprobe-datalake-ducklake/workspaces/ws-e2e-beta-1789239500-mtyr02fk-f6qzk5/"
  "ws-lake-assert-20260912094647-mty7arjv|ws_lake_assert_20260912094647_mty7arjv|gs://softprobe-datalake-ducklake/workspaces/ws-lake-assert-20260912094647-mty7arjv/"
  "ws-lake-v015-20260912100022-mty7s9ro|ws_lake_v015_20260912100022_mty7s9ro|gs://softprobe-datalake-ducklake/workspaces/ws-lake-v015-20260912100022-mty7s9ro/"
  "ws-myworkspace-mtyxusmz-2t77yn|ws_myworkspace_mtyxusmz_2t77yn|gs://softprobe-datalake-ducklake/workspaces/ws-myworkspace-mtyxusmz-2t77yn/"
  "ws-onur-softprobe-test-2-mu1x16zf-0f5rj8|ws_onur_softprobe_test_2_mu1x16zf_0f5rj8|gs://softprobe-datalake-ducklake/workspaces/ws-onur-softprobe-test-2-mu1x16zf-0f5rj8/"
  "ws-softprobe-test-onur-mu09h8ck-nwxkdf|ws_softprobe_test_onur_mu09h8ck_nwxkdf|gs://softprobe-datalake-ducklake/workspaces/ws-softprobe-test-onur-mu09h8ck-nwxkdf/"
  "ws-workspace2-mtzetpds-t8bcf3|ws_workspace2_mtzetpds_t8bcf3|gs://softprobe-datalake-ducklake/workspaces/ws-workspace2-mtzetpds-t8bcf3/"
)

want=("$@")
filter_wanted() {
  local ws="$1"
  if [[ ${#want[@]} -eq 0 ]]; then
    return 0
  fi
  local w
  for w in "${want[@]}"; do
    if [[ "$w" == "$ws" ]]; then
      return 0
    fi
  done
  return 1
}

PGPASS="$(kubectl -n "$NAMESPACE" get secret softprobe-agent-secrets -o jsonpath='{.data.postgres-password}' | base64 -d)"
export GCS_HMAC_ACCESS_KEY_ID
export GCS_HMAC_SECRET
GCS_HMAC_ACCESS_KEY_ID="$(kubectl -n "$NAMESPACE" get secret softprobe-gcs-hmac -o jsonpath='{.data.GCS_HMAC_ACCESS_KEY_ID}' | base64 -d)"
GCS_HMAC_SECRET="$(kubectl -n "$NAMESPACE" get secret softprobe-gcs-hmac -o jsonpath='{.data.GCS_HMAC_SECRET}' | base64 -d)"

export DUCKLAKE_METADATA_PATH="host=127.0.0.1 port=${PF_LOCAL_PORT} dbname=softprobe user=softprobe password=${PGPASS} sslmode=disable"

echo "Starting port-forward to postgres-softprobe-agent on localhost:${PF_LOCAL_PORT}…"
kubectl -n "$NAMESPACE" port-forward svc/postgres-softprobe-agent "${PF_LOCAL_PORT}:5432" >/tmp/thelake-pg-pf.log 2>&1 &
PF_PID=$!
cleanup() {
  kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port
for _ in $(seq 1 30); do
  if (echo >/dev/tcp/127.0.0.1/"${PF_LOCAL_PORT}") 2>/dev/null; then
    break
  fi
  sleep 0.5
done

mkdir -p "$OUT_DIR"
failed=0
for entry in "${ALL_WORKSPACES[@]}"; do
  IFS='|' read -r ws schema data_path <<<"$entry"
  if ! filter_wanted "$ws"; then
    continue
  fi
  echo "=== dumping ${ws} (${schema}) ==="
  if ! python3 "${SCRIPT_DIR}/one_clock_parquet_backup.py" \
    --mode ducklake \
    --workspace "$ws" \
    --schema "$schema" \
    --data-path "$data_path" \
    --out-dir "$OUT_DIR"; then
    echo "FAILED: $ws" >&2
    failed=1
  fi
done

exit "$failed"
