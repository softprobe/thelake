#!/usr/bin/env bash
# Load a scrubbed one-clock Parquet seed (+ optional session_summary CSV) into a local lake.
# Invoked by: make seed-lake SEED_DIR=...
#
# Env:
#   SEED_DIR   (required) scrubbed seed root with traces/logs data.parquet
#   CONFIG_FILE  thelake YAML (default config.yaml)
#   SEED_FORCE=1 allow load into non-empty warehouse
#   DUCKDB_BIN   DuckDB CLI

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SEED_DIR="${SEED_DIR:?SEED_DIR required (e.g. \$HOME/data/thelake-seed/scrubbed/northwind)}"
CONFIG_FILE="${CONFIG_FILE:-${ROOT}/config.yaml}"
DUCKDB_BIN="${DUCKDB_BIN:-}"

if [[ -z "${DUCKDB_BIN}" ]]; then
  if [[ -x "${ROOT}/.tools/duckdb" ]]; then
    DUCKDB_BIN="${ROOT}/.tools/duckdb"
  elif command -v duckdb >/dev/null 2>&1; then
    DUCKDB_BIN="$(command -v duckdb)"
  else
    echo "duckdb CLI required (set DUCKDB_BIN)" >&2
    exit 1
  fi
fi

test -d "${SEED_DIR}" || { echo "SEED_DIR not a directory: ${SEED_DIR}" >&2; exit 1; }
test -f "${CONFIG_FILE}" || { echo "CONFIG_FILE missing: ${CONFIG_FILE}" >&2; exit 1; }

# Parse ducklake block (simple YAML keys)
metadata_path="$(python3 - <<PY
import re,sys
text=open("${CONFIG_FILE}").read()
m=re.search(r'^\s*metadata_path:\s*["\']?([^"\'\n]+)', text, re.M)
print(m.group(1).strip() if m else "")
PY
)"
data_path="$(python3 - <<PY
import re
text=open("${CONFIG_FILE}").read()
m=re.search(r'^\s*data_path:\s*["\']?([^"\'\n]+)', text, re.M)
print(m.group(1).strip() if m else "")
PY
)"
metadata_schema="$(python3 - <<PY
import re
text=open("${CONFIG_FILE}").read()
m=re.search(r'^\s*metadata_schema:\s*["\']?([^"\'\n]+)', text, re.M)
print(m.group(1).strip() if m else "softprobe")
PY
)"

# Resolve relative data_path
case "${data_path}" in
  /*) ;;
  *) data_path="${ROOT}/${data_path}" ;;
esac

if [[ -z "${metadata_path}" || -z "${data_path}" ]]; then
  echo "could not parse ducklake.metadata_path / data_path from ${CONFIG_FILE}" >&2
  exit 1
fi

if [[ -d "${data_path}" ]] && find "${data_path}" -type f 2>/dev/null | head -1 | grep -q .; then
  if [[ "${SEED_FORCE:-0}" != "1" ]]; then
    echo "warehouse not empty: ${data_path} (set SEED_FORCE=1 to override)" >&2
    exit 1
  fi
fi
mkdir -p "${data_path}"

ALIAS=seedlake
sql_escape() {
  python3 -c 'import sys; print(sys.argv[1].replace(chr(39), chr(39)+chr(39)))' "$1"
}

MP_ESC="$(sql_escape "${metadata_path}")"
DP_ESC="$(sql_escape "${data_path}")"
SCHEMA_ESC="$(sql_escape "${metadata_schema}")"

ATTACH_SQL="
INSTALL ducklake; LOAD ducklake;
INSTALL postgres; LOAD postgres;
SET unsafe_enable_version_guessing = true;
ATTACH 'ducklake:postgres:${MP_ESC}' AS ${ALIAS} (
  DATA_PATH '${DP_ESC}',
  METADATA_SCHEMA '${SCHEMA_ESC}',
  META_SCHEMA '${SCHEMA_ESC}'
);
"

load_table() {
  local table="$1"
  local pq="${SEED_DIR}/${table}/data.parquet"
  if [[ ! -f "${pq}" ]]; then
    echo "skip missing ${pq}"
    return 0
  fi
  local pq_esc force_drop
  pq_esc="$(sql_escape "${pq}")"
  force_drop=0
  if [[ "${SEED_FORCE:-0}" == "1" ]]; then
    force_drop=1
  fi
  echo "loading ${table} from ${pq} (one-clock INSERT BY NAME)"
  local sql
  sql="$(
    FORCE_DROP="${force_drop}" ALIAS="${ALIAS}" TABLE="${table}" PQ="${pq}" \
      python3 - <<'PY'
import os, sys
sys.path.insert(0, "scripts/perf")
from seed_sql import load_table_sql
print(load_table_sql(
    alias=os.environ["ALIAS"],
    table=os.environ["TABLE"],
    parquet_path=os.environ["PQ"],
    force_drop=os.environ.get("FORCE_DROP") == "1",
))
PY
  )"
  local out
  out="$("${DUCKDB_BIN}" -c "${ATTACH_SQL}${sql}")"
  echo "${out}"
  local rows
  rows="$(echo "${out}" | awk '/[0-9]+/ {n=$1} END {print n+0}')"
  local pq_rows
  pq_rows="$("${DUCKDB_BIN}" -c "SELECT count(*) FROM read_parquet('${pq_esc}');" | awk '/[0-9]+/ {n=$1} END {print n+0}')"
  if [[ "${pq_rows}" -gt 0 && "${rows}" -eq 0 ]]; then
    echo "seed load failed: ${table} parquet has ${pq_rows} rows but table count is 0" >&2
    return 1
  fi
  echo "${table}: lake_rows=${rows} parquet_rows=${pq_rows}"
}

for t in traces logs; do
  load_table "$t"
done

SUMMARY_CSV="${SEED_DIR}/session_summary/session_summary.csv"
if [[ -f "${SUMMARY_CSV}" ]]; then
  echo "loading session_summary from ${SUMMARY_CSV}"
  python3 - <<PY
import os, re, subprocess, csv, shutil
from pathlib import Path
mp = """${metadata_path}"""
kv = dict(re.findall(r"(\w+)=([^\s]+)", mp))
schema = """${metadata_schema}"""
csv_path = Path("""${SUMMARY_CSV}""")
cols = next(csv.reader(csv_path.open()))
has_tenant = "tenant_id" in cols
if has_tenant:
    ddl = f'''
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ,
  observation_count BIGINT NOT NULL DEFAULT 0,
  error_count BIGINT NOT NULL DEFAULT 0,
  input_tokens BIGINT,
  output_tokens BIGINT,
  total_tokens BIGINT,
  total_cost DOUBLE PRECISION,
  agent_name TEXT,
  user_id TEXT,
  model_name TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, session_id)
);
'''
else:
    ddl = f'''
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  session_id TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ,
  observation_count BIGINT NOT NULL DEFAULT 0,
  error_count BIGINT NOT NULL DEFAULT 0,
  input_tokens BIGINT,
  output_tokens BIGINT,
  total_tokens BIGINT,
  total_cost DOUBLE PRECISION,
  agent_name TEXT,
  user_id TEXT,
  model_name TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);
'''

def run_psql(sql: str) -> None:
    if shutil.which("psql"):
        env = os.environ.copy()
        env["PGPASSWORD"] = kv.get("password", "")
        subprocess.run(
            ["psql", "-h", kv.get("host", "localhost"), "-p", kv.get("port", "5432"),
             "-U", kv.get("user", "ducklake"), "-d", kv.get("dbname", "ducklake"),
             "-v", "ON_ERROR_STOP=1", "-c", sql],
            env=env, check=True,
        )
        return
    # Local compose: docker exec (CSV must be piped; copy file into container first)
    subprocess.run(
        ["docker", "exec", "-i", "ducklake-postgres",
         "psql", "-U", kv.get("user", "ducklake"), "-d", kv.get("dbname", "ducklake"),
         "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
    )

run_psql(ddl)
run_psql(f"TRUNCATE {schema}.session_summary;")
# Remap prod tenant_id → bench tenant so /v1/llm/sessions/search hits seeded rows.
remap_tenant = os.environ.get("SEED_WORKSPACE_ID") or os.environ.get("SOFTPROBE_DEFAULT_WORKSPACE_ID") or ""
load_csv = csv_path
tmp_csv = None
if remap_tenant and "tenant_id" in cols:
    import tempfile
    tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
    tmp_csv = Path(tmp.name)
    with csv_path.open() as src, tmp:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(tmp, fieldnames=cols)
        writer.writeheader()
        n = 0
        for row in reader:
            row["tenant_id"] = remap_tenant
            writer.writerow(row)
            n += 1
    load_csv = tmp_csv
    print(f"session_summary tenant_id remapped → {remap_tenant} ({n} rows)")

# COPY via stdin
copy_sql = f"COPY {schema}.session_summary ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, HEADER true);"
data = load_csv.read_bytes()
try:
    if shutil.which("psql"):
        env = os.environ.copy()
        env["PGPASSWORD"] = kv.get("password", "")
        subprocess.run(
            ["psql", "-h", kv.get("host", "localhost"), "-p", kv.get("port", "5432"),
             "-U", kv.get("user", "ducklake"), "-d", kv.get("dbname", "ducklake"),
             "-v", "ON_ERROR_STOP=1", "-c",
             f"\\\\copy {schema}.session_summary ({', '.join(cols)}) FROM '{load_csv}' WITH (FORMAT csv, HEADER true);"],
            env=env, check=True,
        )
    else:
        proc = subprocess.run(
            ["docker", "exec", "-i", "ducklake-postgres",
             "psql", "-U", kv.get("user", "ducklake"), "-d", kv.get("dbname", "ducklake"),
             "-v", "ON_ERROR_STOP=1", "-c", copy_sql],
            input=data, check=True,
        )
    print("session_summary loaded")
finally:
    if tmp_csv is not None:
        tmp_csv.unlink(missing_ok=True)
PY
fi

# Ensure SESSION_IDS for the bench
if [[ -f "${SEED_DIR}/SESSION_IDS.txt" ]]; then
  echo "session ids: $(wc -l < "${SEED_DIR}/SESSION_IDS.txt")"
elif [[ -f "${SEED_DIR}/MANIFEST.json" ]]; then
  python3 - <<PY
import json
from pathlib import Path
m=json.loads(Path("${SEED_DIR}/MANIFEST.json").read_text())
ids=m.get("session_id_sample") or []
Path("${SEED_DIR}/SESSION_IDS.txt").write_text("\n".join(ids)+"\n")
print(f"wrote SESSION_IDS.txt ({len(ids)}) from MANIFEST")
PY
fi

echo "seed-lake complete (SEED_DIR=${SEED_DIR})"
