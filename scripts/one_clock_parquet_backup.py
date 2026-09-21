#!/usr/bin/env python3
"""Dump a DuckLake (or fixture) catalog to one-clock Parquet under a workspace dir.

Transforms match scripts/one_clock_catalog_copy.sql (drop record_date; window_ts →
timestamp). Output layout:

  {out_dir}/{workspace_name}/{table}/data.parquet
  {out_dir}/{workspace_name}/MANIFEST.json

Design: docs/design-sql-and-schema.md §1.5.

Examples:
  # Synthetic fixture (CI / local unit):
  python3 scripts/one_clock_parquet_backup.py --mode fixture \\
    --workspace demo --out-dir /tmp/ws-backup

  # Production DuckLake (port-forward PG + GCS HMAC; DuckDB ≥ 1.5.5):
  export DUCKLAKE_METADATA_PATH='host=127.0.0.1 port=15432 dbname=softprobe user=softprobe password=… sslmode=disable'
  export GCS_HMAC_ACCESS_KEY_ID=… GCS_HMAC_SECRET=…
  python3 scripts/one_clock_parquet_backup.py --mode ducklake \\
    --schema ws_myworkspace_mtyxusmz_2t77yn \\
    --data-path 'gs://softprobe-datalake-ducklake/workspaces/ws-myworkspace-mtyxusmz-2t77yn/' \\
    --out-dir ~/src/arex/data/workspaces
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

# Table → SELECT body with {src} = qualified catalog prefix (alias or alias.schema).
# Keep in lockstep with scripts/one_clock_catalog_copy.sql.
EXPORT_SELECTS: dict[str, str] = {
    "traces": "SELECT * EXCLUDE (record_date) FROM {src}.traces",
    "logs": "SELECT * EXCLUDE (record_date) FROM {src}.logs",
    "scores": "SELECT * EXCLUDE (record_date) FROM {src}.scores",
}

FORBIDDEN_COLUMNS = frozenset({"record_date", "event_date", "window_ts"})

ATTACH_ALIAS = "old"


@dataclass(frozen=True)
class TableResult:
    table: str
    status: str  # exported | skipped_missing | error
    rows: int | None = None
    path: str | None = None
    error: str | None = None


def schema_to_workspace_name(schema: str) -> str:
    """Postgres metadata_schema → local workspace directory name."""
    if schema == "sp_llm":
        return "sp-llm"
    return schema.replace("_", "-")


def find_duckdb(explicit: str | None) -> str:
    if explicit:
        return explicit
    env = os.environ.get("DUCKDB_BIN")
    if env:
        return env
    here = Path(__file__).resolve().parent.parent / ".tools" / "duckdb"
    if here.is_file() and os.access(here, os.X_OK):
        return str(here)
    which = shutil.which("duckdb")
    if which:
        return which
    raise SystemExit("duckdb CLI not found (set DUCKDB_BIN or install DuckDB ≥ 1.5.5)")


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def run_duckdb(duckdb: str, sql: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [duckdb, "-c", sql],
        text=True,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"duckdb failed ({proc.returncode}):\n{proc.stderr or proc.stdout}"
        )
    return proc


def fixture_bootstrap_sql() -> str:
    """Minimal old-schema tables covering every export recipe."""
    a = ATTACH_ALIAS
    return f"""
CREATE SCHEMA {a};

CREATE TABLE {a}.traces (
  session_id VARCHAR, trace_id VARCHAR, span_id VARCHAR,
  timestamp TIMESTAMP_NS, record_date DATE, attributes MAP(VARCHAR, VARCHAR)
);
INSERT INTO {a}.traces VALUES
  ('ses_a', 'tr1', 'sp1', '2026-09-20 08:00:00'::TIMESTAMP_NS, DATE '2026-09-20', MAP {{'k': 'v'}}),
  ('ses_a', 'tr1', 'sp2', '2026-09-20 08:01:00'::TIMESTAMP_NS, DATE '2026-09-20', NULL);

CREATE TABLE {a}.logs (
  session_id VARCHAR, timestamp TIMESTAMP_NS, body VARCHAR, record_date DATE
);
INSERT INTO {a}.logs VALUES
  ('ses_a', '2026-09-20 08:00:00'::TIMESTAMP_NS, 'hello', DATE '2026-09-20');

CREATE TABLE {a}.scores (
  score_id VARCHAR, timestamp TIMESTAMPTZ, name VARCHAR, data_type VARCHAR,
  source VARCHAR, record_date DATE
);
INSERT INTO {a}.scores VALUES
  ('sc1', TIMESTAMPTZ '2026-09-20 08:00:00+00', 'quality', 'numeric', 'api', DATE '2026-09-20');

"""


def ducklake_attach_sql(
    *,
    metadata_path: str,
    data_path: str,
    metadata_schema: str,
    gcs_key: str,
    gcs_secret: str,
) -> str:
    if metadata_path.startswith("postgres:"):
        attach_target = metadata_path
    else:
        attach_target = f"postgres:{metadata_path}"
    parts = [
        "INSTALL httpfs; LOAD httpfs;",
        "INSTALL ducklake; LOAD ducklake;",
        "INSTALL postgres; LOAD postgres;",
        "SET unsafe_enable_version_guessing = true;",
    ]
    if data_path.startswith("gs://"):
        parts.append(
            "CREATE OR REPLACE SECRET gcs_hmac ("
            f"TYPE GCS, KEY_ID '{sql_escape(gcs_key)}', SECRET '{sql_escape(gcs_secret)}');"
        )
    parts.append(
        f"ATTACH 'ducklake:{sql_escape(attach_target)}' AS {ATTACH_ALIAS} ("
        f"DATA_PATH '{sql_escape(data_path)}', "
        f"METADATA_SCHEMA '{sql_escape(metadata_schema)}', "
        f"META_SCHEMA '{sql_escape(metadata_schema)}'"
        ");"
    )
    return "\n".join(parts)


def list_existing_tables(
    duckdb: str, preamble: str, *, table_schema: str
) -> set[str]:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        out = tmp.name
    try:
        run_duckdb(
            duckdb,
            preamble
            + f"""
COPY (
  SELECT table_name FROM information_schema.tables
  WHERE table_schema = '{sql_escape(table_schema)}'
) TO '{sql_escape(out)}' (FORMAT CSV, HEADER false);
""",
        )
        names: set[str] = set()
        for line in Path(out).read_text().splitlines():
            name = line.strip().strip('"')
            if name:
                names.add(name)
        return names
    finally:
        Path(out).unlink(missing_ok=True)


def parquet_columns(duckdb: str, path: Path) -> list[str]:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        out = tmp.name
    try:
        run_duckdb(
            duckdb,
            f"COPY (SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{sql_escape(str(path))}'))) "
            f"TO '{sql_escape(out)}' (FORMAT CSV, HEADER false);",
        )
        return [
            ln.strip().strip('"')
            for ln in Path(out).read_text().splitlines()
            if ln.strip()
        ]
    finally:
        Path(out).unlink(missing_ok=True)


def parquet_row_count(duckdb: str, path: Path) -> int:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        out = tmp.name
    try:
        run_duckdb(
            duckdb,
            f"COPY (SELECT count(*)::BIGINT FROM read_parquet('{sql_escape(str(path))}')) "
            f"TO '{sql_escape(out)}' (FORMAT CSV, HEADER false);",
        )
        return int(Path(out).read_text().strip().splitlines()[-1])
    finally:
        Path(out).unlink(missing_ok=True)


def export_table(
    duckdb: str,
    preamble: str,
    table: str,
    select_sql: str,
    out_file: Path,
) -> TableResult:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_file.exists():
        out_file.unlink()
    export_sql = (
        preamble
        + "\n"
        + f"COPY ({select_sql}) TO '{sql_escape(str(out_file))}' (FORMAT PARQUET, COMPRESSION ZSTD);"
    )
    try:
        run_duckdb(duckdb, export_sql)
    except RuntimeError as err:
        msg = str(err)
        if "does not exist" in msg or "Catalog Error" in msg or "Table with name" in msg:
            return TableResult(table, "skipped_missing", error=msg.splitlines()[-1][:200])
        return TableResult(table, "error", error=msg[:500])

    cols = parquet_columns(duckdb, out_file)
    bad = FORBIDDEN_COLUMNS.intersection(cols)
    if bad:
        out_file.unlink(missing_ok=True)
        return TableResult(
            table,
            "error",
            error=f"forbidden columns survived into parquet: {sorted(bad)}",
        )
    rows = parquet_row_count(duckdb, out_file)
    return TableResult(table, "exported", rows=rows, path=str(out_file))


def export_workspace(
    *,
    duckdb: str,
    preamble: str,
    workspace: str,
    out_root: Path,
    src_prefix: str,
    table_schema: str,
    tables: Iterable[str] | None = None,
) -> dict:
    ws_dir = out_root / workspace
    ws_dir.mkdir(parents=True, exist_ok=True)
    existing = list_existing_tables(duckdb, preamble, table_schema=table_schema)
    selected = list(tables) if tables else list(EXPORT_SELECTS.keys())
    results: list[TableResult] = []
    for table in selected:
        if table not in EXPORT_SELECTS:
            results.append(TableResult(table, "error", error="unknown table recipe"))
            continue
        if table not in existing:
            results.append(TableResult(table, "skipped_missing"))
            continue
        select_sql = EXPORT_SELECTS[table].format(src=src_prefix)
        out_file = ws_dir / table / "data.parquet"
        results.append(export_table(duckdb, preamble, table, select_sql, out_file))

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        ver_out = tmp.name
    try:
        run_duckdb(
            duckdb,
            f"COPY (SELECT version()) TO '{sql_escape(ver_out)}' (FORMAT CSV, HEADER false);",
        )
        duckdb_version = Path(ver_out).read_text().strip().strip('"')
    finally:
        Path(ver_out).unlink(missing_ok=True)

    errors = [r for r in results if r.status == "error"]
    exported = [r for r in results if r.status == "exported"]
    manifest = {
        "workspace": workspace,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "duckdb": duckdb_version,
        "src_prefix": src_prefix,
        "table_schema": table_schema,
        "tables": [r.__dict__ for r in results],
        "ok": not errors and bool(exported),
    }
    (ws_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fixture", "ducklake"), required=True)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path.home() / "src/arex/data/workspaces",
        help="Parent directory for workspace folders",
    )
    parser.add_argument("--workspace", help="Output workspace folder name")
    parser.add_argument("--schema", help="DuckLake METADATA_SCHEMA (ducklake mode)")
    parser.add_argument("--data-path", help="DuckLake DATA_PATH gs://… (ducklake mode)")
    parser.add_argument("--duckdb", help="Path to duckdb CLI")
    parser.add_argument(
        "--tables",
        help="Comma-separated table subset (default: all recipes)",
    )
    args = parser.parse_args(argv)

    duckdb = find_duckdb(args.duckdb)
    tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "fixture":
        workspace = args.workspace or "fixture-demo"
        manifest = export_workspace(
            duckdb=duckdb,
            preamble=fixture_bootstrap_sql(),
            workspace=workspace,
            out_root=args.out_dir,
            src_prefix=ATTACH_ALIAS,
            table_schema=ATTACH_ALIAS,
            tables=tables,
        )
    else:
        schema = args.schema
        data_path = args.data_path
        if not schema or not data_path:
            raise SystemExit("ducklake mode requires --schema and --data-path")
        workspace = args.workspace or schema_to_workspace_name(schema)
        meta = os.environ.get("DUCKLAKE_METADATA_PATH", "")
        if not meta:
            raise SystemExit("Set DUCKLAKE_METADATA_PATH (Postgres options for DuckLake)")
        gcs_key = os.environ.get("GCS_HMAC_ACCESS_KEY_ID") or os.environ.get(
            "GCP_HMAC_ACCESS_KEY_ID", ""
        )
        gcs_secret = os.environ.get("GCS_HMAC_SECRET") or os.environ.get(
            "GCP_HMAC_SECRET", ""
        )
        if data_path.startswith("gs://") and (not gcs_key or not gcs_secret):
            raise SystemExit("gs:// data path requires GCS_HMAC_ACCESS_KEY_ID + GCS_HMAC_SECRET")
        preamble = ducklake_attach_sql(
            metadata_path=meta,
            data_path=data_path,
            metadata_schema=schema,
            gcs_key=gcs_key,
            gcs_secret=gcs_secret,
        )
        # DuckLake exposes facts as <attach_alias>.<metadata_schema>.<table>
        src_prefix = f"{ATTACH_ALIAS}.{schema}"
        manifest = export_workspace(
            duckdb=duckdb,
            preamble=preamble,
            workspace=workspace,
            out_root=args.out_dir,
            src_prefix=src_prefix,
            table_schema=schema,
            tables=tables,
        )

    print(json.dumps(manifest, indent=2))
    return 0 if manifest.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
