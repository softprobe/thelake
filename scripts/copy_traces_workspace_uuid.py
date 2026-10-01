#!/usr/bin/env python3
"""One-shot ops: copy traces from an old DuckLake into a new shared physical scope.

Rewrites the tenancy column from old `tenant_id` / lake_scope_id slug →
`workspace_id` (UUID). Does **not** copy logs, scores, or session_summary.

Prerequisites
-------------
- Target `traces` table already exists with a `workspace_id` column (greenfield
  CREATE from the workspace-identity cutover). This script never creates tables.
- Writers on both lakes should be stopped for a consistent cutover.
- Python package `duckdb` with ducklake + postgres (+ httpfs when using gs:// or
  s3://) extensions available.

Mapping file
------------
JSON object::

    {"ws-old-slug-…": "550e8400-e29b-41d4-a716-446655440000"}

JSON array of objects (``tenant_id`` or ``lake_scope_id`` → ``workspace_id``)::

    [{"tenant_id": "ws-old-slug-…", "workspace_id": "550e8400-…"}]

CSV with header ``tenant_id,workspace_id`` or ``lake_scope_id,workspace_id``.

Connection (environment)
------------------------
Old catalog (source)::

    OLD_DUCKLAKE_METADATA_PATH   Postgres DSN options (with or without postgres:)
    OLD_DUCKLAKE_DATA_PATH       Warehouse prefix (local path, gs://, or s3://)
    OLD_DUCKLAKE_METADATA_SCHEMA Metadata schema (default: softprobe)
    OLD_DUCKLAKE_CATALOG_ALIAS   ATTACH alias (default: old)

New shared physical scope (target)::

    NEW_DUCKLAKE_METADATA_PATH
    NEW_DUCKLAKE_DATA_PATH
    NEW_DUCKLAKE_METADATA_SCHEMA (default: thelake)
    NEW_DUCKLAKE_CATALOG_ALIAS   (default: softprobe)

Object store (when either data_path is gs:// or s3://)::

    GCS_HMAC_ACCESS_KEY_ID / GCS_HMAC_SECRET  (or GCP_HMAC_*)
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]
    AWS_S3_ENDPOINT / AWS_REGION               (optional for path-style MinIO)

Example
-------
::

    export OLD_DUCKLAKE_METADATA_PATH='host=… dbname=postgres user=… password=… sslmode=require'
    export OLD_DUCKLAKE_DATA_PATH='gs://bucket/old-ducklake/'
    export OLD_DUCKLAKE_METADATA_SCHEMA=softprobe
    export NEW_DUCKLAKE_METADATA_PATH='host=… dbname=postgres user=… password=… sslmode=require'
    export NEW_DUCKLAKE_DATA_PATH='gs://bucket/shared/'
    export NEW_DUCKLAKE_METADATA_SCHEMA=thelake
    export GCS_HMAC_ACCESS_KEY_ID=…
    export GCS_HMAC_SECRET=…

    python3 scripts/copy_traces_workspace_uuid.py --mapping workspace_map.json
    python3 scripts/copy_traces_workspace_uuid.py --mapping workspace_map.csv --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Iterable

SOURCE_TENANCY_COLUMNS = ("tenant_id", "lake_scope_id")
TARGET_TENANCY_COLUMN = "workspace_id"
# Explicitly out of scope for this cutover (never referenced by copy SQL).
EXCLUDED_SIGNAL_TABLES = ("logs", "scores", "session_summary")

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def quote_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def normalize_postgres_dsn(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        raise ValueError("Postgres metadata path is empty")
    return raw if raw.startswith("postgres:") else f"postgres:{raw}"


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def env_or(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value if value else default


def env_first(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def validate_workspace_uuid(value: str) -> str:
    text = value.strip()
    if not UUID_RE.match(text):
        raise ValueError(f"workspace_id is not a UUID: {value!r}")
    # Normalize to canonical lowercase form.
    return str(uuid.UUID(text))


def load_mapping(path: Path) -> dict[str, str]:
    """Return old slug → workspace UUID. Rejects empty / duplicate / bad UUID."""
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".csv":
        mapping = _load_mapping_csv(text)
    else:
        mapping = _load_mapping_json(text)
    if not mapping:
        raise ValueError(f"mapping file is empty: {path}")
    return mapping


def _load_mapping_json(text: str) -> dict[str, str]:
    payload = json.loads(text)
    out: dict[str, str] = {}
    if isinstance(payload, dict):
        for slug, workspace_id in payload.items():
            _put_mapping(out, str(slug), str(workspace_id))
        return out
    if isinstance(payload, list):
        for i, row in enumerate(payload):
            if not isinstance(row, dict):
                raise ValueError(f"mapping[{i}] must be an object")
            slug = row.get("tenant_id") or row.get("lake_scope_id")
            workspace_id = row.get("workspace_id")
            if not slug or not workspace_id:
                raise ValueError(
                    f"mapping[{i}] needs tenant_id|lake_scope_id and workspace_id"
                )
            _put_mapping(out, str(slug), str(workspace_id))
        return out
    raise ValueError("mapping JSON must be an object or an array of objects")


def _load_mapping_csv(text: str) -> dict[str, str]:
    reader = csv.DictReader(text.splitlines())
    if reader.fieldnames is None:
        raise ValueError("CSV mapping has no header")
    fields = {name.strip().lower(): name for name in reader.fieldnames if name}
    slug_key = fields.get("tenant_id") or fields.get("lake_scope_id")
    uuid_key = fields.get("workspace_id")
    if slug_key is None or uuid_key is None:
        raise ValueError(
            "CSV mapping requires tenant_id|lake_scope_id and workspace_id columns"
        )
    out: dict[str, str] = {}
    for i, row in enumerate(reader, start=2):
        slug = (row.get(slug_key) or "").strip()
        workspace_id = (row.get(uuid_key) or "").strip()
        if not slug and not workspace_id:
            continue
        if not slug or not workspace_id:
            raise ValueError(f"CSV line {i}: incomplete mapping row")
        _put_mapping(out, slug, workspace_id)
    return out


def _put_mapping(out: dict[str, str], slug: str, workspace_id: str) -> None:
    slug = slug.strip()
    if not slug:
        raise ValueError("mapping slug is empty")
    workspace_id = validate_workspace_uuid(workspace_id)
    if slug in out and out[slug] != workspace_id:
        raise ValueError(f"duplicate slug with conflicting UUID: {slug}")
    out[slug] = workspace_id


def describe_column_names(conn, qualified_table: str) -> list[str]:
    rows = conn.execute(f"DESCRIBE SELECT * FROM {qualified_table}").fetchall()
    return [row[0] for row in rows]


def source_tenancy_column(source_columns: Iterable[str]) -> str:
    lower = {name.lower(): name for name in source_columns}
    for candidate in SOURCE_TENANCY_COLUMNS:
        if candidate in lower:
            return lower[candidate]
    raise ValueError(
        "source traces table has neither tenant_id nor lake_scope_id; "
        f"columns={list(source_columns)}"
    )


def build_insert_select_sql(
    *,
    source_table: str,
    target_table: str,
    source_columns: list[str],
    target_columns: list[str],
    source_tenancy: str,
    workspace_id: str,
    old_slug: str,
) -> str:
    """Build INSERT…SELECT that emits workspace_id and drops the old slug column.

    Target columns absent from the source are omitted (DuckDB fills NULL). Source
    promotions not present on the greenfield target are dropped.
    """
    if TARGET_TENANCY_COLUMN not in {c.lower() for c in target_columns}:
        raise ValueError(
            f"target traces table is missing {TARGET_TENANCY_COLUMN}; "
            "bootstrap the greenfield schema before copying"
        )
    source_by_lower = {c.lower(): c for c in source_columns}
    insert_cols: list[str] = []
    projections: list[str] = []
    for target_col in target_columns:
        lower = target_col.lower()
        if lower == TARGET_TENANCY_COLUMN:
            insert_cols.append(target_col)
            projections.append(
                f"{quote_literal(workspace_id)} AS {quote_ident(target_col)}"
            )
            continue
        if lower == source_tenancy.lower():
            # Never copy the old slug column into the target.
            continue
        source_col = source_by_lower.get(lower)
        if source_col is None:
            continue
        insert_cols.append(target_col)
        projections.append(f"{quote_ident(source_col)} AS {quote_ident(target_col)}")

    if not insert_cols:
        raise ValueError("no overlapping columns between source and target traces")

    target_list = ", ".join(quote_ident(c) for c in insert_cols)
    select_list = ", ".join(projections)
    return (
        f"INSERT INTO {target_table} ({target_list}) "
        f"SELECT {select_list} FROM {source_table} "
        f"WHERE {quote_ident(source_tenancy)} = {quote_literal(old_slug)}"
    )


def object_store_init_sql(data_paths: list[str]) -> list[str]:
    """Credential / endpoint SET statements for gs:// and s3:// warehouses."""
    statements: list[str] = []
    needs_gcs = any(p.startswith("gs://") for p in data_paths)
    needs_s3 = any(p.startswith("s3://") for p in data_paths)

    if needs_gcs:
        key_id = env_first("GCS_HMAC_ACCESS_KEY_ID", "GCP_HMAC_ACCESS_KEY_ID")
        secret = env_first("GCS_HMAC_SECRET", "GCP_HMAC_SECRET")
        if not key_id or not secret:
            raise SystemExit(
                "gs:// data_path requires GCS_HMAC_ACCESS_KEY_ID and GCS_HMAC_SECRET"
            )
        statements.append(
            "CREATE OR REPLACE SECRET gcs_hmac ("
            f"TYPE GCS, KEY_ID {quote_literal(key_id)}, "
            f"SECRET {quote_literal(secret)}"
            ");"
        )

    if needs_s3:
        endpoint = env_first("AWS_S3_ENDPOINT", "AWS_ENDPOINT_URL")
        region = env_or("AWS_REGION", "us-east-1")
        if endpoint:
            host = endpoint.removeprefix("http://").removeprefix("https://")
            use_ssl = "false" if endpoint.startswith("http://") else "true"
            statements += [
                f"SET s3_endpoint = {quote_literal(host)};",
                "SET s3_url_style = 'path';",
                f"SET s3_use_ssl = {use_ssl};",
            ]
        access_key = env_first("AWS_ACCESS_KEY_ID")
        secret_key = env_first("AWS_SECRET_ACCESS_KEY")
        session_token = env_first("AWS_SESSION_TOKEN")
        if not access_key or not secret_key:
            raise SystemExit(
                "s3:// data_path requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            )
        statements.append(f"SET s3_access_key_id = {quote_literal(access_key)};")
        statements.append(f"SET s3_secret_access_key = {quote_literal(secret_key)};")
        if session_token:
            statements.append(
                f"SET s3_session_token = {quote_literal(session_token)};"
            )
        statements.append(f"SET s3_region = {quote_literal(region)};")

    return statements


def attach_sql(
    *,
    alias: str,
    metadata_path: str,
    data_path: str,
    metadata_schema: str,
) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
        raise ValueError(f"catalog alias must be a simple identifier: {alias!r}")
    dsn = normalize_postgres_dsn(metadata_path)
    opts = [
        f"DATA_PATH {quote_literal(data_path)}",
        "CREATE_IF_NOT_EXISTS false",
        f"METADATA_SCHEMA {quote_literal(metadata_schema)}",
        f"META_SCHEMA {quote_literal(metadata_schema)}",
    ]
    return (
        f"ATTACH 'ducklake:{dsn.replace(chr(39), chr(39) * 2)}' AS {alias} "
        f"({', '.join(opts)});"
    )


def qualified_traces(alias: str, schema: str) -> str:
    return f"{quote_ident(alias)}.{quote_ident(schema)}.traces"


def open_copy_connection(
    *,
    old_meta: str,
    old_data: str,
    old_schema: str,
    old_alias: str,
    new_meta: str,
    new_data: str,
    new_schema: str,
    new_alias: str,
):
    import duckdb  # lazy: unit tests cover pure helpers without the extension

    conn = duckdb.connect()
    for ext in ("httpfs", "ducklake", "postgres"):
        conn.execute(f"INSTALL {ext};")
        conn.execute(f"LOAD {ext};")
    for stmt in object_store_init_sql([old_data, new_data]):
        conn.execute(stmt)
    conn.execute(
        attach_sql(
            alias=old_alias,
            metadata_path=old_meta,
            data_path=old_data,
            metadata_schema=old_schema,
        )
    )
    conn.execute(
        attach_sql(
            alias=new_alias,
            metadata_path=new_meta,
            data_path=new_data,
            metadata_schema=new_schema,
        )
    )
    return conn


def copy_one_workspace(
    conn,
    *,
    source_table: str,
    target_table: str,
    source_columns: list[str],
    target_columns: list[str],
    source_tenancy: str,
    old_slug: str,
    workspace_id: str,
    dry_run: bool,
) -> dict:
    count_sql = (
        f"SELECT count(*) FROM {source_table} "
        f"WHERE {quote_ident(source_tenancy)} = {quote_literal(old_slug)}"
    )
    source_count = int(conn.execute(count_sql).fetchone()[0])
    result = {
        "tenant_id": old_slug,
        "workspace_id": workspace_id,
        "source_rows": source_count,
        "inserted_rows": 0,
        "dry_run": dry_run,
    }
    if source_count == 0 or dry_run:
        return result

    before = int(
        conn.execute(
            f"SELECT count(*) FROM {target_table} "
            f"WHERE {quote_ident(TARGET_TENANCY_COLUMN)} = "
            f"{quote_literal(workspace_id)}"
        ).fetchone()[0]
    )
    if before != 0:
        raise RuntimeError(
            f"target already has {before} rows for workspace_id={workspace_id}; "
            "refuse merge into a non-empty workspace"
        )

    insert_sql = build_insert_select_sql(
        source_table=source_table,
        target_table=target_table,
        source_columns=source_columns,
        target_columns=target_columns,
        source_tenancy=source_tenancy,
        workspace_id=workspace_id,
        old_slug=old_slug,
    )
    conn.execute("BEGIN TRANSACTION;")
    try:
        conn.execute(insert_sql)
        after = int(
            conn.execute(
                f"SELECT count(*) FROM {target_table} "
                f"WHERE {quote_ident(TARGET_TENANCY_COLUMN)} = "
                f"{quote_literal(workspace_id)}"
            ).fetchone()[0]
        )
        if after != source_count:
            raise RuntimeError(
                f"row-count mismatch for workspace_id={workspace_id}: "
                f"source={source_count} target={after}"
            )
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    result["inserted_rows"] = source_count
    return result


def run(mapping: dict[str, str], dry_run: bool) -> list[dict]:
    old_meta = env_required("OLD_DUCKLAKE_METADATA_PATH")
    old_data = env_required("OLD_DUCKLAKE_DATA_PATH")
    old_schema = env_or("OLD_DUCKLAKE_METADATA_SCHEMA", "softprobe")
    old_alias = env_or("OLD_DUCKLAKE_CATALOG_ALIAS", "old")

    new_meta = env_required("NEW_DUCKLAKE_METADATA_PATH")
    new_data = env_required("NEW_DUCKLAKE_DATA_PATH")
    new_schema = env_or("NEW_DUCKLAKE_METADATA_SCHEMA", "thelake")
    new_alias = env_or("NEW_DUCKLAKE_CATALOG_ALIAS", "softprobe")

    conn = open_copy_connection(
        old_meta=old_meta,
        old_data=old_data,
        old_schema=old_schema,
        old_alias=old_alias,
        new_meta=new_meta,
        new_data=new_data,
        new_schema=new_schema,
        new_alias=new_alias,
    )
    try:
        source_table = qualified_traces(old_alias, old_schema)
        target_table = qualified_traces(new_alias, new_schema)
        source_columns = describe_column_names(conn, source_table)
        target_columns = describe_column_names(conn, target_table)
        source_tenancy = source_tenancy_column(source_columns)

        results: list[dict] = []
        for old_slug, workspace_id in mapping.items():
            results.append(
                copy_one_workspace(
                    conn,
                    source_table=source_table,
                    target_table=target_table,
                    source_columns=source_columns,
                    target_columns=target_columns,
                    source_tenancy=source_tenancy,
                    old_slug=old_slug,
                    workspace_id=workspace_id,
                    dry_run=dry_run,
                )
            )
        return results
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Copy traces from an old DuckLake (tenant_id slug) into a new shared "
            "physical scope (workspace_id UUID). Does not copy logs/scores/"
            "session_summary."
        )
    )
    parser.add_argument(
        "--mapping",
        type=Path,
        required=True,
        help="JSON or CSV mapping: old tenant_id/lake_scope_id → workspace UUID",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count matching source rows only; do not INSERT",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        mapping = load_mapping(args.mapping)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: failed to load mapping: {exc}", file=sys.stderr)
        return 2

    try:
        results = run(mapping, dry_run=args.dry_run)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"error: copy failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps({"dry_run": args.dry_run, "workspaces": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
