#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.5.2",
#   "psycopg[binary]>=3.1",
# ]
# ///
"""Standalone blue/green lakehouse migration tool for thelake.

Migrates Postgres control tables and DuckLake telemetry from a source (blue)
catalog to a pre-initialized destination (green) catalog without mutating the
source catalog.

Typical flow:
  1. Boot a temporary/pre-cutover thelake instance against dst_schema so
     thelake creates physical_scope, control tables, and DuckLake tables
     (plus any promotion_specs via POST /v1/promotions/apply).
  2. Run pre-cutover backfill up to a cutoff timestamp:
       uv run scripts/migrate_lakehouse.py \\
         --src-schema thelake --src-data-path gs://bucket/shared/ \\
         --dst-schema thelake_v2 --dst-data-path gs://bucket/shared-v2/ \\
         --until 2026-11-17T00:00:00Z
  3. Flip deployment-k8s ConfigMap to dst_schema / dst_data_path and roll thelake.
  4. Once the old pod stops, run the final delta pass:
       uv run scripts/migrate_lakehouse.py \\
         --src-schema thelake --src-data-path gs://bucket/shared/ \\
         --dst-schema thelake_v2 --dst-data-path gs://bucket/shared-v2/ \\
         --since 2026-11-17T00:00:00Z --until <cutover_ts>
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import duckdb
import psycopg

SQL_DIR = Path(__file__).resolve().parent / "migrate_lakehouse_sql"
TELEMETRY_TABLES = ("traces", "logs", "scores", "score_configs")
OPERATIONAL_PG_TABLES = (
    "promotion_specs",
    "capture_session_mappings",
    "session_summary",
    "session_summary_dirty",
    "tag_index",
    "maintenance_scope_config",
)
_PG_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


def normalize_release_version(version: str) -> tuple[str, str]:
    """Return (gcs_tag, pg_token) e.g. ('v0.1.34', 'v0_1_34')."""
    match = _SEMVER_RE.match(version.strip())
    if not match:
        raise ValueError(
            f"version must match vX.Y.Z (e.g. v0.1.34): {version!r}"
        )
    major, minor, patch = match.groups()
    return f"v{major}.{minor}.{patch}", f"v{major}_{minor}_{patch}"


def resolve_endpoint(
    side: str,
    schema: str | None,
    data_path: str | None,
    env: str | None,
    version: str | None,
    bucket: str,
) -> tuple[str, str]:
    if schema is not None and data_path is not None:
        return validate_pg_ident(schema), data_path
    if not env or not version:
        raise ValueError(
            f"--{side}-schema and --{side}-data-path are required unless both --env and --{side}-version are set"
        )
    validate_pg_ident(env)
    gcs_tag, pg_token = normalize_release_version(version)
    resolved_schema = schema or f"thelake_{env}_{pg_token}"
    resolved_path = data_path or f"gs://{bucket}/{env}/shared-{gcs_tag}/"
    return validate_pg_ident(resolved_schema), resolved_path


def load_sql(name: str, **params: str) -> str:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    for key, value in params.items():
        text = text.replace("{{" + key + "}}", value)
    return text


@dataclass(frozen=True)
class MigrationConfig:
    pg_dsn: str
    src_schema: str
    src_data_path: str
    dst_schema: str
    dst_data_path: str
    migration_id: str
    ducklake_extension: str | None = None
    src_auto_migrate: bool = False
    clone_src_schema: str | None = None
    allow_live_src_auto_migrate: bool = False
    since: str | None = None
    until: str | None = None
    dry_run: bool = False
    skip_pg: bool = False
    skip_data: bool = False
    allow_missing_dst_cols: bool = False
    memory_limit: str = "2GB"
    threads: int = 2


def validate_pg_ident(ident: str) -> str:
    if not _PG_IDENT_RE.match(ident):
        raise ValueError(f"invalid Postgres identifier: {ident!r}")
    return ident


def validate_iso_utc(value: str) -> str:
    if not _ISO_TS_RE.match(value):
        raise ValueError(f"timestamp must be UTC ISO-8601 ending in Z: {value!r}")
    return value


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def build_column_projection(
    table_name: str,
    src_cols: Sequence[tuple[str, str]],
    dst_cols: Sequence[tuple[str, str]],
    allow_missing_dst_cols: bool = False,
) -> tuple[list[str], list[str]]:
    """Build (insert_cols, select_exprs) mapping src_cols to dst_cols.

    Fails if src has columns missing from dst unless allow_missing_dst_cols is
    set, so forgotten promotion_specs are caught before copying rows.
    """
    dst_by_lower = {name.lower(): (name, ty) for name, ty in dst_cols}
    missing_in_dst = [
        name for name, _ in src_cols if name.lower() not in dst_by_lower
    ]
    if missing_in_dst and not allow_missing_dst_cols:
        raise ValueError(
            f"destination table {table_name} is missing source columns {missing_in_dst}; "
            "apply promotion_specs to destination before migrating or pass --allow-missing-dst-cols"
        )

    insert_cols: list[str] = []
    select_exprs: list[str] = []
    for src_name, src_type in src_cols:
        dst_entry = dst_by_lower.get(src_name.lower())
        if dst_entry is None:
            continue
        dst_name, dst_type = dst_entry
        insert_cols.append(dst_name)
        if dst_type.upper() == "VARIANT" and src_type.upper() != "VARIANT":
            select_exprs.append(
                f"TRY_CAST({src_name} AS JSON)::VARIANT AS {dst_name}"
            )
        else:
            select_exprs.append(src_name)

    if not insert_cols:
        raise ValueError(f"no common columns found for table {table_name}")
    return insert_cols, select_exprs


def day_window_bounds(
    day: str,
    since: str | None = None,
    until: str | None = None,
) -> tuple[str, str] | None:
    """Return [start_iso, end_iso) for a YYYY-MM-DD day intersected with [since, until)."""
    day_dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    next_dt = day_dt + timedelta(days=1)

    if since is not None:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if since_dt > day_dt:
            day_dt = since_dt
    if until is not None:
        until_dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
        if until_dt < next_dt:
            next_dt = until_dt

    if day_dt >= next_dt:
        return None
    return (
        day_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        next_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def _table_columns_pg(
    conn: psycopg.Connection, schema: str, table: str
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(load_sql("pg_table_columns.sql"), (schema, table))
        return [row[0] for row in cur.fetchall()]


def ensure_progress_table(conn: psycopg.Connection, dst_schema: str) -> None:
    validate_pg_ident(dst_schema)
    with conn.cursor() as cur:
        cur.execute(load_sql("pg_create_progress.sql", dst_schema=dst_schema))
    conn.commit()


def is_partition_done(
    conn: psycopg.Connection,
    dst_schema: str,
    migration_id: str,
    table_name: str,
    partition_day: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            load_sql("pg_is_partition_done.sql", dst_schema=dst_schema),
            (migration_id, table_name, partition_day),
        )
        return cur.fetchone() is not None


def mark_partition_done(
    conn: psycopg.Connection,
    dst_schema: str,
    migration_id: str,
    table_name: str,
    partition_day: str,
    src_rows: int,
    dst_rows: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            load_sql("pg_mark_partition_done.sql", dst_schema=dst_schema),
            (migration_id, table_name, partition_day, src_rows, dst_rows),
        )
    conn.commit()


def sync_postgres_control(cfg: MigrationConfig) -> dict[str, int]:
    """Copy workspace bindings and operational Postgres tables from src to dst."""
    validate_pg_ident(cfg.src_schema)
    validate_pg_ident(cfg.dst_schema)

    counts: dict[str, int] = {}
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(load_sql("pg_physical_scope.sql", dst_schema=cfg.dst_schema))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(
                    f"{cfg.dst_schema}.physical_scope is empty; boot thelake against "
                    f"{cfg.dst_schema} once before migrating"
                )
            dst_scope_id: str = row[0]

        src_ws_cols = _table_columns_pg(conn, cfg.src_schema, "workspace_scope_binding")
        dst_ws_cols = set(
            _table_columns_pg(conn, cfg.dst_schema, "workspace_scope_binding")
        )
        common_ws_cols = [c for c in src_ws_cols if c in dst_ws_cols]
        if "workspace_id" not in common_ws_cols:
            raise RuntimeError("workspace_scope_binding missing workspace_id column")

        if cfg.dry_run:
            with conn.cursor() as cur:
                cur.execute(load_sql("pg_workspace_count.sql", src_schema=cfg.src_schema))
                counts["workspace_scope_binding"] = cur.fetchone()[0]
        else:
            insert_list = ", ".join(common_ws_cols)
            select_list = ", ".join(
                "%s::text AS physical_scope_id" if c == "physical_scope_id" else c
                for c in common_ws_cols
            )
            params: tuple[str, ...] = (
                (dst_scope_id,) if "physical_scope_id" in common_ws_cols else ()
            )
            with conn.cursor() as cur:
                cur.execute(
                    load_sql(
                        "pg_copy_workspaces.sql",
                        src_schema=cfg.src_schema,
                        dst_schema=cfg.dst_schema,
                        insert_list=insert_list,
                        select_list=select_list,
                    ),
                    params,
                )
                counts["workspace_scope_binding"] = cur.rowcount or 0
                cur.execute(
                    load_sql(
                        "pg_missing_workspaces.sql",
                        src_schema=cfg.src_schema,
                        dst_schema=cfg.dst_schema,
                    )
                )
                missing = cur.fetchone()[0]
                if missing > 0:
                    raise RuntimeError(
                        f"{missing} workspaces from {cfg.src_schema} are still missing "
                        f"in {cfg.dst_schema}"
                    )

        for table in OPERATIONAL_PG_TABLES:
            src_cols = _table_columns_pg(conn, cfg.src_schema, table)
            dst_cols = set(_table_columns_pg(conn, cfg.dst_schema, table))
            common_cols = [c for c in src_cols if c in dst_cols]
            if not common_cols:
                continue
            if cfg.dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        load_sql("table_count.sql", schema=cfg.src_schema, table=table)
                    )
                    counts[table] = cur.fetchone()[0]
                continue

            insert_list = ", ".join(common_cols)
            uses_scope = "physical_scope_id" in common_cols
            select_list = ", ".join(
                "%s::text AS physical_scope_id" if c == "physical_scope_id" else c
                for c in common_cols
            )
            with conn.cursor() as cur:
                cur.execute(
                    load_sql(
                        "pg_copy_table.sql",
                        src_schema=cfg.src_schema,
                        dst_schema=cfg.dst_schema,
                        table=table,
                        insert_list=insert_list,
                        select_list=select_list,
                    ),
                    (dst_scope_id,) if uses_scope else (),
                )
                counts[table] = cur.rowcount or 0

        conn.commit()
    return counts


def _configure_object_store(conn: duckdb.DuckDBPyConnection, data_path: str) -> None:
    if data_path.startswith("gs://"):
        key_id = os.environ.get("GCS_HMAC_ACCESS_KEY_ID") or os.environ.get(
            "GCP_HMAC_ACCESS_KEY_ID"
        )
        secret = os.environ.get("GCS_HMAC_SECRET") or os.environ.get("GCP_HMAC_SECRET")
        if key_id and secret:
            conn.execute(
                load_sql(
                    "duckdb_create_gcs_secret.sql",
                    key_id=escape_sql_literal(key_id),
                    secret=escape_sql_literal(secret),
                )
            )
    elif data_path.startswith("s3://"):
        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        endpoint = os.environ.get("AWS_S3_ENDPOINT")
        region = os.environ.get("AWS_REGION", "us-east-1")
        if key_id and secret:
            opts = [
                "TYPE S3",
                f"KEY_ID '{escape_sql_literal(key_id)}'",
                f"SECRET '{escape_sql_literal(secret)}'",
                f"REGION '{escape_sql_literal(region)}'",
            ]
            if endpoint:
                clean_ep = endpoint.removeprefix("https://").removeprefix("http://")
                use_ssl = "true" if endpoint.startswith("https://") else "false"
                opts.extend(
                    [
                        f"ENDPOINT '{escape_sql_literal(clean_ep)}'",
                        f"USE_SSL {use_ssl}",
                        "URL_STYLE 'path'",
                    ]
                )
            conn.execute(
                load_sql("duckdb_create_s3_secret.sql", options=", ".join(opts))
            )


def _describe_table(
    conn: duckdb.DuckDBPyConnection, db: str, table: str
) -> list[tuple[str, str]] | None:
    if "." in db:
        catalog_name, schema_name = db.split(".", 1)
    else:
        catalog_name, schema_name = db, "main"
    row = conn.execute(
        load_sql("duckdb_table_exists.sql"), [catalog_name, schema_name, table]
    ).fetchone()
    if not row or row[0] == 0:
        return None
    return [
        (r[0], r[1])
        for r in conn.execute(
            load_sql("duckdb_describe_table.sql", db=db, table=table)
        ).fetchall()
    ]


def migrate_table_partitions(
    duck_conn: duckdb.DuckDBPyConnection,
    pg_conn: psycopg.Connection | None,
    cfg: MigrationConfig,
    src_db: str,
    dst_db: str,
    table: str,
) -> tuple[int, int]:
    """Migrate one table from src_db to dst_db using bounded partitions.

    Returns (partitions_copied, rows_copied).
    """
    src_cols = _describe_table(duck_conn, src_db, table)
    if src_cols is None:
        return (0, 0)
    dst_cols = _describe_table(duck_conn, dst_db, table)
    if dst_cols is None:
        raise RuntimeError(f"destination table {dst_db}.{table} does not exist")

    insert_cols, select_exprs = build_column_projection(
        table, src_cols, dst_cols, allow_missing_dst_cols=cfg.allow_missing_dst_cols
    )
    insert_cols_sql = ", ".join(insert_cols)
    select_exprs_sql = ", ".join(select_exprs)

    has_timestamp = any(name.lower() == "timestamp" for name, _ in src_cols)
    if not has_timestamp:
        if pg_conn is not None and is_partition_done(
            pg_conn, cfg.dst_schema, cfg.migration_id, table, "__all__"
        ):
            return (0, 0)
        src_count = duck_conn.execute(
            load_sql("table_count.sql", schema=src_db, table=table)
        ).fetchone()[0]
        if cfg.dry_run or src_count == 0:
            return (0, src_count)
        duck_conn.execute(load_sql("duckdb_delete_all.sql", db=dst_db, table=table))
        duck_conn.execute(
            load_sql(
                "duckdb_insert_all.sql",
                dst_db=dst_db,
                src_db=src_db,
                table=table,
                insert_cols=insert_cols_sql,
                select_exprs=select_exprs_sql,
            )
        )
        dst_count = duck_conn.execute(
            load_sql("table_count.sql", schema=dst_db, table=table)
        ).fetchone()[0]
        if dst_count != src_count:
            raise RuntimeError(
                f"{table}: dst_count ({dst_count}) != src_count ({src_count})"
            )
        if pg_conn is not None:
            mark_partition_done(
                pg_conn, cfg.dst_schema, cfg.migration_id, table, "__all__", src_count, dst_count
            )
        return (1, dst_count)

    since_ts = escape_sql_literal(cfg.since or "1970-01-01T00:00:00Z")
    until_ts = escape_sql_literal(cfg.until or "2262-01-01T00:00:00Z")
    days = [
        r[0]
        for r in duck_conn.execute(
            load_sql(
                "duckdb_distinct_days.sql",
                src_db=src_db,
                table=table,
                since_ts=since_ts,
                until_ts=until_ts,
            )
        ).fetchall()
    ]

    partitions_copied = 0
    rows_copied = 0
    for day in days:
        bounds = day_window_bounds(day, cfg.since, cfg.until)
        if bounds is None:
            continue
        start_ts, end_ts = bounds
        partition_key = f"{day}|{start_ts}|{end_ts}"
        if pg_conn is not None and is_partition_done(
            pg_conn, cfg.dst_schema, cfg.migration_id, table, partition_key
        ):
            continue

        src_count = duck_conn.execute(
            load_sql(
                "duckdb_window_count.sql",
                db=src_db,
                table=table,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        ).fetchone()[0]
        if cfg.dry_run or src_count == 0:
            rows_copied += src_count
            continue

        duck_conn.execute(
            load_sql(
                "duckdb_delete_window.sql",
                db=dst_db,
                table=table,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )
        duck_conn.execute(
            load_sql(
                "duckdb_insert_window.sql",
                dst_db=dst_db,
                src_db=src_db,
                table=table,
                insert_cols=insert_cols_sql,
                select_exprs=select_exprs_sql,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )
        dst_count = duck_conn.execute(
            load_sql(
                "duckdb_window_count.sql",
                db=dst_db,
                table=table,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        ).fetchone()[0]
        if dst_count != src_count:
            raise RuntimeError(
                f"{table} [{start_ts}, {end_ts}): dst_count ({dst_count}) != src_count ({src_count})"
            )
        if pg_conn is not None:
            mark_partition_done(
                pg_conn,
                cfg.dst_schema,
                cfg.migration_id,
                table,
                partition_key,
                src_count,
                dst_count,
            )
        partitions_copied += 1
        rows_copied += dst_count

    return (partitions_copied, rows_copied)


def clone_pg_schema(
    conn: psycopg.Connection, src_schema: str, clone_schema: str
) -> None:
    validate_pg_ident(src_schema)
    validate_pg_ident(clone_schema)
    with conn.cursor() as cur:
        cur.execute(load_sql("pg_recreate_schema.sql", schema=clone_schema))
        cur.execute(load_sql("pg_list_tables.sql"), (src_schema,))
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            validate_pg_ident(table)
            cur.execute(
                load_sql(
                    "pg_clone_table.sql",
                    src_schema=src_schema,
                    dst_schema=clone_schema,
                    table=table,
                )
            )
    conn.commit()


def drop_pg_schema(conn: psycopg.Connection, schema: str) -> None:
    validate_pg_ident(schema)
    with conn.cursor() as cur:
        cur.execute(load_sql("pg_drop_schema.sql", schema=schema))
    conn.commit()


def migrate_ducklake_data(cfg: MigrationConfig) -> dict[str, tuple[int, int]]:
    validate_pg_ident(cfg.src_schema)
    validate_pg_ident(cfg.dst_schema)
    if cfg.clone_src_schema:
        validate_pg_ident(cfg.clone_src_schema)

    src_attach_schema = cfg.clone_src_schema or cfg.src_schema
    if cfg.clone_src_schema:
        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            clone_pg_schema(pg_conn, cfg.src_schema, cfg.clone_src_schema)

    try:
        duck_conn = duckdb.connect(
            ":memory:",
            config={
                "allow_unsigned_extensions": "true",
                "memory_limit": cfg.memory_limit,
                "threads": cfg.threads,
            },
        )
        if cfg.ducklake_extension:
            duck_conn.execute(f"LOAD '{escape_sql_literal(cfg.ducklake_extension)}'")
        else:
            duck_conn.execute("LOAD ducklake")

        _configure_object_store(duck_conn, cfg.src_data_path)
        if cfg.dst_data_path != cfg.src_data_path:
            _configure_object_store(duck_conn, cfg.dst_data_path)

        src_opts = [
            f"METADATA_SCHEMA '{escape_sql_literal(src_attach_schema)}'",
            f"DATA_PATH '{escape_sql_literal(cfg.src_data_path)}'",
            "CREATE_IF_NOT_EXISTS false",
        ]
        if cfg.src_auto_migrate:
            src_opts.append("AUTOMATIC_MIGRATION true")

        duck_conn.execute(
            load_sql(
                "duckdb_attach_lake.sql",
                dsn=escape_sql_literal(cfg.pg_dsn),
                alias="src_lake",
                options=", ".join(src_opts),
            )
        )
        dst_opts = [
            f"METADATA_SCHEMA '{escape_sql_literal(cfg.dst_schema)}'",
            f"DATA_PATH '{escape_sql_literal(cfg.dst_data_path)}'",
            "CREATE_IF_NOT_EXISTS false",
        ]
        duck_conn.execute(
            load_sql(
                "duckdb_attach_lake.sql",
                dsn=escape_sql_literal(cfg.pg_dsn),
                alias="dst_lake",
                options=", ".join(dst_opts),
            )
        )

        results: dict[str, tuple[int, int]] = {}
        with psycopg.connect(cfg.pg_dsn) as pg_conn:
            if not cfg.dry_run:
                ensure_progress_table(pg_conn, cfg.dst_schema)
            src_qualifier = f"src_lake.{src_attach_schema}"
            dst_qualifier = f"dst_lake.{cfg.dst_schema}"
            for table in TELEMETRY_TABLES:
                results[table] = migrate_table_partitions(
                    duck_conn, pg_conn, cfg, src_qualifier, dst_qualifier, table
                )
        return results
    finally:
        if cfg.clone_src_schema:
            with psycopg.connect(cfg.pg_dsn) as pg_conn:
                drop_pg_schema(pg_conn, cfg.clone_src_schema)


def parse_args(argv: Sequence[str] | None = None) -> MigrationConfig:
    parser = argparse.ArgumentParser(
        description="Blue/green lakehouse migration tool for thelake"
    )
    parser.add_argument("--pg-dsn", default=os.environ.get("DUCKLAKE_METADATA_PATH"))
    parser.add_argument("--env", default=None, help="Environment name (e.g. staging, prod) for versioned naming")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("DATALAKE_BUCKET", "softprobe-thelake"),
        help="GCS bucket used when deriving data paths from --env and --*-version",
    )
    parser.add_argument("--src-version", default=None, help="Source thelake release version (e.g. v0.1.33)")
    parser.add_argument("--dst-version", default=None, help="Destination thelake release version (e.g. v0.1.34)")
    parser.add_argument("--src-schema", default=None, help="Override source Postgres schema (for legacy names)")
    parser.add_argument("--src-data-path", default=None, help="Override source GCS/local data path (for legacy prefixes)")
    parser.add_argument("--dst-schema", default=None, help="Override destination Postgres schema")
    parser.add_argument("--dst-data-path", default=None, help="Override destination GCS/local data path")
    parser.add_argument(
        "--migration-id",
        default=None,
        help="Stable id for resumable _migration_progress tracking (defaults to <src>-to-<dst>)",
    )
    parser.add_argument(
        "--ducklake-extension",
        default=os.environ.get("DUCKLAKE_EXTENSION_PATH"),
    )
    parser.add_argument(
        "--src-auto-migrate",
        action="store_true",
        help="Pass AUTOMATIC_MIGRATION true when attaching src_lake (requires --clone-src-schema or --allow-live-src-auto-migrate)",
    )
    parser.add_argument(
        "--clone-src-schema",
        default=None,
        help="Clone src_schema metadata into a temporary Postgres schema before attaching DuckLake (dropped on exit)",
    )
    parser.add_argument(
        "--allow-live-src-auto-migrate",
        action="store_true",
        help="Allow --src-auto-migrate directly against live src_schema (mutates source catalog metadata)",
    )
    parser.add_argument("--since", default=None, help="Inclusive UTC ISO-8601 bound (Z)")
    parser.add_argument("--until", default=None, help="Exclusive UTC ISO-8601 bound (Z)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-pg", action="store_true")
    parser.add_argument("--skip-data", action="store_true")
    parser.add_argument("--allow-missing-dst-cols", action="store_true")
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--threads", type=int, default=2)

    ns = parser.parse_args(argv)
    if not ns.pg_dsn:
        parser.error("--pg-dsn or DUCKLAKE_METADATA_PATH is required")
    try:
        src_schema, src_data_path = resolve_endpoint(
            "src", ns.src_schema, ns.src_data_path, ns.env, ns.src_version, ns.bucket
        )
        dst_schema, dst_data_path = resolve_endpoint(
            "dst", ns.dst_schema, ns.dst_data_path, ns.env, ns.dst_version, ns.bucket
        )
    except ValueError as exc:
        parser.error(str(exc))

    if src_schema == dst_schema:
        parser.error("--src-schema and --dst-schema must differ for blue/green safety")

    if ns.clone_src_schema:
        validate_pg_ident(ns.clone_src_schema)
        if ns.clone_src_schema in (src_schema, dst_schema):
            parser.error("--clone-src-schema must differ from --src-schema and --dst-schema")
    if ns.src_auto_migrate and not ns.clone_src_schema and not ns.allow_live_src_auto_migrate:
        parser.error(
            "--src-auto-migrate mutates source catalog metadata; pass --clone-src-schema <tmp_schema> or --allow-live-src-auto-migrate"
        )
    if ns.since:
        validate_iso_utc(ns.since)
    if ns.until:
        validate_iso_utc(ns.until)

    migration_id = ns.migration_id or f"{src_schema}-to-{dst_schema}"
    return MigrationConfig(
        pg_dsn=ns.pg_dsn,
        src_schema=src_schema,
        src_data_path=src_data_path,
        dst_schema=dst_schema,
        dst_data_path=dst_data_path,
        migration_id=migration_id,
        ducklake_extension=ns.ducklake_extension,
        src_auto_migrate=ns.src_auto_migrate,
        clone_src_schema=ns.clone_src_schema,
        allow_live_src_auto_migrate=ns.allow_live_src_auto_migrate,
        since=ns.since,
        until=ns.until,
        dry_run=ns.dry_run,
        skip_pg=ns.skip_pg,
        skip_data=ns.skip_data,
        allow_missing_dst_cols=ns.allow_missing_dst_cols,
        memory_limit=ns.memory_limit,
        threads=ns.threads,
    )


def main(argv: Sequence[str] | None = None) -> int:
    cfg = parse_args(argv)
    if not cfg.skip_pg:
        pg_counts = sync_postgres_control(cfg)
        print(f"pg_control={pg_counts}", flush=True)
    if not cfg.skip_data:
        data_counts = migrate_ducklake_data(cfg)
        print(f"ducklake_data={data_counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
