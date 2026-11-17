#!/usr/bin/env python3
"""Tests for scripts/migrate_lakehouse.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migrate_lakehouse as ml


def test_build_column_projection_casts_map_to_variant() -> None:
    src = [
        ("trace_id", "VARCHAR"),
        ("attributes", "MAP(VARCHAR, VARCHAR)"),
        ("events", "MAP(VARCHAR, VARCHAR)"),
    ]
    dst = [
        ("trace_id", "VARCHAR"),
        ("attributes", "VARIANT"),
        ("events", "VARIANT"),
    ]
    insert_cols, select_exprs = ml.build_column_projection("traces", src, dst)
    assert insert_cols == ["trace_id", "attributes", "events"]
    assert select_exprs == [
        "trace_id",
        "TRY_CAST(attributes AS JSON)::VARIANT AS attributes",
        "TRY_CAST(events AS JSON)::VARIANT AS events",
    ]


def test_build_column_projection_rejects_missing_promoted_col() -> None:
    src = [("trace_id", "VARCHAR"), ("promo_col", "VARCHAR")]
    dst = [("trace_id", "VARCHAR")]
    with pytest.raises(ValueError, match="promo_col"):
        ml.build_column_projection("traces", src, dst)

    cols, exprs = ml.build_column_projection(
        "traces", src, dst, allow_missing_dst_cols=True
    )
    assert cols == ["trace_id"]
    assert exprs == ["trace_id"]


def test_day_window_bounds_respects_since_and_until() -> None:
    assert ml.day_window_bounds("2026-11-16", None, None) == (
        "2026-11-16T00:00:00Z",
        "2026-11-17T00:00:00Z",
    )
    assert ml.day_window_bounds(
        "2026-11-16", "2026-11-16T06:00:00Z", "2026-11-16T18:00:00Z"
    ) == ("2026-11-16T06:00:00Z", "2026-11-16T18:00:00Z")
    assert (
        ml.day_window_bounds(
            "2026-11-16", "2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z"
        )
        is None
    )


def test_parse_args_rejects_same_src_and_dst_schema() -> None:
    with pytest.raises(SystemExit):
        ml.parse_args(
            [
                "--pg-dsn",
                "host=localhost dbname=test",
                "--src-schema",
                "thelake",
                "--src-data-path",
                "/tmp/a",
                "--dst-schema",
                "thelake",
                "--dst-data-path",
                "/tmp/b",
            ]
        )


def test_migrate_table_partitions_preserves_post_until_live_rows() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(ml.load_sql("test_seed_partition_tables.sql"))

    cfg = ml.MigrationConfig(
        pg_dsn="unused",
        src_schema="src",
        src_data_path="/tmp/src",
        dst_schema="dst",
        dst_data_path="/tmp/dst",
        migration_id="test",
        until="2026-11-16T12:00:00Z",
    )
    parts, rows = ml.migrate_table_partitions(
        conn, None, cfg, "src_db", "dst_db", "traces"
    )
    assert parts == 1
    assert rows == 1

    dst_rows = conn.execute(ml.load_sql("test_select_dst_traces.sql")).fetchall()
    assert dst_rows == [("live_1", "live"), ("t1", "v1")]

    delta_cfg = ml.MigrationConfig(
        pg_dsn="unused",
        src_schema="src",
        src_data_path="/tmp/src",
        dst_schema="dst",
        dst_data_path="/tmp/dst",
        migration_id="test",
        since="2026-11-16T12:00:00Z",
        until="2026-11-16T15:30:00Z",
    )
    parts_d, rows_d = ml.migrate_table_partitions(
        conn, None, delta_cfg, "src_db", "dst_db", "traces"
    )
    assert parts_d == 1
    assert rows_d == 1
    dst_rows_after = conn.execute(ml.load_sql("test_select_dst_traces.sql")).fetchall()
    assert dst_rows_after == [("live_1", "live"), ("t1", "v1"), ("t2", "v2")]


def test_parse_args_guards_src_auto_migrate_without_clone() -> None:
    base_args = [
        "--pg-dsn",
        "host=localhost dbname=test",
        "--src-schema",
        "thelake",
        "--src-data-path",
        "/tmp/a",
        "--dst-schema",
        "thelake_v2",
        "--dst-data-path",
        "/tmp/b",
        "--src-auto-migrate",
    ]
    with pytest.raises(SystemExit):
        ml.parse_args(base_args)

    cfg = ml.parse_args([*base_args, "--clone-src-schema", "thelake_mig_src"])
    assert cfg.clone_src_schema == "thelake_mig_src"
    assert cfg.src_auto_migrate is True


def test_migrate_table_partitions_with_attached_named_schemas() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(ml.load_sql("test_seed_attached_schemas.sql"))

    cfg = ml.MigrationConfig(
        pg_dsn="unused",
        src_schema="thelake",
        src_data_path="/tmp/src",
        dst_schema="thelake_v2",
        dst_data_path="/tmp/dst",
        migration_id="test",
    )
    parts, rows = ml.migrate_table_partitions(
        conn, None, cfg, "src_lake.thelake", "dst_lake.thelake_v2", "traces"
    )
    assert parts == 1
    assert rows == 2
    assert ml._describe_table(conn, "src_lake.thelake", "missing") is None

    dst_rows = conn.execute(
        ml.load_sql("test_select_attached_dst_traces.sql")
    ).fetchall()
    assert dst_rows == [("t1", "v"), ("t2", None)]


def test_parse_args_derives_versioned_naming_and_allows_legacy_overrides() -> None:
    cfg = ml.parse_args(
        [
            "--pg-dsn",
            "host=localhost dbname=test",
            "--env",
            "prod",
            "--src-version",
            "0.1.33",
            "--dst-version",
            "v0.1.34",
        ]
    )
    assert cfg.src_schema == "thelake_prod_v0_1_33"
    assert cfg.src_data_path == "gs://softprobe-thelake/prod/shared-v0.1.33/"
    assert cfg.dst_schema == "thelake_prod_v0_1_34"
    assert cfg.dst_data_path == "gs://softprobe-thelake/prod/shared-v0.1.34/"

    legacy_cfg = ml.parse_args(
        [
            "--pg-dsn",
            "host=localhost dbname=test",
            "--env",
            "staging",
            "--src-schema",
            "thelake_staging_ducklake11",
            "--src-data-path",
            "gs://softprobe-thelake/staging/shared-ducklake11/",
            "--dst-version",
            "v0.1.34",
        ]
    )
    assert legacy_cfg.src_schema == "thelake_staging_ducklake11"
    assert legacy_cfg.src_data_path == "gs://softprobe-thelake/staging/shared-ducklake11/"
    assert legacy_cfg.dst_schema == "thelake_staging_v0_1_34"
    assert legacy_cfg.dst_data_path == "gs://softprobe-thelake/staging/shared-v0.1.34/"

    with pytest.raises(SystemExit):
        ml.parse_args(
            [
                "--pg-dsn",
                "host=localhost dbname=test",
                "--env",
                "staging",
                "--dst-version",
                "v0.1.34",
            ]
        )


def test_migrate_table_partitions_dry_run_does_not_mutate_dst() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(ml.load_sql("test_seed_attached_schemas.sql"))

    class FailIfQueriedPgConn:
        def cursor(self):
            raise AssertionError("dry_run must not query _migration_progress")

    cfg = ml.MigrationConfig(
        pg_dsn="unused",
        src_schema="thelake",
        src_data_path="/tmp/src",
        dst_schema="thelake_v2",
        dst_data_path="/tmp/dst",
        migration_id="test-dry-run",
        dry_run=True,
    )
    parts, rows = ml.migrate_table_partitions(
        conn, FailIfQueriedPgConn(), cfg, "src_lake.thelake", "dst_lake.thelake_v2", "traces"
    )
    assert parts == 0
    assert rows == 2
    assert conn.execute(ml.load_sql("test_select_attached_dst_traces.sql")).fetchall() == []


def test_migrate_ducklake_data_closes_duck_before_drop_and_uses_src_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    seen_qualifiers: list[tuple[str, str]] = []

    class FakeDuckConn:
        def execute(self, sql: str):
            return self

        def close(self) -> None:
            events.append("duck_close")

    class FakePgConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(ml.psycopg, "connect", lambda dsn: FakePgConn())
    monkeypatch.setattr(ml.duckdb, "connect", lambda *a, **kw: FakeDuckConn())
    monkeypatch.setattr(
        ml, "clone_pg_schema", lambda conn, s, c: events.append(f"clone:{s}->{c}")
    )
    monkeypatch.setattr(
        ml, "drop_pg_schema", lambda conn, c: events.append(f"drop:{c}")
    )

    def fake_migrate(duck_conn, pg_conn, cfg, src_q, dst_q, table):
        seen_qualifiers.append((src_q, dst_q))
        return (0, 0)

    monkeypatch.setattr(ml, "migrate_table_partitions", fake_migrate)

    cfg = ml.MigrationConfig(
        pg_dsn="host=localhost dbname=test",
        src_schema="thelake_prod",
        src_data_path="/tmp/src",
        dst_schema="thelake_prod_v0_1_34",
        dst_data_path="/tmp/dst",
        migration_id="test",
        src_auto_migrate=True,
        clone_src_schema="thelake_prod_mig_src",
        dry_run=True,
    )
    ml.migrate_ducklake_data(cfg)
    assert events == [
        "clone:thelake_prod->thelake_prod_mig_src",
        "duck_close",
        "drop:thelake_prod_mig_src",
    ]
    assert all(
        sq == "src_lake.thelake_prod" and dq == "dst_lake.thelake_prod_v0_1_34"
        for sq, dq in seen_qualifiers
    )
