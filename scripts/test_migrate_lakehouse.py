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
