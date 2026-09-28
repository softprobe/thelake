"""SQL fragments for seeding DuckLake from one-clock Parquet.

Lockstep with `src/sql/writer/mod.rs` create_from_parquet_sql / insert_batch_sql
and `src/sql/schema/mod.rs` ONE_CLOCK_PARTITION_BY + OTLP sorted_by.
"""

from __future__ import annotations

# Mirror src/sql/schema/mod.rs
ONE_CLOCK_PARTITION_BY = "year(timestamp), month(timestamp), day(timestamp)"
SORTED_BY = {
    "traces": "session_id, trace_id, timestamp",
    "logs": "session_id, timestamp",
}


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def load_table_sql(
    *,
    alias: str,
    table: str,
    parquet_path: str,
    force_drop: bool,
) -> str:
    """Schema-only CREATE + partition/sort + INSERT BY NAME (never CTAS-with-data)."""
    if table not in SORTED_BY:
        raise ValueError(f"unsupported OTLP table: {table}")
    q = f"{alias}.{table}"
    path = sql_escape(parquet_path)
    parts: list[str] = []
    if force_drop:
        parts.append(f"DROP TABLE IF EXISTS {q};")
    # Same shape as create_from_parquet_sql(..., LIMIT 0)
    parts.append(
        f"CREATE TABLE IF NOT EXISTS {q} AS SELECT * FROM read_parquet('{path}') LIMIT 0;"
    )
    parts.append(
        f"ALTER TABLE {q} SET PARTITIONED BY ({ONE_CLOCK_PARTITION_BY});"
    )
    parts.append(f"ALTER TABLE {q} SET SORTED BY ({SORTED_BY[table]});")
    # Same shape as insert_batch_sql(..., BY NAME ... FROM read_parquet)
    parts.append(f"INSERT INTO {q} BY NAME SELECT * FROM read_parquet('{path}');")
    parts.append(f"SELECT count(*) AS {table}_rows FROM {q};")
    return "\n".join(parts)
