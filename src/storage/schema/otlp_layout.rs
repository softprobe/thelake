//! DuckLake partition + sort for OTLP `traces` / `logs` (SoftProbe Rolling prune).
//!
//! Metrics already use `SET PARTITIONED BY (record_date)`. Traces/logs historically only
//! had insert `ORDER BY record_date, …` + `hive_file_pattern`, so product id filters still
//! opened growing Parquet sets. Apply the same partition invariant so backend
//! `record_date` predicates can prune.

use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::ducklake_partition::table_partition_sort_ready;

const PARTITION_COLUMN: &str = "record_date";

#[derive(Debug, Clone, Copy)]
struct OtlpLayoutTable {
    name: &'static str,
    sorted_by: &'static str,
}

const OTLP_LAYOUT_TABLES: &[OtlpLayoutTable] = &[
    OtlpLayoutTable {
        name: "traces",
        sorted_by: "app_id, session_id, timestamp",
    },
    OtlpLayoutTable {
        name: "logs",
        sorted_by: "session_id, timestamp",
    },
];

/// Idempotent `SET PARTITIONED BY (record_date)` + `SET SORTED BY (…)`.
///
/// `qualified_table` is whatever form the writer used for CREATE/INSERT (catalog.table or
/// catalog.schema.table). Table name for catalog readiness checks is the last path segment.
pub fn ensure_otlp_table_partition_sort(conn: &Connection, qualified_table: &str) -> Result<()> {
    let table_name = qualified_table
        .rsplit('.')
        .next()
        .unwrap_or(qualified_table);
    let Some(layout) = OTLP_LAYOUT_TABLES.iter().find(|t| t.name == table_name) else {
        return Ok(());
    };
    if table_partition_sort_ready(conn, qualified_table, table_name)? {
        return Ok(());
    }
    let sql = format!(
        "ALTER TABLE {qualified_table} SET PARTITIONED BY ({PARTITION_COLUMN});\n\
         ALTER TABLE {qualified_table} SET SORTED BY ({});",
        layout.sorted_by
    );
    conn.execute_batch(&sql)
        .map_err(|e| anyhow!("failed to apply OTLP partition/sort on {qualified_table}: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_covers_traces_and_logs() {
        assert_eq!(OTLP_LAYOUT_TABLES.len(), 2);
        assert!(OTLP_LAYOUT_TABLES.iter().any(|t| t.name == "traces"));
        assert!(OTLP_LAYOUT_TABLES.iter().any(|t| t.name == "logs"));
    }
}
