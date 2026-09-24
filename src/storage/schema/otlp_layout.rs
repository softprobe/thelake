//! DuckLake partition + sort for OTLP `traces` / `logs` / `scores`.
//!
//! Partition = calendar day of `timestamp` via [`crate::sql::ONE_CLOCK_PARTITION_BY`].
//! No `record_date` column. Session locality is sort, not partition.

use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::ducklake_partition::table_partition_sort_ready;
use crate::sql::schema::{
    insert_order_by as registry_insert_order_by, is_otlp_table, partition_sort_sql, OTLP_TABLES,
};

pub use crate::models::partition_day_from_event_time;

/// Idempotent one-clock `SET PARTITIONED BY` + `SET SORTED BY`.
pub(crate) fn ensure_otlp_table_partition_sort(
    conn: &Connection,
    qualified_table: &str,
) -> Result<()> {
    let table_name = qualified_table
        .rsplit('.')
        .next()
        .unwrap_or(qualified_table);
    let Some(table) = OTLP_TABLES.iter().find(|table| table.name == table_name) else {
        return Ok(());
    };
    if table_partition_sort_ready(conn, qualified_table, table_name)? {
        return Ok(());
    }
    let catalog = qualified_table
        .rsplit_once('.')
        .map(|(catalog, _)| catalog)
        .unwrap_or("main");
    let sql = partition_sort_sql(catalog, table);
    conn.execute_batch(&sql)
        .map_err(|e| anyhow!("failed to apply OTLP partition/sort on {qualified_table}: {e}"))?;
    Ok(())
}

/// Whether `ensure_otlp_table_partition_sort` applies to this table name.
pub fn is_otlp_layout_table(table_name: &str) -> bool {
    is_otlp_table(table_name)
}

/// Writer `ORDER BY` clause aligned with `SET SORTED BY` (no partition-column lead).
pub fn insert_order_by(table_name: &str) -> &'static str {
    registry_insert_order_by(table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_is_otlp_layout_table() {
        assert!(is_otlp_layout_table("scores"));
        assert!(is_otlp_layout_table("traces"));
        assert!(is_otlp_layout_table("logs"));
        assert!(!is_otlp_layout_table("metric_samples"));
    }

    #[test]
    fn layout_covers_traces_logs_scores() {
        assert_eq!(OTLP_TABLES.len(), 3);
        assert!(OTLP_TABLES.iter().any(|t| t.name == "traces"));
        assert!(OTLP_TABLES.iter().any(|t| t.name == "logs"));
        assert!(OTLP_TABLES.iter().any(|t| t.name == "scores"));
    }

    #[test]
    fn partition_sql_is_one_clock_not_record_date() {
        for layout in OTLP_TABLES {
            let sql = partition_sort_sql("softprobe", layout);
            assert!(!sql.contains("record_date"), "{sql}");
            assert!(
                sql.contains(&format!("SET SORTED BY ({})", layout.sorted_by)),
                "{sql}"
            );
        }
    }

    #[test]
    fn writer_order_by_matches_sorted_by() {
        for layout in OTLP_TABLES {
            let order = insert_order_by(layout.name);
            assert!(order.starts_with("ORDER BY "), "{}: {order}", layout.name);
            assert!(!order.contains("record_date"), "{}: {order}", layout.name);
            let sorted_tail = order.trim_start_matches("ORDER BY ");
            assert_eq!(sorted_tail, layout.sorted_by, "{}", layout.name);
        }
    }

    #[test]
    fn single_partition_day_assignment_path_in_otlp_writers() {
        let arrow = include_str!("arrow.rs");
        assert!(
            arrow.contains("partition_day_from_event_time"),
            "arrow write path must use partition_day_from_event_time"
        );
        for needle in [
            "s.timestamp.date_naive()",
            "l.timestamp.date_naive()",
            "score.timestamp.date_naive()",
        ] {
            assert!(
                !arrow.contains(needle),
                "arrow must not assign partition day via {needle}"
            );
        }
    }
}
