//! DuckLake partition + sort for OTLP `traces` / `logs` / `scores`.
//!
//! Partition day is `date(timestamp)` stored as `record_date` (legacy spelling).
//! Session locality is sort, not partition — see `docs/design-event-time-layout.md`.

use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::ducklake_partition::table_partition_sort_ready;

pub use crate::models::partition_day_from_event_time;

const PARTITION_COLUMN: &str = "record_date";

#[derive(Debug, Clone, Copy)]
struct OtlpLayoutTable {
    name: &'static str,
    /// Columns after partition key — must match writer `ORDER BY` without `record_date`.
    sorted_by: &'static str,
}

const OTLP_LAYOUT_TABLES: &[OtlpLayoutTable] = &[
    OtlpLayoutTable {
        name: "traces",
        sorted_by: "session_id, trace_id, timestamp",
    },
    OtlpLayoutTable {
        name: "logs",
        sorted_by: "session_id, timestamp",
    },
    OtlpLayoutTable {
        name: "scores",
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

/// Whether `ensure_otlp_table_partition_sort` applies to this table name.
pub fn is_otlp_layout_table(table_name: &str) -> bool {
    OTLP_LAYOUT_TABLES.iter().any(|t| t.name == table_name)
}

/// Writer `ORDER BY` clause aligned with `SET SORTED BY` (includes partition column lead).
pub fn insert_order_by(table_name: &str) -> &'static str {
    match table_name {
        "traces" => "ORDER BY record_date, session_id, trace_id, timestamp",
        "logs" => "ORDER BY record_date, session_id, timestamp",
        "scores" => "ORDER BY record_date, session_id, timestamp",
        _ => "",
    }
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
        assert_eq!(OTLP_LAYOUT_TABLES.len(), 3);
        assert!(OTLP_LAYOUT_TABLES.iter().any(|t| t.name == "traces"));
        assert!(OTLP_LAYOUT_TABLES.iter().any(|t| t.name == "logs"));
        assert!(OTLP_LAYOUT_TABLES.iter().any(|t| t.name == "scores"));
    }

    #[test]
    fn sort_keys_match_design_d8_d9() {
        let traces = OTLP_LAYOUT_TABLES
            .iter()
            .find(|t| t.name == "traces")
            .unwrap();
        let logs = OTLP_LAYOUT_TABLES
            .iter()
            .find(|t| t.name == "logs")
            .unwrap();
        let scores = OTLP_LAYOUT_TABLES
            .iter()
            .find(|t| t.name == "scores")
            .unwrap();
        assert_eq!(traces.sorted_by, "session_id, trace_id, timestamp");
        assert_eq!(logs.sorted_by, "session_id, timestamp");
        assert_eq!(scores.sorted_by, "session_id, timestamp");
        assert!(!traces.sorted_by.contains("app_id"));
    }

    #[test]
    fn partition_sql_is_record_date_not_timestamp_transforms() {
        for layout in OTLP_LAYOUT_TABLES {
            let sql = format!(
                "ALTER TABLE softprobe.{} SET PARTITIONED BY ({PARTITION_COLUMN});\n\
                 ALTER TABLE softprobe.{} SET SORTED BY ({});",
                layout.name, layout.name, layout.sorted_by
            );
            assert!(sql.contains("SET PARTITIONED BY (record_date)"), "{sql}");
            assert!(
                !sql.contains("year(")
                    && !sql.contains("month(")
                    && !sql.contains("day(timestamp)"),
                "no timestamp transform partition: {sql}"
            );
            assert!(
                sql.contains(&format!("SET SORTED BY ({})", layout.sorted_by)),
                "{sql}"
            );
        }
    }

    #[test]
    fn writer_order_by_matches_sorted_by() {
        for layout in OTLP_LAYOUT_TABLES {
            let order = insert_order_by(layout.name);
            assert!(
                order.starts_with("ORDER BY record_date, "),
                "{}: {order}",
                layout.name
            );
            let sorted_tail = order.trim_start_matches("ORDER BY record_date, ");
            assert_eq!(sorted_tail, layout.sorted_by, "{}", layout.name);
        }
    }

    #[test]
    fn single_partition_day_assignment_path_in_otlp_writers() {
        let arrow = include_str!("arrow.rs");
        let llm = include_str!("../../api/llm/mod.rs");
        assert!(
            arrow.contains("partition_day_from_event_time"),
            "arrow write path must use partition_day_from_event_time"
        );
        assert!(
            llm.contains("partition_day_from_event_time"),
            "score create path must use partition_day_from_event_time"
        );
        for needle in [
            "s.timestamp.date_naive()",
            "l.timestamp.date_naive()",
            "score.timestamp.date_naive()",
            "request.timestamp.date_naive()",
        ] {
            assert!(
                !arrow.contains(needle),
                "arrow must not assign partition day via {needle}"
            );
            if needle.starts_with("request.") {
                assert!(
                    !llm.contains(needle),
                    "llm score create must not use {needle}"
                );
            }
        }
    }
}
