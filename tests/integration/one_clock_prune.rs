//! One-clock production contract: real writers create calendar-day files and
//! timestamp-bounded recipes prune them without legacy date columns.

use chrono::{TimeZone, Utc};
use softprobe_runtime::ingest_engine::IngestEngine;
use softprobe_runtime::models::{Log, Span, SpanEvent};
use softprobe_runtime::query::{LogCountFilter, TraceCountFilter};
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

/// Locked partition clause (no DATE identity column).
pub const ONE_CLOCK_PARTITION_BY: &str = "year(timestamp), month(timestamp), day(timestamp)";

fn flatten_plan(plan: &str) -> String {
    plan.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '=' | '/' | ':'))
        .collect()
}

fn files_read_count(plan: &str) -> Option<u32> {
    let marker = "Total Files Read:";
    let idx = plan.find(marker)?;
    plan[idx + marker.len()..]
        .split(|c: char| !c.is_ascii_digit())
        .find(|tok| !tok.is_empty())
        .and_then(|tok| tok.parse().ok())
}

fn walk_paths(dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_paths(&path, out);
        } else {
            out.push(path.to_string_lossy().into_owned());
        }
    }
}

fn attach(config: &softprobe_runtime::config::DuckLakeConfig) -> duckdb::Connection {
    softprobe_runtime::storage::ducklake::open_attached_from_config(config, Some(0))
}

fn explain_plan(conn: &duckdb::Connection, sql: &str) -> String {
    let mut stmt = conn
        .prepare(&format!("EXPLAIN ANALYZE {sql}"))
        .expect("prepare explain");
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("explain query");
    rows.filter_map(|r| r.ok()).collect::<Vec<_>>().join("\n")
}

fn timestamp_type(conn: &duckdb::Connection, table: &str) -> String {
    conn.query_row(
        &format!("SELECT column_type FROM (DESCRIBE {table}) WHERE column_name = 'timestamp'"),
        [],
        |row| row.get(0),
    )
    .unwrap_or_else(|e| panic!("timestamp type for {table}: {e}"))
}

fn span(day: u32, id: &str) -> Span {
    let timestamp = Utc.with_ymd_and_hms(2026, 9, day, 12, 0, 0).unwrap();
    Span {
        session_id: "persistent-session".into(),
        trace_id: format!("trace-{id}"),
        span_id: format!("span-{id}"),
        parent_span_id: None,
        app_id: "one-clock".into(),
        organization_id: None,
        tenant_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "INTERNAL".into(),
        span_kind: Some("INTERNAL".into()),
        timestamp,
        end_timestamp: Some(timestamp + chrono::Duration::seconds(1)),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: vec![SpanEvent {
            name: "finished".into(),
            timestamp,
            attributes: HashMap::new(),
        }],
        status_code: Some("OK".into()),
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}

fn log(day: u32, id: &str) -> Log {
    let timestamp = Utc.with_ymd_and_hms(2026, 9, day, 12, 0, 0).unwrap();
    Log {
        session_id: Some("persistent-session".into()),
        timestamp,
        observed_timestamp: Some(timestamp),
        severity_number: 9,
        severity_text: "INFO".into(),
        body: format!("log-{id}"),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(format!("trace-{id}")),
        span_id: Some(format!("span-{id}")),
        tenant_id: None,
        agent_id: None,
        agent_name: None,
    }
}

#[tokio::test]
async fn production_writers_partition_and_prune_one_clock_fact_tables() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ingest.flush_interval_seconds = 0;
    // Force every production write to publish a parquet file so EXPLAIN observes
    // physical day pruning rather than DuckLake catalog-inline rows.
    config.ducklake.data_inlining_row_limit = Some(0);
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");

    pipeline
        .add_spans(vec![span(10, "a"), span(11, "b")], 0)
        .await
        .expect("traces ingest");
    pipeline
        .add_logs(vec![log(10, "a"), log(11, "b")], 0)
        .await
        .expect("logs ingest");

    let mut paths = Vec::new();
    walk_paths(Path::new(&data_path), &mut paths);
    let joined = paths.join("\n");
    for table in ["traces", "logs"] {
        assert!(
            joined.contains(&format!("{table}/year=2026/month=9/day=10")),
            "{table} day A:\n{joined}"
        );
        assert!(
            joined.contains(&format!("{table}/year=2026/month=9/day=11")),
            "{table} day B:\n{joined}"
        );
    }
    assert!(
        !joined.contains("record_date="),
        "legacy partition path:\n{joined}"
    );

    let conn = attach(&config.ducklake);
    assert_eq!(timestamp_type(&conn, "traces"), "TIMESTAMP_NS");
    assert_eq!(timestamp_type(&conn, "logs"), "TIMESTAMP_NS");

    let narrow = "SELECT trace_id FROM traces \
                  WHERE timestamp >= '2026-09-10'::TIMESTAMP_NS \
                    AND timestamp < '2026-09-11'::TIMESTAMP_NS";
    let wide = "SELECT trace_id FROM traces \
                WHERE timestamp >= '2026-09-10'::TIMESTAMP_NS \
                  AND timestamp < '2026-09-12'::TIMESTAMP_NS";
    let wide_plan = explain_plan(&conn, wide);
    assert_eq!(
        files_read_count(&wide_plan),
        Some(2),
        "wide plan:\n{wide_plan}"
    );
    let narrow_plan = explain_plan(&conn, narrow);
    assert_eq!(
        files_read_count(&narrow_plan),
        Some(1),
        "narrow plan:\n{narrow_plan}"
    );
    assert!(
        !flatten_plan(&narrow_plan).contains("day=11"),
        "narrow plan opened day B:\n{narrow_plan}"
    );
}

#[tokio::test]
async fn typed_query_gate_covers_traces_and_logs() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ingest.flush_interval_seconds = 0;
    config.ducklake.data_inlining_row_limit = Some(0);
    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");
    pipeline
        .add_spans(vec![span(10, "gate")], 0)
        .await
        .expect("trace seed");
    pipeline
        .add_logs(vec![log(10, "gate")], 0)
        .await
        .expect("log seed");

    let query = softprobe_runtime::query::create_query_engine(&config)
        .await
        .expect("query engine");
    assert_eq!(
        query
            .count_traces(TraceCountFilter {
                session_id: Some("persistent-session".into()),
                ..Default::default()
            })
            .await
            .expect("trace count"),
        1
    );
    assert_eq!(
        query
            .count_logs(LogCountFilter {
                session_id: Some("persistent-session".into()),
                ..Default::default()
            })
            .await
            .expect("log count"),
        1
    );
}

#[test]
fn locked_partition_expression_is_year_month_day_of_timestamp() {
    assert_eq!(
        ONE_CLOCK_PARTITION_BY,
        "year(timestamp), month(timestamp), day(timestamp)"
    );
    assert!(!ONE_CLOCK_PARTITION_BY.contains("record_date"));
}
