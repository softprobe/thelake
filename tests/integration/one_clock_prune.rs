//! One-clock production contract: real writers create calendar-day files and
//! timestamp-bounded recipes prune them without legacy date columns.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use chrono::{TimeZone, Utc};
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::{Log, Metric, Span, SpanEvent};
use softprobe_runtime::runtime_api::runtime_control_routes;
use softprobe_runtime::storage::ducklake::DuckLakeWriter;
use tempfile::TempDir;
use tower::ServiceExt;

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

fn attach(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");
    connection
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
        &format!(
            "SELECT column_type FROM (DESCRIBE softprobe.{table}) WHERE column_name = 'timestamp'"
        ),
        [],
        |row| row.get(0),
    )
    .unwrap_or_else(|e| panic!("timestamp type for {table}: {e}"))
}

fn metric(name: &str, timestamp: chrono::DateTime<Utc>, value: f64) -> Metric {
    Metric {
        metric_name: name.into(),
        description: "one-clock integration metric".into(),
        unit: "1".into(),
        metric_type: "gauge".into(),
        timestamp,
        value,
        attributes: HashMap::from([(String::from("job"), String::from("one-clock"))]),
        resource_attributes: HashMap::from([(
            String::from("service.name"),
            String::from("contract"),
        )]),
        ..Metric::default()
    }
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
        agent_id: None,
        agent_name: None,
    }
}

async fn build_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router.merge(runtime_control_routes().with_state(state))
}

async fn post_sql(router: &Router, sql: &str) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "sql": sql }).to_string()))
        .expect("sql request");
    router
        .clone()
        .oneshot(request)
        .await
        .expect("sql response")
        .status()
}

#[tokio::test]
async fn production_writers_partition_and_prune_one_clock_fact_tables() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    // Force every production write to publish a parquet file so EXPLAIN observes
    // physical day pruning rather than DuckLake catalog-inline rows.
    config.ducklake.data_inlining_row_limit = Some(0);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let writer = DuckLakeWriter::new(&config, None).await.expect("writer");

    let metrics = [
        metric(
            "persistent_metric",
            Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap(),
            1.0,
        ),
        metric(
            "persistent_metric",
            Utc.with_ymd_and_hms(2026, 2, 10, 12, 0, 0).unwrap(),
            2.0,
        ),
        metric(
            "persistent_metric",
            Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap(),
            3.0,
        ),
        metric(
            "persistent_metric",
            Utc.with_ymd_and_hms(2026, 9, 11, 12, 0, 0).unwrap(),
            4.0,
        ),
    ];
    writer
        .write_metric_batches(vec![metrics.to_vec()])
        .await
        .expect("metrics writer");
    writer
        .write_span_batches(vec![vec![span(10, "a"), span(11, "b")]])
        .await
        .expect("traces writer");
    writer
        .write_log_batches(vec![vec![log(10, "a"), log(11, "b")]])
        .await
        .expect("logs writer");

    let mut paths = Vec::new();
    walk_paths(Path::new(&data_path), &mut paths);
    let joined = paths.join("\n");
    for table in [
        "metric_samples",
        "metric_series",
        "metric_postings",
        "traces",
        "logs",
    ] {
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

    let conn = attach(&metadata_path, &data_path);
    assert_eq!(
        timestamp_type(&conn, "metric_samples"),
        "TIMESTAMP WITH TIME ZONE"
    );
    assert_eq!(
        timestamp_type(&conn, "metric_series"),
        "TIMESTAMP WITH TIME ZONE"
    );
    assert_eq!(timestamp_type(&conn, "traces"), "TIMESTAMP_NS");
    assert_eq!(timestamp_type(&conn, "logs"), "TIMESTAMP_NS");

    let series_days: i64 = conn
        .query_row(
            "SELECT count(*) FROM softprobe.metric_series \
             WHERE timestamp >= TIMESTAMPTZ '2026-09-10' AND timestamp < TIMESTAMPTZ '2026-09-12'",
            [],
            |row| row.get(0),
        )
        .expect("persistent series query");
    assert_eq!(
        series_days, 2,
        "series metadata must persist on both query days"
    );

    let narrow = "SELECT series_id, value FROM softprobe.metric_samples \
                  WHERE timestamp >= TIMESTAMPTZ '2026-09-10' \
                    AND timestamp < TIMESTAMPTZ '2026-09-11'";
    let wide = "SELECT series_id, value FROM softprobe.metric_samples \
                WHERE timestamp >= TIMESTAMPTZ '2026-09-10' \
                  AND timestamp < TIMESTAMPTZ '2026-09-12'";
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
    assert_eq!(
        conn.query_row(narrow, [], |row| row.get::<_, u64>(0))
            .unwrap_or_default(),
        conn.query_row(
            "SELECT series_id FROM softprobe.metric_samples WHERE timestamp = TIMESTAMPTZ '2026-09-10 12:00:00+00'",
            [],
            |row| row.get::<_, u64>(0),
        )
        .unwrap()
    );
    assert!(
        !flatten_plan(&narrow_plan).contains("day=11"),
        "narrow plan opened day B:\n{narrow_plan}"
    );
}

#[tokio::test]
async fn recipe_gate_covers_traces_logs_metrics_and_alias_expansion() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);
    let writer = DuckLakeWriter::new(&config, None).await.expect("writer");
    writer
        .write_metric_batches(vec![vec![metric(
            "gate_metric",
            Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap(),
            1.0,
        )]])
        .await
        .expect("metric seed");
    writer
        .write_span_batches(vec![vec![span(10, "gate")]])
        .await
        .expect("trace seed");
    writer
        .write_log_batches(vec![vec![log(10, "gate")]])
        .await
        .expect("log seed");

    let router = build_router(config).await;
    for sql in [
        "SELECT count(*) FROM traces",
        "SELECT count(*) FROM logs",
        "SELECT count(*) FROM metrics",
    ] {
        assert_eq!(
            post_sql(&router, sql).await,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{sql}"
        );
    }
    for sql in [
        "SELECT count(*) FROM traces WHERE timestamp >= '2026-09-10'::TIMESTAMP_NS",
        "SELECT count(*) FROM logs WHERE timestamp <= '2026-09-11'::TIMESTAMP_NS",
        "SELECT count(*) FROM metrics WHERE timestamp >= TIMESTAMPTZ '2026-09-10'",
    ] {
        assert_eq!(post_sql(&router, sql).await, StatusCode::OK, "{sql}");
    }
}

#[test]
fn locked_partition_expression_is_year_month_day_of_timestamp() {
    assert_eq!(
        ONE_CLOCK_PARTITION_BY,
        "year(timestamp), month(timestamp), day(timestamp)"
    );
    assert!(!ONE_CLOCK_PARTITION_BY.contains("record_date"));
}
