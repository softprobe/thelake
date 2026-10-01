//! One-clock production contract: real writers create calendar-day files and
//! bare timestamp predicates prune them without legacy date columns.

use chrono::{TimeZone, Utc};
use softprobe_runtime::ingest_engine::IngestEngine;
use softprobe_runtime::models::{Log, Score, ScoreDataType, ScoreSource, Span, SpanEvent};
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
        workspace_id: None,
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
        workspace_id: None,
        agent_id: None,
        agent_name: None,
    }
}

fn score(day: u32, id: &str) -> Score {
    Score {
        score_id: format!("score-{id}"),
        timestamp: Utc.with_ymd_and_hms(2026, 9, day, 12, 0, 0).unwrap(),
        trace_id: Some(format!("trace-{id}")),
        span_id: Some(format!("span-{id}")),
        session_id: Some("persistent-session".into()),
        name: "quality".into(),
        data_type: ScoreDataType::Numeric,
        numeric_value: Some(0.9),
        string_value: None,
        boolean_value: None,
        source: ScoreSource::Evaluator,
        comment: None,
        config_id: None,
        author_id: None,
        metadata: HashMap::new(),
        workspace_id: None,
    }
}

fn high_entropy_payload(seed: u64, bytes: usize) -> String {
    let mut state = seed;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'!' + (state % 90) as u8) as char
        })
        .collect()
}

#[tokio::test]
async fn production_writers_partition_and_prune_one_clock_fact_tables() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ingest.flush_interval_seconds = 0;
    // Keep the shared 500-row inlining setting and exceed it to publish files.
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");

    pipeline
        .add_spans(
            (0..501)
                .map(|i| span(if i < 251 { 10 } else { 11 }, &format!("trace-{i}")))
                .collect(),
            0,
        )
        .await
        .expect("traces ingest");
    pipeline
        .add_logs(
            (0..501)
                .map(|i| log(if i < 251 { 10 } else { 11 }, &format!("log-{i}")))
                .collect(),
            0,
        )
        .await
        .expect("logs ingest");
    pipeline
        .add_scores(
            (0..501)
                .map(|i| score(if i < 251 { 10 } else { 11 }, &format!("score-{i}")))
                .collect(),
        )
        .await
        .expect("scores ingest");

    let mut paths = Vec::new();
    walk_paths(Path::new(&data_path), &mut paths);
    let joined = paths.join("\n");
    for table in ["traces", "logs", "scores"] {
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
    assert_eq!(timestamp_type(&conn, "scores"), "TIMESTAMP_NS");

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

    let narrow_scores = "SELECT score_id FROM scores \
                         WHERE timestamp >= TIMESTAMP_NS '2026-09-10 00:00:00' \
                           AND timestamp < TIMESTAMP_NS '2026-09-11 00:00:00'";
    let score_plan = explain_plan(&conn, narrow_scores);
    assert_eq!(
        files_read_count(&score_plan),
        Some(1),
        "bare TIMESTAMP_NS predicate must prune score day partitions:\n{score_plan}"
    );
    assert!(
        !flatten_plan(&score_plan).contains("day=11"),
        "score plan opened day B:\n{score_plan}"
    );

    // Product QueryWindow shape must prune identically to bare (design law).
    let product = softprobe_runtime::sql::QueryWindow::try_new(
        Utc.with_ymd_and_hms(2026, 9, 10, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 10, 23, 59, 59).unwrap(),
    )
    .unwrap()
    .scan_with_timestamp_filter("", |bound| {
        format!("SELECT trace_id FROM traces WHERE {bound}")
    })
    .into_sql();
    assert!(
        !product.contains("make_timestamp_ns(epoch_ns("),
        "QueryWindow must not wrap timestamp: {product}"
    );
    let product_plan = explain_plan(&conn, &product);
    assert_eq!(
        files_read_count(&product_plan),
        Some(1),
        "product QueryWindow bound must day-prune:\n{product_plan}"
    );
    assert!(
        !flatten_plan(&product_plan).contains("day=11"),
        "product bound opened day B:\n{product_plan}"
    );

    // Locked anti-pattern: epoch_ns wrap disables prune (prod-proven).
    let wrapped = "SELECT trace_id FROM traces \
         WHERE make_timestamp_ns(epoch_ns(timestamp)) >= '2026-09-10'::TIMESTAMP_NS \
           AND make_timestamp_ns(epoch_ns(timestamp)) < '2026-09-11'::TIMESTAMP_NS";
    let wrapped_plan = explain_plan(&conn, wrapped);
    let wrapped_files = files_read_count(&wrapped_plan).expect("wrapped files");
    assert!(
        wrapped_files > 1,
        "expected wrapped bound to fail day prune (files={wrapped_files}):\n{wrapped_plan}"
    );
}

#[tokio::test]
async fn production_writers_apply_byte_row_group_target_to_traces_logs_and_scores() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ingest.flush_interval_seconds = 0;
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");

    let spans = (0..2501)
        .map(|i| {
            let mut row = span(12, &format!("wide-{i}"));
            row.session_id = format!("session-{:03}", i % 31);
            row.resource_attributes.insert(
                "wide_payload".into(),
                high_entropy_payload(i as u64 + 1, 4096),
            );
            row
        })
        .collect();
    pipeline
        .add_spans(spans, 0)
        .await
        .expect("wide traces ingest");

    let logs = (0..2501)
        .map(|i| {
            let mut row = log(12, &format!("wide-{i}"));
            row.session_id = Some(format!("session-{:03}", i % 31));
            row.body = high_entropy_payload(i as u64 + 11, 4096);
            row
        })
        .collect();
    pipeline.add_logs(logs, 0).await.expect("wide logs ingest");

    let scores = (0..2501)
        .map(|i| {
            let mut row = score(12, &format!("wide-{i}"));
            row.session_id = Some(format!("session-{:03}", i % 31));
            row.comment = Some(high_entropy_payload(i as u64 + 21, 4096));
            row
        })
        .collect();
    pipeline
        .add_scores(scores)
        .await
        .expect("wide scores ingest");

    let mut paths = Vec::new();
    walk_paths(Path::new(&data_path), &mut paths);
    let conn = attach(&config.ducklake);
    for table in ["traces", "logs", "scores"] {
        let table_files = paths
            .iter()
            .filter(|path| {
                path.contains(&format!("/{table}/year=2026/month=9/day=12/"))
                    && path.ends_with(".parquet")
            })
            .collect::<Vec<_>>();
        assert!(!table_files.is_empty(), "no Parquet files for {table}");
        let metadata = table_files
            .iter()
            .map(|path| {
                conn.query_row(
                    "SELECT count(*), max(uncompressed_bytes) FROM ( \
                       SELECT row_group_id, sum(total_uncompressed_size) AS uncompressed_bytes \
                       FROM parquet_metadata(?) GROUP BY row_group_id)",
                    [path.as_str()],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap_or_else(|error| panic!("read {table} Parquet metadata: {error}"))
            })
            .collect::<Vec<_>>();
        let groups = metadata.iter().map(|item| item.0).sum::<i64>();
        let largest_group_bytes = metadata.iter().map(|item| item.1).max().unwrap_or(0);
        assert!(
            groups > 1,
            "{table}: 10+ MiB of payload stayed in {groups} row group(s); 8 MiB byte target was not applied"
        );
        assert!(
            largest_group_bytes <= 9 * 1024 * 1024,
            "{table}: largest row group is {largest_group_bytes} uncompressed bytes; expected the 8 MiB target plus a small row-boundary overshoot"
        );
    }

    // Make a collector-sized batch inline, then explicitly materialize it.
    // The larger test-only inline threshold isolates the flush writer while
    // leaving the Parquet profile itself unchanged.
    let schema = config.ducklake.metadata_schema.clone();
    conn.execute_batch(&format!(
        "CALL softprobe.set_option('data_inlining_row_limit', 3000, schema => '{schema}', table_name => 'traces');\
         SET preserve_insertion_order = false;"
    ))
    .expect("configure inlined flush fixture");
    conn.execute_batch(&format!(
        "INSERT INTO softprobe.{schema}.traces \
         (session_id, trace_id, span_id, app_id, message_type, timestamp, resource_attributes) \
         SELECT 'inline-session-' || lpad((i % 31)::VARCHAR, 3, '0'), \
                'inline-trace-' || i::VARCHAR, 'inline-span-' || i::VARCHAR, \
                'inline-test', 'INTERNAL', TIMESTAMP_NS '2026-09-13 12:00:00', \
                MAP {{'wide_payload': repeat(md5(i::VARCHAR), 128)}} \
         FROM range(2501) t(i) ORDER BY 1, 2, 6;"
    ))
    .expect("write inlined trace batch");

    let mut paths = Vec::new();
    walk_paths(Path::new(&data_path), &mut paths);
    assert!(
        !paths.iter().any(|path| {
            path.contains("/traces/year=2026/month=9/day=13/") && path.ends_with(".parquet")
        }),
        "trace rows should still be inline before flush"
    );
    let escaped_schema = schema.replace('\'', "''");
    conn.execute_batch(&format!(
        "CALL ducklake_flush_inlined_data('softprobe', schema_name => '{escaped_schema}', table_name => 'traces');"
    ))
    .expect("flush inlined traces");
    paths.clear();
    walk_paths(Path::new(&data_path), &mut paths);
    let inline_files = paths
        .iter()
        .filter(|path| {
            path.contains("/traces/year=2026/month=9/day=13/") && path.ends_with(".parquet")
        })
        .collect::<Vec<_>>();
    assert!(!inline_files.is_empty(), "flush did not materialize traces");
    let inline_metadata = inline_files
        .iter()
        .map(|path| {
            conn.query_row(
                "SELECT count(*), max(uncompressed_bytes) FROM ( \
                   SELECT row_group_id, sum(total_uncompressed_size) AS uncompressed_bytes \
                   FROM parquet_metadata(?) GROUP BY row_group_id)",
                [path.as_str()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .expect("read flushed trace metadata")
        })
        .collect::<Vec<_>>();
    let inline_groups = inline_metadata.iter().map(|item| item.0).sum::<i64>();
    let inline_largest_group_bytes = inline_metadata.iter().map(|item| item.1).max().unwrap_or(0);
    assert!(
        inline_groups > 1,
        "inline flush wrote 10+ MiB in {inline_groups} row group(s); expected the shared 8 MiB target"
    );
    assert!(
        inline_largest_group_bytes <= 9 * 1024 * 1024,
        "inline flush wrote a {inline_largest_group_bytes}-byte uncompressed row group"
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
                time_window: crate::util::query_window(),

                app_id: None,
                span_id: None,
            })
            .await
            .expect("trace count"),
        1
    );
    assert_eq!(
        query
            .count_logs(LogCountFilter {
                session_id: Some("persistent-session".into()),
                time_window: crate::util::query_window(),

                body: None,
                trace_id: None,
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
