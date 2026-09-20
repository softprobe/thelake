//! AC6: one-day session fetch with D4 predicates does not open unrelated day files.

use chrono::{TimeZone, Utc};
use softprobe_runtime::api::llm::query::compile_session_observations_sql;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::Span as SpanData;
use softprobe_runtime::query;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;

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

#[tokio::test]
async fn one_day_session_fetch_does_not_list_unrelated_day_files() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    // Force Parquet so partition-day files exist on disk for EXPLAIN / path asserts.
    config.ducklake.data_inlining_row_limit = Some(0);

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine = query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
        .await
        .expect("query engine");

    let day_a = Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap();
    let day_b = Utc.with_ymd_and_hms(2026, 9, 11, 12, 0, 0).unwrap();
    let session = "sess-ac6-prune";

    let mut spans = Vec::new();
    for (i, ts) in [(0, day_a), (1, day_b)] {
        spans.push(SpanData {
            session_id: session.into(),
            trace_id: format!("tr-ac6-{i}"),
            span_id: format!("sp-ac6-{i}"),
            parent_span_id: None,
            app_id: "ac6".into(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "chat".into(),
            span_kind: Some("INTERNAL".into()),
            timestamp: ts,
            end_timestamp: Some(ts + chrono::Duration::seconds(1)),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".into()),
            status_message: None,
        });
    }
    pipeline
        .write_span_batches(vec![spans])
        .await
        .expect("write");

    let data_root = Path::new(&config.ducklake.data_path);
    let mut paths = Vec::new();
    walk_paths(data_root, &mut paths);
    assert!(
        !paths.is_empty(),
        "expected lake files under {:?}, got none",
        data_root
    );
    let all_paths = paths.join("\n");
    // Positive control: both partition days must exist as files/dirs before prune.
    assert!(
        all_paths.contains("2026-09-10"),
        "day-A partition path missing after write:\n{all_paths}"
    );
    assert!(
        all_paths.contains("2026-09-11"),
        "day-B partition path missing after write:\n{all_paths}"
    );

    let sql = compile_session_observations_sql(
        session,
        day_a,
        day_a + chrono::Duration::hours(1),
        50,
        None,
    )
    .expect("compile");
    assert!(sql.contains("record_date BETWEEN DATE '2026-09-10' AND DATE '2026-09-10'"));
    assert!(!sql.contains("DATE '2026-09-11'"));

    // Wide window: EXPLAIN (or ANALYZE) should still be able to see day-B in the plan/files.
    let wide_sql = compile_session_observations_sql(
        session,
        day_a,
        day_b + chrono::Duration::hours(1),
        50,
        None,
    )
    .expect("wide compile");
    let wide_explain = query_engine
        .execute_query(&format!("EXPLAIN ANALYZE {wide_sql}"))
        .await
        .expect("wide explain");
    let wide_plan = wide_explain
        .rows
        .iter()
        .flat_map(|row| row.iter().filter_map(|c| c.as_str()))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        wide_plan.contains("2026-09-11"),
        "positive control: wide EXPLAIN ANALYZE must list day-B (2026-09-11):\n{wide_plan}"
    );

    let explain = query_engine
        .execute_query(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("explain");
    let plan = explain
        .rows
        .iter()
        .flat_map(|row| row.iter().filter_map(|c| c.as_str()))
        .collect::<Vec<_>>()
        .join("\n");

    let rows = query_engine.execute_query(&sql).await.expect("query");
    assert_eq!(
        rows.row_count, 1,
        "one-day window must not return day-B span (got {})",
        rows.row_count
    );

    // Narrow window: day-B file path must not appear in the analyzed plan.
    assert!(
        !plan.contains("2026-09-11"),
        "EXPLAIN ANALYZE listed unrelated day 2026-09-11:\n{plan}"
    );
}
