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

/// Collapse EXPLAIN ASCII-art wrapping so parquet basenames stay searchable.
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

fn day_b_parquet_stem(paths: &[String]) -> String {
    let day_b = paths
        .iter()
        .find(|p| p.contains("record_date=2026-09-11") && p.ends_with(".parquet"))
        .unwrap_or_else(|| panic!("day-B parquet missing in paths:\n{}", paths.join("\n")));
    Path::new(day_b)
        .file_stem()
        .expect("stem")
        .to_string_lossy()
        .into_owned()
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
    assert!(
        all_paths.contains("record_date=2026-09-10"),
        "day-A partition path missing after write:\n{all_paths}"
    );
    assert!(
        all_paths.contains("record_date=2026-09-11"),
        "day-B partition path missing after write:\n{all_paths}"
    );
    // Day-B parquet basename cannot appear in SQL text (only in Files Read / Filename(s)).
    let day_b_stem = day_b_parquet_stem(&paths);
    assert!(
        day_b_stem.starts_with("ducklake-"),
        "unexpected day-B stem {day_b_stem}"
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
    assert!(!sql.contains(&day_b_stem));
    assert!(!sql.contains("DATE '2026-09-11'"));

    let wide_sql = compile_session_observations_sql(
        session,
        day_a,
        day_b + chrono::Duration::hours(1),
        50,
        None,
    )
    .expect("wide compile");
    assert!(!wide_sql.contains(&day_b_stem));

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
    let wide_flat = flatten_plan(&wide_plan);
    assert_eq!(
        files_read_count(&wide_plan),
        Some(2),
        "positive control: wide window must read both day files:\n{wide_plan}"
    );
    assert!(
        wide_flat.contains(&day_b_stem),
        "positive control: wide EXPLAIN ANALYZE Filename(s) must include day-B stem {day_b_stem}:\n{wide_plan}"
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
    let plan_flat = flatten_plan(&plan);

    let rows = query_engine.execute_query(&sql).await.expect("query");
    assert_eq!(
        rows.row_count, 1,
        "one-day window must not return day-B span (got {})",
        rows.row_count
    );

    assert_eq!(
        files_read_count(&plan),
        Some(1),
        "narrow window must read exactly one partition file:\n{plan}"
    );
    assert!(
        !plan_flat.contains(&day_b_stem),
        "EXPLAIN ANALYZE Filename(s) listed unrelated day-B file {day_b_stem}:\n{plan}"
    );
}
