use chrono::{TimeZone, Utc};
use softprobe_runtime::ingest::IngestEngine;
use softprobe_runtime::models::{Span, SpanEvent};
use softprobe_runtime::sql::insert_order_by;
use softprobe_runtime::sql::schema::OTLP_LAYOUT_SQL;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

const ROW_GROUPS_SQL: &str = include_str!("../fixtures/session_trace_parquet_row_groups.sql");
const SORTED_BY_SQL: &str = include_str!("../fixtures/session_trace_parquet_sorted_by.sql");
const LIVE_FILES_SQL: &str = include_str!("../fixtures/session_trace_parquet_live_files.sql");
const MERGE_SQL: &str = include_str!("../fixtures/session_trace_parquet_merge.sql");
const UNASSIGNED_WINDOW_EXPLAIN_SQL: &str =
    include_str!("../fixtures/session_trace_parquet_unassigned_window_explain.sql");
const SESSION_EXPLAIN_SQL: &str =
    include_str!("../fixtures/session_trace_parquet_session_explain.sql");

#[derive(Debug, Clone)]
struct RowGroupStats {
    row_group_id: i64,
    session_min: String,
    session_max: String,
    ts_min: String,
    ts_max: String,
    num_rows: i64,
    uncompressed_bytes: i64,
}

fn high_entropy_payload(seed: u64, bytes: usize) -> String {
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'!' + (state % 90) as u8) as char
        })
        .collect()
}

fn pseudo_random_trace_id(seed: u64) -> String {
    let mut a = seed.wrapping_mul(0xbf58_476d_1ce4_e5b9).wrapping_add(1);
    a ^= a >> 30;
    let mut b = a.wrapping_mul(0x94d0_49bb_1331_11eb);
    b ^= b >> 31;
    format!("{a:016x}{b:016x}")
}

fn make_span(session_id: &str, hour: u32, minute: u32, idx: usize, payload_bytes: usize) -> Span {
    let ts = Utc
        .with_ymd_and_hms(2026, 9, 15, hour, minute % 60, (idx % 60) as u32)
        .unwrap();
    let mut resource_attributes = HashMap::new();
    resource_attributes.insert(
        "wide_payload".to_string(),
        high_entropy_payload(idx as u64 + 1, payload_bytes),
    );
    Span {
        session_id: session_id.to_string(),
        trace_id: pseudo_random_trace_id(idx as u64 + 100),
        span_id: format!("{:016x}", idx + 1),
        parent_span_id: None,
        app_id: "layout-test".to_string(),
        organization_id: None,
        workspace_id: None,
        agent_id: Some("agt_layout".to_string()),
        agent_name: Some("checkout-api".to_string()),
        message_type: "SERVER".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: ts,
        end_timestamp: Some(ts + chrono::Duration::milliseconds(40)),
        attributes: HashMap::new(),
        resource_attributes,
        events: vec![SpanEvent {
            name: "handled".to_string(),
            timestamp: ts,
            attributes: HashMap::new(),
        }],
        status_code: Some("OK".to_string()),
        status_message: None,
        http_request_method: Some("POST".to_string()),
        http_request_path: Some("/api/checkout".to_string()),
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: Some(200),
        http_response_headers: None,
        http_response_body: None,
    }
}

fn walk_parquet_files(dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_parquet_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path.to_string_lossy().into_owned());
        }
    }
}

fn read_row_group_stats(conn: &duckdb::Connection, parquet_path: &str) -> Vec<RowGroupStats> {
    let mut stmt = conn.prepare(ROW_GROUPS_SQL).expect("prepare row_groups");
    stmt.query_map([parquet_path], |row| {
        Ok(RowGroupStats {
            row_group_id: row.get(0)?,
            session_min: row.get(1)?,
            session_max: row.get(2)?,
            ts_min: row.get(3)?,
            ts_max: row.get(4)?,
            num_rows: row.get(5)?,
            uncompressed_bytes: row.get(6)?,
        })
    })
    .expect("query row_groups")
    .collect::<Result<Vec<_>, _>>()
    .expect("collect row_groups")
}

#[test]
fn otlp_layout_profile_sorts_by_session_timestamp_trace() {
    let conn = duckdb::Connection::open_in_memory().expect("in-memory duckdb");
    conn.execute_batch(OTLP_LAYOUT_SQL)
        .expect("execute canonical otlp_layout.sql");
    let sorted_by: String = conn
        .query_row(SORTED_BY_SQL, [], |row| row.get(0))
        .expect("read thelake_otlp_sorted_by");
    assert_eq!(sorted_by, "session_id, timestamp, trace_id");
    for table in ["traces", "logs", "scores"] {
        assert_eq!(
            insert_order_by(table).trim_start_matches("ORDER BY "),
            "session_id, timestamp, trace_id",
            "table {table} writer order must follow canonical sort"
        );
    }
}

#[tokio::test]
async fn unassigned_and_assigned_sessions_preserve_parquet_row_group_locality_and_time_pruning() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = crate::util::config::file_backed_test_config(&temp);
    config.ingest.flush_interval_seconds = 0;
    config.ducklake.data_inlining_row_limit = Some(0);
    let data_path = config.ducklake.data_path.clone();
    let schema = config.ducklake.metadata_schema.clone();

    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");

    // DuckDB checks `parquet_row_group_size_bytes` (8 MiB) at 2,048-row vector boundaries,
    // so ingest 5,100 unassigned rows (~20.4 MiB across >2 vectors) and 2,200 assigned rows
    // (~8.8 MiB across >1 vector) with 4 KiB high-entropy payloads.
    let mut wave1 = Vec::new();
    let mut wave2 = Vec::new();

    for i in 0..5100usize {
        let hour = match i % 3 {
            0 => 9,
            1 => 12,
            _ => 15,
        };
        let span = make_span("", hour, ((i / 3) % 60) as u32, i, 4096);
        if i % 2 == 0 {
            wave1.push(span);
        } else {
            wave2.push(span);
        }
    }

    for i in 0..2200usize {
        let sid = format!("ses-{:03}", (i % 20) + 1);
        let hour = 10 + ((i % 4) as u32);
        let span = make_span(&sid, hour, ((i / 4) % 60) as u32, 20_000 + i, 4096);
        if i % 2 == 0 {
            wave1.push(span);
        } else {
            wave2.push(span);
        }
    }

    pipeline.add_spans(wave1, 0).await.expect("ingest wave 1");
    pipeline.add_spans(wave2, 0).await.expect("ingest wave 2");

    let conn =
        softprobe_runtime::storage::ducklake::open_attached_from_config(&config.ducklake, Some(0));
    let escaped_schema = schema.replace('\'', "''");
    conn.execute_batch(&MERGE_SQL.replace("{{schema}}", &escaped_schema))
        .expect("flush and merge adjacent files");

    let mut live_rel_paths = Vec::new();
    {
        let mut stmt = conn.prepare(LIVE_FILES_SQL).expect("prepare live files");
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query live files");
        for r in rows {
            live_rel_paths.push(r.expect("rel_path"));
        }
    }
    assert_eq!(
        live_rel_paths.len(),
        1,
        "compaction should merge same-day files under 128 MiB target into 1 live Parquet file, got {live_rel_paths:?}"
    );

    let mut all_parquet = Vec::new();
    walk_parquet_files(Path::new(&data_path), &mut all_parquet);
    let live_parquet = all_parquet
        .iter()
        .find(|p| p.ends_with(&live_rel_paths[0]))
        .expect("locate live compacted Parquet file on disk");

    let groups = read_row_group_stats(&conn, live_parquet);
    assert_eq!(
        groups.iter().map(|g| g.num_rows).sum::<i64>(),
        7300,
        "all 5100 unassigned and 2200 assigned spans must be preserved across compaction"
    );
    assert!(
        groups.len() >= 3,
        "expected >= 3 8-MiB row groups for ~30 MiB uncompressed payload, got {}: {groups:?}",
        groups.len()
    );
    for g in &groups {
        assert!(
            g.uncompressed_bytes <= 10 * 1024 * 1024,
            "row group {} exceeded 8 MiB target + 2048-row vector boundary: {} bytes",
            g.row_group_id,
            g.uncompressed_bytes
        );
    }

    let pure_unassigned: Vec<&RowGroupStats> = groups
        .iter()
        .filter(|g| g.session_min.is_empty() && g.session_max.is_empty())
        .collect();
    assert!(
        pure_unassigned.len() >= 2,
        "expected >= 2 pure unassigned row groups so timestamp zone-map separation is exercised, got {groups:?}"
    );

    for pair in groups.windows(2) {
        assert!(
            pair[0].session_max <= pair[1].session_min,
            "session_id zone maps must be monotonically non-decreasing across row groups: {:?} vs {:?}",
            pair[0],
            pair[1]
        );
    }

    let mut single_group_sessions = 0usize;
    for s in 1..=20 {
        let sid = format!("ses-{s:03}");
        let matching: Vec<i64> = groups
            .iter()
            .filter(|g| {
                g.session_min.as_str() <= sid.as_str() && sid.as_str() <= g.session_max.as_str()
            })
            .map(|g| g.row_group_id)
            .collect();
        assert!(
            (1..=2).contains(&matching.len()),
            "session {sid} must span 1 (or at most 2 adjacent boundary) row groups, got {matching:?} in {groups:?}"
        );
        if matching.len() == 2 {
            assert_eq!(
                matching[1] - matching[0],
                1,
                "boundary session {sid} row groups must be adjacent: {matching:?}"
            );
        } else {
            single_group_sessions += 1;
        }
    }
    assert!(
        single_group_sessions >= 18,
        "expected >= 18 of 20 sessions in 1 single row group, got {single_group_sessions} ({groups:?})"
    );

    for pair in pure_unassigned.windows(2) {
        assert!(
            pair[0].ts_max <= pair[1].ts_min,
            "unassigned row groups must sort by timestamp before trace_id so zone maps stay tight; got {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    assert_ne!(
        pure_unassigned.first().unwrap().ts_max,
        pure_unassigned.last().unwrap().ts_max,
        "unassigned row groups must not all span the entire 09:00..15:00 day window: {pure_unassigned:?}"
    );

    let groups_matching_15h = groups
        .iter()
        .filter(|g| g.session_min.is_empty() && g.ts_max.as_str() >= "2026-09-15 15:00:00")
        .count();
    assert!(
        groups_matching_15h < groups.len(),
        "narrow timestamp window on unassigned spans must prune earlier unassigned and assigned row groups ({groups_matching_15h} vs {})",
        groups.len()
    );

    let unassigned_plan: String = conn
        .prepare(&UNASSIGNED_WINDOW_EXPLAIN_SQL.replace("{{schema}}", &escaped_schema))
        .expect("prepare unassigned query plan")
        .query_map([], |r| r.get::<_, String>(1))
        .expect("run unassigned query plan")
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        unassigned_plan.contains("Total Files Read: 1"),
        "expected 1 live file read in unassigned plan:\n{unassigned_plan}"
    );

    let session_plan: String = conn
        .prepare(&SESSION_EXPLAIN_SQL.replace("{{schema}}", &escaped_schema))
        .expect("prepare session query plan")
        .query_map([], |r| r.get::<_, String>(1))
        .expect("run session query plan")
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        session_plan.contains("Total Files Read: 1"),
        "expected 1 live file read in session plan:\n{session_plan}"
    );
}
