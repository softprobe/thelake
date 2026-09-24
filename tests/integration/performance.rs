use anyhow::Result;
use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::Log as LogData;
use std::collections::HashMap;
use std::time::Instant;

use crate::util::pipeline::TestPipeline;
use crate::util::storage_config::load_test_config;

// =============================================================================
// Performance quality bar (do not raise PERF_TARGET_MS to hide regressions)
//
// Asserted here (latency + concurrency):
//   Warm logs COUNT(*) p95  <  PERF_TARGET_MS   (default 1000 ms / 1s)
//   Latency:      5 timed iterations after warmup
//   Concurrency:  one timed query per worker (N = PERF_CONCURRENCY)
//
// Workload defaults:
//   PERF_EVENTS_PER_SESSION = 1000   rows per base/staged/buffer session
//   PERF_CONCURRENCY        = 8      parallel workers (concurrency test)
//   PERF_TARGET_MS          = 1000   warm p95 latency ceiling
//   Makefile exports: PERF_TARGET_MS, PERF_CONCURRENCY, PERF_EVENTS_PER_SESSION
//
// Suite wall-clock SLO lives in the Makefile, not here:
//   make test-perf  ≤ 480s  (ENFORCE_SLO=1 / CI=true)
//   See Makefile header + .github/workflows/performance.yml
// =============================================================================

/// Wide one-clock bound so warm COUNT(*) queries satisfy D12 without day filters.
const PERF_TS_BOUND: &str = "make_timestamp_ns(epoch_ns(timestamp)) >= '1970-01-01'::TIMESTAMP_NS \
     AND make_timestamp_ns(epoch_ns(timestamp)) <= '2100-01-01'::TIMESTAMP_NS";

fn load_perf_config() -> Config {
    if let Ok(config_file) = std::env::var("PERF_CONFIG_FILE") {
        std::env::set_var("CONFIG_FILE", &config_file);
    }
    let mut config = load_test_config();
    // Local e2e infra is MinIO/GCS data + DuckLake Postgres. Concurrent query workers contend on
    // SQLite metadata (`database is locked`); prefer Postgres whenever the local catalog is up.
    let backend = std::env::var("E2E_BACKEND").unwrap_or_else(|_| "local".to_string());
    if backend == "local" || backend == "gcs" {
        config.ducklake.metadata_path =
            "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    }
    config
}

/// Warm logs p95 must stay under this duration (default **1s**).
/// Override with `PERF_TARGET_MS`; do not raise the default to mask regressions.
fn perf_target() -> std::time::Duration {
    if let Ok(target_ms) = std::env::var("PERF_TARGET_MS") {
        let ms = target_ms.parse::<u64>().unwrap_or(1000);
        return std::time::Duration::from_millis(ms);
    }
    std::time::Duration::from_millis(1000)
}

fn diagnostics_enabled() -> bool {
    std::env::var("PERF_DIAG").ok().as_deref() == Some("1")
}

fn profile_enabled() -> bool {
    std::env::var("PERF_CACHE_PROFILE").ok().as_deref() == Some("1")
}

fn format_result(result: &softprobe_runtime::storage::duckdb::QueryResult) -> String {
    let mut output = String::new();
    output.push_str(&format!("columns: {:?}\n", result.columns));
    for row in &result.rows {
        output.push_str(&format!("row: {:?}\n", row));
    }
    output
}

async fn maybe_log_cache_profile(engine: &softprobe_runtime::query::QueryEngine, label: &str) {
    if !profile_enabled() {
        return;
    }
    println!("cache_httpfs profile ({label})");
    if let Ok(result) = engine
        .execute_query("SELECT cache_httpfs_get_profile();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query(
            "SELECT name, value FROM duckdb_settings() WHERE name LIKE 'cache_httpfs_%';",
        )
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_get_cache_filesystems();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_cache_status_query();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_cache_access_info_query();")
        .await
    {
        println!("{}", format_result(&result));
    }
}

async fn explain_analyze(engine: &softprobe_runtime::query::QueryEngine, label: &str, sql: &str) {
    let explain_sql = format!("EXPLAIN ANALYZE {sql}");
    match engine.execute_query(&explain_sql).await {
        Ok(result) => {
            println!("EXPLAIN ANALYZE ({label})");
            println!("{}", format_result(&result));
        }
        Err(err) => {
            println!("EXPLAIN ANALYZE ({label}) failed: {err}");
        }
    }
}

async fn run_diagnostics(engine: &softprobe_runtime::query::QueryEngine, label: &str, sql: &str) {
    let start = Instant::now();
    let result = engine.execute_query(sql).await;
    let duration = start.elapsed();
    match result {
        Ok(result) => {
            println!(
                "DIAG {label}: duration={:?}, rows={}",
                duration, result.row_count
            );
        }
        Err(err) => {
            println!("DIAG {label}: failed after {:?}: {}", duration, err);
        }
    }
    explain_analyze(engine, label, sql).await;
}

/// Retry a query with exponential backoff to handle R2 eventual consistency
/// Returns the query result once it succeeds, or the last error after max retries
async fn retry_query_until_count(
    engine: &softprobe_runtime::query::QueryEngine,
    sql: &str,
    expected_count: i64,
    max_retries: u32,
) -> Result<softprobe_runtime::storage::duckdb::QueryResult, anyhow::Error> {
    let is_r2 = std::env::var("E2E_BACKEND").ok().as_deref() == Some("r2");
    let max_retries = if is_r2 {
        max_retries.max(10)
    } else {
        max_retries
    };

    for attempt in 0..max_retries {
        match engine.execute_query(sql).await {
            Ok(result) => {
                let count = result.rows[0][0].as_i64().unwrap_or(0);
                if count == expected_count {
                    return Ok(result);
                }
                if attempt < max_retries - 1 {
                    let delay_ms = 100 * (1 << attempt.min(5)); // Exponential backoff, max 3.2s
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
            Err(e) => {
                if attempt < max_retries - 1 {
                    let delay_ms = 100 * (1 << attempt.min(5));
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                } else {
                    return Err(anyhow::anyhow!(
                        "Query failed after {} retries: {}",
                        max_retries,
                        e
                    ));
                }
            }
        }
    }

    // Final attempt
    let result = engine.execute_query(sql).await?;
    let count = result.rows[0][0].as_i64().unwrap_or(0);
    if count == expected_count {
        Ok(result)
    } else {
        Err(anyhow::anyhow!(
            "Query returned {} rows, expected {} after {} retries",
            count,
            expected_count,
            max_retries
        ))
    }
}

/// Single-client warm read: p95 of 5 timed `logs` queries < `PERF_TARGET_MS` (1s).
#[tokio::test]
async fn perf_union_read_latency() {
    let mut config = load_perf_config();
    if std::env::var("PERF_FORCE_SINGLE_WORKER").ok().as_deref() == Some("1") {
        config.query.max_connections = 1;
    }

    let warmup_workers = std::cmp::max(1, config.query.max_connections);
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let per_session = std::env::var("PERF_EVENTS_PER_SESSION")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);

    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("perf-buffer-{}", uuid::Uuid::new_v4());

    let mut base_logs = Vec::new();
    for i in 0..per_session {
        base_logs.push(LogData {
            session_id: Some(base_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Base log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(base_logs, per_session * 256)
        .await
        .expect("base ingest");
    pipeline.force_flush_logs().await.expect("base flush");

    let mut staged_logs = Vec::new();
    for i in 0..per_session {
        staged_logs.push(LogData {
            session_id: Some(staged_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Staged log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(staged_logs, per_session * 256)
        .await
        .expect("stage add");
    pipeline.force_flush_logs().await.expect("stage flush");
    let mut buffer_logs = Vec::new();
    for i in 0..per_session {
        buffer_logs.push(LogData {
            session_id: Some(buffer_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Buffer log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(buffer_logs, per_session * 256)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();
    maybe_log_cache_profile(&query_engine, "before_warmup").await;
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        staged_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_iceberg_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        base_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    // Retry query to handle R2 eventual consistency after the base ingest flush.
    let warmup =
        retry_query_until_count(&query_engine, &warmup_iceberg_sql, per_session as i64, 15)
            .await
            .expect("iceberg warmup should eventually return data");
    assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);

    // Now run additional warmup queries (data should be visible now)
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_iceberg_sql)
            .await
            .expect("iceberg warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_iceberg_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        base_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_iceberg_sql)
            .await
            .expect("iceberg warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        buffer_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        buffer_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..2 {
        let warm = query_engine.execute_query(&sql).await.expect("warmup");
        assert_eq!(warm.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let iterations = 5;
    let mut durations = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let result = query_engine.execute_query(&sql).await.expect("query");
        let duration = start.elapsed();
        let count = result.rows[0][0].as_i64().unwrap_or(0);
        assert_eq!(count, per_session as i64);
        durations.push(duration);
    }
    durations.sort();
    let p95_idx = ((durations.len() as f64) * 0.95).ceil() as usize - 1;
    let p95 = durations[p95_idx.min(durations.len() - 1)];
    maybe_log_cache_profile(&query_engine, "after_latency").await;
    if diagnostics_enabled() {
        let base_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            base_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        let staged_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            staged_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        run_diagnostics(&query_engine, "logs", &base_sql).await;
        run_diagnostics(&query_engine, "logs_staged_session", &staged_sql).await;
        let buffer_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            buffer_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        run_diagnostics(&query_engine, "logs_buffer_session", &buffer_sql).await;
        run_diagnostics(&query_engine, "logs", &sql).await;
    }
    // Quality bar: warm p95 < 1s (PERF_TARGET_MS).
    println!("p95 warm logs latency: {:?}", p95);
    assert!(
        p95 < perf_target(),
        "Expected p95 warm query under {:?} (PERF_TARGET_MS), got {:?}",
        perf_target(),
        p95
    );
}

/// Concurrent warm reads (`PERF_CONCURRENCY` workers): same p95 < `PERF_TARGET_MS` (1s) bar.
#[tokio::test]
async fn perf_union_read_concurrency() {
    let mut config = load_perf_config();
    if std::env::var("PERF_FORCE_SINGLE_WORKER").ok().as_deref() == Some("1") {
        config.query.max_connections = 1;
    }

    let warmup_workers = std::cmp::max(1, config.query.max_connections);
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let per_session = std::env::var("PERF_EVENTS_PER_SESSION")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let concurrency = std::env::var("PERF_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);

    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("perf-buffer-{}", uuid::Uuid::new_v4());

    let mut base_logs = Vec::new();
    for i in 0..per_session {
        base_logs.push(LogData {
            session_id: Some(base_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Base log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(base_logs, per_session * 256)
        .await
        .expect("base ingest");
    pipeline.force_flush_logs().await.expect("base flush");

    let mut staged_logs = Vec::new();
    for i in 0..per_session {
        staged_logs.push(LogData {
            session_id: Some(staged_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Staged log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(staged_logs, per_session * 256)
        .await
        .expect("stage add");
    pipeline.force_flush_logs().await.expect("stage flush");
    let mut buffer_logs = Vec::new();
    for i in 0..per_session {
        buffer_logs.push(LogData {
            session_id: Some(buffer_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Buffer log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(buffer_logs, per_session * 256)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();
    maybe_log_cache_profile(&query_engine, "before_warmup").await;
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        staged_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        base_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    // Retry query to handle R2 eventual consistency after the base ingest flush.
    let warmup = retry_query_until_count(&query_engine, &warmup_sql, per_session as i64, 15)
        .await
        .expect("warmup should eventually return data");
    assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);

    // Now run additional warmup queries (data should be visible now)
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM logs \
         WHERE session_id = '{}' AND {}",
        buffer_session.replace('\'', "''"),
        PERF_TS_BOUND,
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let sessions = vec![
        base_session.clone(),
        staged_session.clone(),
        buffer_session.clone(),
    ];
    let mut handles = Vec::new();
    for i in 0..concurrency {
        let engine = query_engine.clone();
        let session_id = sessions[i % sessions.len()].clone();
        let date_filter = PERF_TS_BOUND;
        handles.push(tokio::spawn(async move {
            let sql = format!(
                "SELECT COUNT(*) AS count FROM logs \
                 WHERE session_id = '{}' AND {}",
                session_id.replace('\'', "''"),
                date_filter,
            );
            let _ = engine.execute_query(&sql).await.expect("warmup");
            let start = Instant::now();
            let result = engine.execute_query(&sql).await.expect("query");
            let duration = start.elapsed();
            let count = result.rows[0][0].as_i64().unwrap_or(0);
            (duration, count)
        }));
    }

    let mut durations = Vec::new();
    for handle in handles {
        let (duration, count) = handle.await.expect("task");
        assert_eq!(count, per_session as i64, "Expected {} rows", per_session);
        durations.push(duration);
    }
    durations.sort();
    let p95_idx = ((durations.len() as f64) * 0.95).ceil() as usize - 1;
    let p95 = durations[p95_idx.min(durations.len() - 1)];
    maybe_log_cache_profile(&query_engine, "after_concurrency").await;
    if diagnostics_enabled() {
        let base_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            base_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        let staged_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            staged_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        let buffer_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            buffer_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        let union_sql = format!(
            "SELECT COUNT(*) AS count FROM logs \
             WHERE session_id = '{}' AND {}",
            buffer_session.replace('\'', "''"),
            PERF_TS_BOUND,
        );
        run_diagnostics(&query_engine, "logs", &base_sql).await;
        run_diagnostics(&query_engine, "logs_staged_session", &staged_sql).await;
        run_diagnostics(&query_engine, "logs_buffer_session", &buffer_sql).await;
        run_diagnostics(&query_engine, "logs", &union_sql).await;
    }
    // Quality bar: warm p95 < 1s under concurrent load (same PERF_TARGET_MS).
    println!("p95 warm logs latency: {:?}", p95);
    assert!(
        p95 < perf_target(),
        "Expected p95 warm query under {:?} (PERF_TARGET_MS), got {:?}",
        perf_target(),
        p95
    );
}
