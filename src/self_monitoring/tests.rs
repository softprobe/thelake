//! Unit tests for self-monitoring helpers.

use crate::config::Config;
use crate::self_monitoring::{is_reserved_workspace_id, OPS_TENANT_ID};

#[test]
fn reserved_workspace_id_is_thelake_ops() {
    assert_eq!(OPS_TENANT_ID, "thelake-ops");
    assert!(is_reserved_workspace_id("thelake-ops"));
    assert!(is_reserved_workspace_id(" thelake-ops "));
    assert!(!is_reserved_workspace_id("softprobe-local"));
}

#[test]
fn self_monitoring_config_defaults_disabled() {
    let c = Config::default();
    assert!(!c.self_monitoring.enabled);
    assert_eq!(c.self_monitoring.export_interval_seconds, 60);
}

#[test]
fn self_monitoring_yaml_parses() {
    let yaml = r#"
ducklake:
  metadata_path: /tmp/meta.sqlite
  data_path: /tmp/data/
self_monitoring:
  enabled: true
  export_interval_seconds: 15
"#;
    let c: Config = serde_yaml::from_str(yaml).expect("parse");
    assert!(c.self_monitoring.enabled);
    assert_eq!(c.self_monitoring.export_interval_seconds, 15);
}

#[test]
fn otlp_metrics_exporter_builds_with_defaults() {
    super::export::try_build_otlp_exporter().expect("otlp exporter builds");
}

#[tokio::test]
async fn health_stays_ok_when_only_export_drops_rise() {
    use crate::api::health::health_check;
    use crate::self_monitoring::instruments::ensure_noop_instruments_for_test;
    use crate::self_monitoring::record_export_drop;
    use crate::storage::duckdb;
    use axum::http::StatusCode;

    ensure_noop_instruments_for_test();
    duckdb::set_self_heal_failures_for_test(0);
    record_export_drop();
    let (status, j) = health_check().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(j.0["status"], "ok");
    assert!(j.0["exportDrops"].as_u64().unwrap_or(0) >= 1);
}

#[test]
fn bottleneck_duration_recorders_accept_bounded_labels() {
    use crate::self_monitoring::instruments::ensure_noop_instruments_for_test;
    use crate::self_monitoring::{
        maintenance_step, record_ingest_commit, record_job_duration, record_maintenance_step,
        record_session_summary_dirty_upsert, record_session_summary_reduce_step, reduce_step,
        set_async_jobs_wake_ms,
    };
    use std::time::Duration;

    ensure_noop_instruments_for_test();
    set_async_jobs_wake_ms(10_000);
    record_job_duration(
        "physical_scope_maintenance",
        "scope-a",
        "ok",
        Duration::from_millis(1400),
    );
    record_maintenance_step(
        "scope-a",
        maintenance_step::PASS_TOTAL,
        Some("traces"),
        Duration::from_millis(900),
    );
    crate::self_monitoring::record_session_detail_stage(
        "tenant-a",
        crate::self_monitoring::session_detail_stage::LAKE_SQL,
        Duration::from_millis(1700),
    );
    crate::self_monitoring::record_query_stage(
        "tenant-a",
        "session_detail",
        crate::self_monitoring::query_stage::SQL_GATE,
        Duration::from_millis(200),
    );
    crate::self_monitoring::record_query_stage(
        "tenant-a",
        "session_detail",
        crate::self_monitoring::query_stage::SQL_EXEC,
        Duration::from_millis(1500),
    );
    record_maintenance_step(
        "scope-a",
        maintenance_step::PASS_TOTAL,
        None,
        Duration::from_millis(14000),
    );
    record_ingest_commit("t", "traces", 10, true, Duration::from_millis(50));
    record_session_summary_reduce_step("t", reduce_step::AGGREGATE, Duration::from_millis(200));
    record_session_summary_dirty_upsert("t", Duration::from_millis(5));
}

#[test]
fn async_jobs_runner_records_job_duration() {
    let src = include_str!("../async_jobs/mod.rs");
    assert!(
        src.contains("record_job_duration") && src.contains("set_async_jobs_wake_ms"),
        "shared runner must record job duration and configured wake_ms"
    );
}

#[test]
fn maintenance_pass_records_step_durations() {
    let engine = include_str!("../compaction/engine.rs");
    let engine_prod = engine.split("#[cfg(test)]").next().expect("production");
    assert!(
        engine_prod.contains("record_maintenance_step")
            && engine_prod.contains("OPEN_ATTACH")
            && engine_prod.contains("PASS_TOTAL"),
        "physical-scope pass must time script invocation"
    );
    let maintenance_sql = include_str!("../sql/maintenance/maintenance.sql");
    assert!(
        maintenance_sql.contains("ducklake_expire_snapshots")
            && maintenance_sql.contains("ducklake_cleanup_old_files")
            && !maintenance_sql.contains("ducklake_delete_orphaned_files"),
        "snapshot and scheduled-file cleanup must be SQL-owned"
    );
}

#[test]
fn ingest_and_session_summary_record_commit_and_reduce_durations() {
    let ingest = include_str!("../ingest_engine/mod.rs");
    assert!(
        ingest.contains("commit_started") && ingest.contains("record_ingest_commit"),
        "ingest commit path must time DuckLake writes"
    );
    let reduce = include_str!("../session_summary/reduce.rs");
    assert!(
        reduce.contains("record_session_summary_reduce_step")
            && reduce.contains("reduce_step::CLAIM")
            && reduce.contains("reduce_step::AGGREGATE")
            && reduce.contains("reduce_step::UPSERT")
            && reduce.contains("reduce_step::ACK")
            && reduce.contains("reduce_step::TOTAL"),
        "reduce_tenant must time claim/aggregate/upsert/ack/total"
    );
    let dirty = include_str!("../session_summary/dirty.rs");
    assert!(
        dirty.contains("record_session_summary_dirty_upsert")
            && dirty.contains("started.elapsed()"),
        "dirty UPSERT must record duration"
    );
}

#[test]
fn query_worker_splits_sql_gate_from_sql_exec() {
    let engine = include_str!("../storage/duckdb/engine.rs");
    assert!(
        engine.contains("struct TimedExecute")
            && engine.contains("gate_elapsed")
            && engine.contains("run_elapsed")
            && engine.contains("query_stage::SQL_GATE")
            && engine.contains("query_stage::SQL_EXEC")
            && engine.contains("sql_gate_ms")
            && engine.contains("sql_exec_ms"),
        "worker must time EXPLAIN gate vs run and log both on slow queries"
    );
}

#[test]
fn session_detail_records_stages_before_question_mark() {
    let query = include_str!("../api/llm/query.rs");
    let get_session = query
        .split("pub async fn get_session(")
        .nth(1)
        .expect("get_session")
        .split("pub async fn")
        .next()
        .expect("get_session body");
    let pg_rec = get_session
        .find("session_detail_stage::PG_WINDOW")
        .expect("pg_window record");
    let pg_q = get_session.find("window?").expect("window?");
    assert!(
        pg_rec < pg_q,
        "pg_window stage must record before window? so failures still emit"
    );
    let lake_rec = get_session
        .find("session_detail_stage::LAKE_SQL")
        .expect("lake_sql record");
    let lake_map = get_session
        .find("lake.map_err(storage_error)")
        .expect("lake.map_err");
    assert!(
        lake_rec < lake_map,
        "lake_sql stage must record before map_err so failures still emit"
    );
}
