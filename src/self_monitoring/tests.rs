//! Unit tests for self-monitoring helpers.

use crate::config::Config;
use crate::self_monitoring::{is_reserved_tenant_id, OPS_TENANT_ID};

#[test]
fn reserved_tenant_id_is_thelake_ops() {
    assert_eq!(OPS_TENANT_ID, "thelake-ops");
    assert!(is_reserved_tenant_id("thelake-ops"));
    assert!(is_reserved_tenant_id(" thelake-ops "));
    assert!(!is_reserved_tenant_id("softprobe-local"));
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
        maintenance_step::BACKLOG_PROBE,
        Some("traces"),
        Duration::from_millis(900),
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
            && engine_prod.contains("EXPIRE_SNAPSHOTS")
            && engine_prod.contains("ORPHAN_CLEANUP")
            && engine_prod.contains("PASS_TOTAL"),
        "physical-scope pass must time open/attach, metadata, and total"
    );
    let merge = include_str!("../compaction/merge.rs");
    let merge_prod = merge.split("#[cfg(test)]").next().expect("production");
    assert!(
        merge_prod.contains("record_maintenance_step")
            && merge_prod.contains("BACKLOG_PROBE")
            && merge_prod.contains("PARTITION_STATS")
            && merge_prod.contains("TWCS_CLOSED")
            && merge_prod.contains("TWCS_OPEN"),
        "TWCS path must time backlog probe, stats, and wave kinds"
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
