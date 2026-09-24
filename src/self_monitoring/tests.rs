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
