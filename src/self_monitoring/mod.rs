//! thelake self-monitoring: OTel Meter API + standard OTLP metrics export.

mod export;
pub mod gauge_store;
mod ids;
mod instruments;
mod inventory;
mod labels;
mod size_bucket;

#[cfg(test)]
mod tests;

pub use ids::{instrument_customer_tenant, is_reserved_tenant_id, OPS_TENANT_ID};
pub use instruments::{
    maintenance_step, record_compaction_pass, record_compaction_wave, record_export_drop,
    record_ingest, record_ingest_commit, record_job_duration, record_job_error,
    record_lease_acquire, record_lease_heartbeat_failure, record_lease_steal, record_maintenance,
    record_maintenance_step, record_orphan_remove, record_query, record_query_queue_wait,
    record_sample_scan, record_session_summary_dirty_upsert,
    record_session_summary_dirty_upsert_error, record_session_summary_reduce_step,
    record_session_summary_reducer_lag, record_session_summary_sessions_reduced, record_slow_query,
    record_snapshot_expire, record_write, reduce_step, self_monitoring_export_drops,
    set_async_jobs_wake_ms, set_session_summary_dirty_depth,
};
pub use labels::{bound_app, classify_sql_kind};

use crate::api::AppState;
use crate::config::Config;
use std::sync::Arc;
use tracing::info;

/// Best-effort: start OTel PeriodicReader → OTLP metrics exporter.
pub async fn bootstrap(state: AppState, config: Arc<Config>) {
    if !config.self_monitoring.enabled {
        return;
    }
    info!(
        interval_secs = config.self_monitoring.export_interval_seconds,
        "self-monitoring bootstrap starting (OTLP export)"
    );
    export::spawn_exporter(state, config);
}
