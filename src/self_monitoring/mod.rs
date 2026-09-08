//! thelake self-monitoring: OTel Meter API + internal DuckLake export.

mod convert;
mod events;
mod export;
pub mod gauge_store;
mod ids;
mod instruments;
mod inventory;
mod labels;
mod size_bucket;

#[cfg(test)]
mod tests;

pub use events::{try_enqueue_slow_query, SlowQueryEvent};
pub use ids::{instrument_customer_tenant, is_reserved_tenant_id, OPS_TENANT_ID};
pub use instruments::{
    record_compaction_pass, record_compaction_wave, record_export_drop, record_ingest,
    record_maintenance, record_orphan_remove, record_query, record_query_queue_wait,
    record_slow_query, record_snapshot_expire, record_write, self_monitoring_export_drops,
};
pub use labels::{bound_app, classify_sql_kind};

use crate::api::AppState;
use crate::config::Config;
use crate::runtime_engine::{DuckLakeScope, ScopeProvisioningRequest};
use std::sync::Arc;
use tracing::{info, warn};

/// Best-effort: ensure ops scope, warm ops engine, start OTel PeriodicReader exporter.
pub async fn bootstrap(state: AppState, config: Arc<Config>) {
    if !config.self_monitoring.enabled {
        return;
    }
    info!(
        tenant = OPS_TENANT_ID,
        schema = %config.self_monitoring.ops_metadata_schema,
        data_path = %config.self_monitoring.ops_data_path,
        "self-monitoring bootstrap starting"
    );

    if let Some(registry) = state.engines.scope_registry() {
        let req = ScopeProvisioningRequest {
            scope_id: OPS_TENANT_ID.to_string(),
            metadata_schema: config.self_monitoring.ops_metadata_schema.clone(),
            data_path: config.self_monitoring.ops_data_path.clone(),
        };
        if let Err(err) = registry.provision_scope(req).await {
            warn!("self-monitoring ops scope ensure failed (continuing degraded): {err}");
        }
    }

    match state.engines.engine_for(OPS_TENANT_ID).await {
        Ok(_engine) => {
            info!("self-monitoring ops RuntimeEngine ready");
        }
        Err(err) => {
            warn!("self-monitoring ops engine attach failed (continuing degraded): {err}");
        }
    }

    export::spawn_exporter(state, config);
}

/// Ops DuckLakeScope from config (no registry).
pub fn ops_scope_from_config(config: &Config) -> DuckLakeScope {
    DuckLakeScope {
        metadata_schema: config.self_monitoring.ops_metadata_schema.clone(),
        data_path: config.self_monitoring.ops_data_path.clone(),
    }
}
