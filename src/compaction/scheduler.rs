use crate::async_jobs::{self, Job};
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::maintenance_job::MaintenanceJob;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start maintenance when either TWCS or metadata work is enabled.
/// One leased job; each pass runs whichever sides are enabled.
pub async fn start_maintenance_scheduler(
    config: &Config,
    scope_registry: Option<DuckLakeScopeResolver>,
) -> Result<Option<JoinHandle<()>>> {
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    if !metadata_enabled && !compaction_enabled {
        return Ok(None);
    }

    let wake = Duration::from_secs(config.maintenance.interval_seconds.max(1));
    let executor = MaintenanceExecutor::new(config, scope_registry.clone()).await?;
    let job: Arc<dyn Job> = Arc::new(MaintenanceJob::new(executor, wake, compaction_enabled));
    let leases = async_jobs::lease_store_for(scope_registry.as_ref());
    Ok(async_jobs::spawn_runner(
        &config.async_jobs,
        leases,
        vec![job],
    ))
}

#[cfg(test)]
mod tests {
    use super::start_maintenance_scheduler;
    use crate::config::Config;

    #[tokio::test]
    async fn scheduler_skips_when_compaction_and_metadata_disabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = false;
        let out = start_maintenance_scheduler(&c, None)
            .await
            .expect("scheduler");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn scheduler_starts_when_only_metadata_enabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = true;
        c.maintenance.interval_seconds = 60;
        let out = start_maintenance_scheduler(&c, None)
            .await
            .expect("scheduler");
        assert!(out.is_some());
        out.unwrap().abort();
    }
}
