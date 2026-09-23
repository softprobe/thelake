use crate::async_jobs::{self, Job};
use crate::compaction::MaintenanceEngine;
use crate::compaction::PhysicalScopeMaintenanceJob;
use crate::runtime_engine::RuntimeEngineManager;
use crate::session_summary::{SessionSummaryRebuildJob, SessionSummaryReduceJob};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start the shared async job runner with maintenance and/or session_summary jobs.
///
/// One `spawn_runner` only — never a second timer loop.
pub async fn start_maintenance_scheduler(
    engines: Arc<RuntimeEngineManager>,
) -> Result<Option<JoinHandle<()>>> {
    let config = engines.config();
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    let mut jobs: Vec<Arc<dyn Job>> = Vec::new();
    let scope_registry = engines.scope_registry().clone();

    if metadata_enabled || compaction_enabled {
        let wake = Duration::from_secs(config.maintenance.interval_seconds.max(1));
        let executor = MaintenanceEngine::new(config, scope_registry.clone()).await?;
        jobs.push(Arc::new(PhysicalScopeMaintenanceJob::new(
            executor,
            wake,
            compaction_enabled,
        )));
    }

    jobs.push(Arc::new(SessionSummaryReduceJob::new(
        scope_registry.clone(),
        config.clone(),
    )));
    jobs.push(Arc::new(SessionSummaryRebuildJob::new(
        scope_registry.clone(),
        config.clone(),
    )));

    if jobs.is_empty() {
        return Ok(None);
    }

    let leases = async_jobs::lease_store_for(&scope_registry);
    Ok(async_jobs::spawn_runner(&config.async_jobs, leases, jobs))
}

#[cfg(test)]
mod tests {
    use super::start_maintenance_scheduler;
    use crate::config::Config;
    use crate::runtime_engine::RuntimeEngineManager;
    use std::sync::Arc;

    #[tokio::test]
    async fn scheduler_starts_when_only_metadata_enabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = true;
        c.maintenance.interval_seconds = 60;
        let engines = Arc::new(
            RuntimeEngineManager::connect(Arc::new(c), None)
                .await
                .expect("connect engines"),
        );
        let out = start_maintenance_scheduler(engines)
            .await
            .expect("scheduler");
        assert!(out.is_some());
        out.unwrap().abort();
    }
}
