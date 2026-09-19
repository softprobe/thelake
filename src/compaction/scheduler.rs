use crate::async_jobs::{self, Job};
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::maintenance_job::MaintenanceJob;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::session_summary::{SessionSummaryRebuildJob, SessionSummaryReduceJob};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start the shared async job runner with maintenance and/or session_summary jobs.
///
/// One `spawn_runner` only — never a second timer loop.
pub async fn start_maintenance_scheduler(
    config: &Config,
    scope_registry: Option<DuckLakeScopeResolver>,
) -> Result<Option<JoinHandle<()>>> {
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    let summary_enabled = config.session_summary.enabled;

    let mut jobs: Vec<Arc<dyn Job>> = Vec::new();

    if metadata_enabled || compaction_enabled {
        let wake = Duration::from_secs(config.maintenance.interval_seconds.max(1));
        let executor = MaintenanceExecutor::new(config, scope_registry.clone()).await?;
        jobs.push(Arc::new(MaintenanceJob::new(
            executor,
            wake,
            compaction_enabled,
        )));
    }

    if summary_enabled {
        let registry = scope_registry.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "session_summary.enabled requires a Postgres DuckLakeScopeResolver (catalog)"
            )
        })?;
        jobs.push(Arc::new(SessionSummaryReduceJob::new(
            registry.pool().clone(),
            Some(registry.clone()),
            config.ducklake.clone(),
            config.session_summary.clone(),
        )));
        jobs.push(Arc::new(SessionSummaryRebuildJob::new(
            registry.pool().clone(),
            Some(registry),
            config.ducklake.clone(),
            config.session_summary.clone(),
        )));
    }

    if jobs.is_empty() {
        return Ok(None);
    }

    let leases = async_jobs::lease_store_for(scope_registry.as_ref());
    Ok(async_jobs::spawn_runner(&config.async_jobs, leases, jobs))
}

#[cfg(test)]
mod tests {
    use super::start_maintenance_scheduler;
    use crate::config::Config;

    #[tokio::test]
    async fn scheduler_skips_when_compaction_metadata_and_summary_disabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = false;
        c.session_summary.enabled = false;
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

    #[tokio::test]
    async fn scheduler_summary_without_registry_errs() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = false;
        c.session_summary.enabled = true;
        c.session_summary.reducer_interval_ms = 1000;
        c.ingest.flush_interval_seconds = 2;
        c.ducklake.catalog_type = "postgres".to_string();
        let err = start_maintenance_scheduler(&c, None)
            .await
            .expect_err("needs registry");
        assert!(err.to_string().contains("DuckLakeScopeResolver"));
    }
}
