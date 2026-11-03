use crate::async_jobs::{self, Job};
use crate::compaction::{PhysicalScopeMaintenanceJob, WorkspacePurgeJob};
use crate::session_summary::SessionSummaryRebuildJob;
use crate::workspace::WorkspaceManager;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start the shared async job runner with maintenance and/or session_summary jobs.
///
/// One `spawn_runner` only — never a second timer loop.
pub async fn start_maintenance_scheduler(
    workspaces: Arc<WorkspaceManager>,
) -> Result<Option<JoinHandle<()>>> {
    let config = workspaces.config();
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    let mut jobs: Vec<Arc<dyn Job>> = Vec::new();
    let maintenance = workspaces.maintenance_engine().await?;

    // Workspace deletion has a user-visible lifecycle deadline. Run its
    // tombstone queue before maintenance and summary rebuilds, which can be
    // slow or repeatedly fail on unrelated workspace scopes.
    jobs.push(Arc::new(WorkspacePurgeJob::new(
        workspaces.clone(),
        Duration::from_secs(5),
    )));

    if metadata_enabled || compaction_enabled {
        let wake = Duration::from_secs(config.maintenance.interval_seconds.max(1));
        jobs.push(Arc::new(PhysicalScopeMaintenanceJob::new(
            maintenance.clone(),
            wake,
        )));
    }

    jobs.push(Arc::new(SessionSummaryRebuildJob::new(
        maintenance.clone(),
        workspaces.clone(),
        config,
    )));

    if jobs.is_empty() {
        return Ok(None);
    }

    let leases = Arc::new(workspaces.lease_store());
    Ok(async_jobs::spawn_runner(&config.async_jobs, leases, jobs))
}

#[cfg(test)]
mod tests {
    use super::start_maintenance_scheduler;
    use crate::config::Config;
    use crate::workspace::WorkspaceManager;
    use std::sync::Arc;

    #[tokio::test]
    async fn scheduler_starts_when_only_metadata_enabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = true;
        c.maintenance.interval_seconds = 60;
        let workspaces = Arc::new(
            WorkspaceManager::connect(Arc::new(c), None)
                .await
                .expect("connect workspaces"),
        );
        let out = start_maintenance_scheduler(workspaces)
            .await
            .expect("scheduler");
        assert!(out.is_some());
        out.unwrap().abort();
    }

    #[test]
    fn scheduler_does_not_construct_jobs_with_raw_resolver() {
        let src = include_str!("scheduler.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        assert!(
            production.contains("maintenance_engine()")
                && !production.contains("MaintenanceEngine::new(")
                && !production.contains("SessionSummaryReduceJob")
                && !production.contains("start_session_summary_reducer")
                && production.contains("SessionSummaryRebuildJob::new("),
            "maintenance scheduler must register only leased maintenance/rebuild jobs"
        );
        let main = include_str!("../main.rs");
        assert!(main.contains("start_session_summary_reducer("));
    }

    #[test]
    fn workspace_purge_runs_before_maintenance_jobs() {
        let src = include_str!("scheduler.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        let purge = production
            .find("WorkspacePurgeJob::new(")
            .expect("workspace purge job");
        let physical = production
            .find("PhysicalScopeMaintenanceJob::new(")
            .expect("physical maintenance job");
        let summary = production
            .find("SessionSummaryRebuildJob::new(")
            .expect("summary rebuild job");
        assert!(purge < physical && purge < summary);
    }
}
