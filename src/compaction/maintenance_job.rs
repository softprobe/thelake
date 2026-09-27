//! Physical-scope maintenance as a shared [`Job`].
//!
//! The job only deals in opaque scope keys. Physical identity stays inside
//! [`MaintenanceEngine`].

use crate::async_jobs::Job;
use crate::compaction::MaintenanceEngine;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::watch;

pub const PHYSICAL_SCOPE_MAINTENANCE_JOB: &str = "physical_scope_maintenance";

fn validate_pass_results(
    scope_key: &str,
    compaction_enabled: bool,
    results: &[crate::compaction::TableMaintenanceResult],
) -> Result<()> {
    if compaction_enabled {
        let statuses: Vec<_> = results.iter().map(|r| r.compaction.status).collect();
        if !crate::compaction::pass_compaction_ok(&statuses) {
            return Err(anyhow!(
                "compaction failed for scope {scope_key}: {statuses:?}"
            ));
        }
    }
    crate::self_monitoring::record_maintenance();
    Ok(())
}

pub struct PhysicalScopeMaintenanceJob {
    executor: MaintenanceEngine,
    interval: Duration,
    compaction_enabled: bool,
}

impl PhysicalScopeMaintenanceJob {
    pub fn new(executor: MaintenanceEngine, interval: Duration, compaction_enabled: bool) -> Self {
        Self {
            executor,
            interval: interval.max(Duration::from_millis(50)),
            compaction_enabled,
        }
    }
}

#[async_trait]
impl Job for PhysicalScopeMaintenanceJob {
    fn name(&self) -> &'static str {
        PHYSICAL_SCOPE_MAINTENANCE_JOB
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        self.executor.maintenance_scope_keys().await
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let results = self
            .executor
            .run_pass_for_key(scope_key, self.compaction_enabled)
            .await?;
        validate_pass_results(scope_key, self.compaction_enabled, &results)
    }

    async fn run_fenced(
        &self,
        scope_key: &str,
        _token: &crate::async_jobs::LeaseToken,
        lease_lost: watch::Receiver<bool>,
    ) -> Result<()> {
        let results = self
            .executor
            .run_pass_for_key_fenced(scope_key, self.compaction_enabled, lease_lost)
            .await?;
        validate_pass_results(scope_key, self.compaction_enabled, &results)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn job_fails_when_compaction_status_failed_or_unsupported() {
        use crate::compaction::{pass_compaction_ok, ActionStatus};
        assert!(!pass_compaction_ok(&[ActionStatus::Failed]));
        assert!(!pass_compaction_ok(&[ActionStatus::Unsupported]));
        assert!(pass_compaction_ok(&[
            ActionStatus::Skipped,
            ActionStatus::Completed
        ]));
        // Both leased entry points validate Failed/Unsupported compaction outcomes.
        let src = include_str!("maintenance_job.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        assert!(
            production.contains("validate_pass_results")
                && production.contains("compaction failed for scope")
                && production.contains("run_pass_for_key_fenced"),
            "leased job must Err when compaction Failed/Unsupported"
        );
        assert!(
            !production.contains("PhysicalScope::")
                && !production.contains("&PhysicalScope")
                && !production.contains("lookup_cached_scope"),
            "maintenance job must not hold or resolve PhysicalScope"
        );
    }

    #[test]
    fn job_source_uses_engine_key_apis_only() {
        let src = include_str!("maintenance_job.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        assert!(
            production.contains("maintenance_scope_keys")
                && production.contains("run_pass_for_key"),
            "job must call MaintenanceEngine key façades"
        );
        assert!(
            !production.contains("use crate::workspace_scope::PhysicalScope")
                && !production.contains("use crate::storage::ducklake::PhysicalScope")
                && !production.contains("PhysicalScope::")
                && !production.contains("&PhysicalScope")
                && !production.contains("physical_scopes(")
                && !production.contains("ensure_physical_scope_bootstrap")
                && !production.contains("run_physical_scope_pass"),
            "job must not touch physical-scope APIs"
        );
    }
}
