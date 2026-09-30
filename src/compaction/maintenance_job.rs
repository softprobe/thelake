//! Physical-scope maintenance as a shared [`Job`].
//!
//! The job only deals in opaque scope keys. Physical identity stays inside
//! [`MaintenanceEngine`].

use crate::async_jobs::Job;
use crate::compaction::MaintenanceEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::watch;

pub const PHYSICAL_SCOPE_MAINTENANCE_JOB: &str = "physical_scope_maintenance";

pub struct PhysicalScopeMaintenanceJob {
    executor: MaintenanceEngine,
    interval: Duration,
}

impl PhysicalScopeMaintenanceJob {
    pub fn new(executor: MaintenanceEngine, interval: Duration) -> Self {
        Self {
            executor,
            interval: interval.max(Duration::from_millis(50)),
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
        self.executor.run_pass_for_key(scope_key).await
    }

    async fn run_fenced(
        &self,
        scope_key: &str,
        token: &crate::async_jobs::LeaseToken,
        lease_lost: watch::Receiver<bool>,
    ) -> Result<()> {
        self.executor
            .run_pass_for_key_fenced(scope_key, token, lease_lost)
            .await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn job_delegates_scope_pass_to_sql_engine() {
        let src = include_str!("maintenance_job.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        assert!(
            production.contains("run_pass_for_key_fenced")
                && production.contains("run_pass_for_key(scope_key"),
            "job must only invoke the scoped SQL maintenance pass"
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
