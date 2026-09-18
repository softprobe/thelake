use crate::async_jobs::{self, Job};
use crate::catalog::DropdownCatalog;
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::maintenance_job::MaintenanceJob;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Wake often enough for snapshot expiry without forcing TWCS at that rate.
pub fn scheduler_wake_seconds(
    metadata_enabled: bool,
    compaction_enabled: bool,
    metadata_interval_seconds: u64,
    compaction_interval_seconds: u64,
) -> Option<u64> {
    match (metadata_enabled, compaction_enabled) {
        (false, false) => None,
        (true, false) => Some(metadata_interval_seconds.max(1)),
        (false, true) => Some(compaction_interval_seconds.max(1)),
        (true, true) => Some(
            metadata_interval_seconds
                .min(compaction_interval_seconds)
                .max(1),
        ),
    }
}

/// TWCS/ladder is due on its own interval, not on every metadata tick (AC-Q9).
///
/// Uses shared [`async_jobs::interval_due`] (+2s slack) so runner and compact
/// gating stay identical.
pub fn compaction_due(elapsed: Duration, compaction_interval_seconds: u64) -> bool {
    async_jobs::interval_due(
        elapsed,
        Duration::from_secs(compaction_interval_seconds),
    )
}

/// Start maintenance on the shared async job runner (leased per tenant).
pub async fn start_maintenance_scheduler(
    config: &Config,
    dropdown_catalog: Option<Arc<DropdownCatalog>>,
    scope_registry: Option<DuckLakeScopeResolver>,
) -> Result<Option<JoinHandle<()>>> {
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    let Some(wake_secs) = scheduler_wake_seconds(
        metadata_enabled,
        compaction_enabled,
        config.maintenance.metadata_interval_seconds,
        config.maintenance.interval_seconds,
    ) else {
        return Ok(None);
    };

    // Sticky hold has no inter-wake heartbeat; TTL must outlive the wake gap.
    let ttl = config.async_jobs.lease_ttl_seconds.max(1);
    if ttl <= wake_secs {
        anyhow::bail!(
            "async_jobs.lease_ttl_seconds ({ttl}) must be > maintenance wake ({wake_secs}s) \
             so the sticky holder survives until the next try_acquire"
        );
    }

    let executor =
        MaintenanceExecutor::new(config, dropdown_catalog, scope_registry.clone()).await?;
    let job: Arc<dyn Job> = Arc::new(MaintenanceJob::new(
        executor,
        wake_secs,
        config.maintenance.interval_seconds.max(1),
        compaction_enabled,
    ));
    let leases = async_jobs::lease_store_for(scope_registry.as_ref());
    Ok(async_jobs::spawn_runner(
        &config.async_jobs,
        leases,
        vec![job],
    ))
}

#[cfg(test)]
mod tests {
    use super::{compaction_due, scheduler_wake_seconds, start_maintenance_scheduler};
    use crate::config::Config;

    #[tokio::test]
    async fn scheduler_skips_when_compaction_and_metadata_disabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = false;
        let out = start_maintenance_scheduler(&c, None, None)
            .await
            .expect("scheduler");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn scheduler_rejects_lease_ttl_not_greater_than_wake() {
        let mut c = Config::default();
        c.maintenance.enabled = true;
        c.maintenance.metadata_enabled = false;
        c.maintenance.interval_seconds = 300;
        c.async_jobs.lease_ttl_seconds = 120; // ≤ wake
        c.async_jobs.heartbeat_seconds = 30;
        let err = start_maintenance_scheduler(&c, None, None)
            .await
            .expect_err("ttl <= wake");
        assert!(
            err.to_string().contains("lease_ttl_seconds"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn wake_uses_metadata_interval_when_both_enabled() {
        let cfg = Config::default();
        assert_eq!(cfg.maintenance.metadata_interval_seconds, 60);
        assert_eq!(cfg.maintenance.interval_seconds, 300);
        assert_eq!(
            scheduler_wake_seconds(true, true, 60, 300),
            Some(60),
            "wake for expiry; TWCS must not inherit this as its merge period"
        );
    }

    #[test]
    fn twcs_does_not_run_every_metadata_tick() {
        use std::time::Duration;
        assert!(!compaction_due(Duration::from_secs(0), 300));
        assert!(!compaction_due(Duration::from_secs(60), 300));
        assert!(!compaction_due(Duration::from_secs(297), 300));
        assert!(
            compaction_due(Duration::from_secs(298), 300),
            "2s early slack matching pre-lease Instant compare"
        );
        assert!(compaction_due(Duration::from_secs(300), 300));
        assert!(compaction_due(Duration::from_secs(301), 300));
    }
}
