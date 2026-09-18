//! Maintenance as a shared [`Job`] — one job_name `maintenance` per tenant scope.

use crate::async_jobs::Job;
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::scheduler::compaction_due;
use crate::config::DuckLakeConfig;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct MaintenanceJob {
    executor: MaintenanceExecutor,
    /// Shared wake interval (min of metadata / compaction intervals).
    wake: Duration,
    compact_interval_secs: u64,
    compaction_enabled: bool,
    /// Last time TWCS/ladder completed successfully (any scope).
    last_compact: Mutex<Instant>,
    /// Frozen in [`scope_keys`] for the whole wake — never recomputed mid-pass.
    compact_this_wake: Mutex<bool>,
    /// Advance `last_compact` at most once per wake after a successful compact pass.
    compact_clock_advanced: Mutex<bool>,
    /// Scopes from the latest [`scope_keys`] call (avoids a second registry list).
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
}

impl MaintenanceJob {
    pub fn new(
        executor: MaintenanceExecutor,
        wake_secs: u64,
        compact_interval_secs: u64,
        compaction_enabled: bool,
    ) -> Self {
        let compact = compact_interval_secs.max(1);
        Self {
            executor,
            wake: Duration::from_secs(wake_secs.max(1)),
            compact_interval_secs: compact,
            compaction_enabled,
            last_compact: Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(compact))
                    .unwrap_or_else(Instant::now),
            ),
            compact_this_wake: Mutex::new(false),
            compact_clock_advanced: Mutex::new(false),
            cached_scopes: Mutex::new(Vec::new()),
        }
    }

    fn freeze_compaction_for_wake(&self) {
        let due = self.compaction_enabled
            && compaction_due(
                self.last_compact.lock().expect("last_compact").elapsed(),
                self.compact_interval_secs,
            );
        *self.compact_this_wake.lock().expect("compact_this_wake") = due;
        *self
            .compact_clock_advanced
            .lock()
            .expect("compact_clock_advanced") = false;
    }
}

#[async_trait]
impl Job for MaintenanceJob {
    fn name(&self) -> &'static str {
        "maintenance"
    }

    fn interval(&self) -> Duration {
        self.wake
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        let scopes = self.executor.maintenance_scopes().await?;
        self.freeze_compaction_for_wake();
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *self.cached_scopes.lock().expect("cached_scopes") = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let ducklake = {
            let scopes = self.cached_scopes.lock().expect("cached_scopes");
            scopes
                .iter()
                .find(|(id, _)| id == scope_key)
                .map(|(_, dk)| dk.clone())
                .ok_or_else(|| anyhow!("unknown maintenance scope {scope_key}"))?
        };
        let run_compaction = *self.compact_this_wake.lock().expect("compact_this_wake");
        self.executor
            .run_tenant_pass(scope_key, &ducklake, run_compaction)
            .await?;
        crate::self_monitoring::record_maintenance();
        if run_compaction {
            let mut advanced = self
                .compact_clock_advanced
                .lock()
                .expect("compact_clock_advanced");
            if !*advanced {
                *self.last_compact.lock().expect("last_compact") = Instant::now();
                *advanced = true;
            }
        }
        // Idempotent; every successful leased run may prune (no last-scope heuristic).
        self.executor.prune_dropdown_catalog().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in for compact_this_wake freeze semantics without a full executor.
    struct FreezeProbe {
        compact_this_wake: Mutex<bool>,
        last_compact: Mutex<Instant>,
        interval_secs: u64,
    }

    impl FreezeProbe {
        fn freeze(&self) {
            let due = compaction_due(
                self.last_compact.lock().unwrap().elapsed(),
                self.interval_secs,
            );
            *self.compact_this_wake.lock().unwrap() = due;
        }

        fn read_frozen(&self) -> bool {
            *self.compact_this_wake.lock().unwrap()
        }
    }

    #[test]
    fn freeze_survives_clock_advance_past_due_boundary() {
        let probe = FreezeProbe {
            compact_this_wake: Mutex::new(false),
            last_compact: Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(300))
                    .unwrap(),
            ),
            interval_secs: 300,
        };
        probe.freeze();
        assert!(probe.read_frozen(), "should freeze due=true");

        // Simulate wall time advancing as if another tenant ran for a long time;
        // frozen flag must not flip even if last_compact were recomputed.
        *probe.last_compact.lock().unwrap() = Instant::now();
        let would_be_due_now = compaction_due(
            probe.last_compact.lock().unwrap().elapsed(),
            probe.interval_secs,
        );
        assert!(
            !would_be_due_now,
            "fresh last_compact would recompute due=false"
        );
        assert!(
            probe.read_frozen(),
            "frozen wake decision must stay true mid-pass"
        );
    }
}
