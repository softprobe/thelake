//! Maintenance as a shared [`Job`] — one job_name `maintenance` per tenant scope.

use crate::async_jobs::Job;
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::scheduler::compaction_due;
use crate::config::DuckLakeConfig;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Recover from poisoned mutexes so one panicked pass cannot kill the runner forever.
fn lock_mutex<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lookup_cached_scope(
    cached: &[(String, DuckLakeConfig)],
    scope_key: &str,
) -> Option<DuckLakeConfig> {
    cached
        .iter()
        .find(|(id, _)| id == scope_key)
        .map(|(_, dk)| dk.clone())
}

struct CompactWakeState {
    last_compact: Instant,
    compact_this_wake: bool,
    /// `None` = none yet, `Some(true)` = all Ok, `Some(false)` = at least one Err.
    compact_wake_ok: Option<bool>,
    prune_pending: bool,
}

/// Process-local TWCS + prune-once gate. Sticky lease holder keeps the compact
/// clock meaningful across wakes (requires `lease_ttl > wake`).
pub(crate) struct CompactWakeGate {
    state: Mutex<CompactWakeState>,
    compact_interval_secs: u64,
    compaction_enabled: bool,
}

impl CompactWakeGate {
    pub(crate) fn new(compact_interval_secs: u64, compaction_enabled: bool) -> Self {
        let compact = compact_interval_secs.max(1);
        Self {
            state: Mutex::new(CompactWakeState {
                last_compact: Instant::now()
                    .checked_sub(Duration::from_secs(compact))
                    .unwrap_or_else(Instant::now),
                compact_this_wake: false,
                compact_wake_ok: None,
                prune_pending: false,
            }),
            compact_interval_secs: compact,
            compaction_enabled,
        }
    }

    /// Close prior wake outcomes, freeze TWCS for this wake, arm prune-once.
    pub(crate) fn freeze_for_wake(&self) -> bool {
        let mut s = lock_mutex(&self.state);
        if matches!(s.compact_wake_ok.take(), Some(true)) {
            s.last_compact = Instant::now();
        }
        let due = self.compaction_enabled
            && compaction_due(s.last_compact.elapsed(), self.compact_interval_secs);
        s.compact_this_wake = due;
        s.prune_pending = true;
        due
    }

    pub(crate) fn run_compaction(&self) -> bool {
        lock_mutex(&self.state).compact_this_wake
    }

    pub(crate) fn note_outcome(&self, ok: bool) {
        let mut s = lock_mutex(&self.state);
        match s.compact_wake_ok {
            None => s.compact_wake_ok = Some(ok),
            Some(true) if !ok => s.compact_wake_ok = Some(false),
            Some(_) => {}
        }
    }

    /// First leased `run` of the wake takes prune; later scopes see false.
    pub(crate) fn take_prune_pending(&self) -> bool {
        let mut s = lock_mutex(&self.state);
        std::mem::replace(&mut s.prune_pending, false)
    }

    #[cfg(test)]
    pub(crate) fn last_compact(&self) -> Instant {
        lock_mutex(&self.state).last_compact
    }

    #[cfg(test)]
    pub(crate) fn set_last_compact(&self, t: Instant) {
        lock_mutex(&self.state).last_compact = t;
    }
}

pub struct MaintenanceJob {
    executor: MaintenanceExecutor,
    /// Shared wake interval (min of metadata / compaction intervals).
    wake: Duration,
    compact: CompactWakeGate,
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
        Self {
            executor,
            wake: Duration::from_secs(wake_secs.max(1)),
            compact: CompactWakeGate::new(compact_interval_secs, compaction_enabled),
            cached_scopes: Mutex::new(Vec::new()),
        }
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
        self.compact.freeze_for_wake();
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let ducklake = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key)
            .ok_or_else(|| anyhow!("unknown maintenance scope {scope_key}"))?;
        let run_compaction = self.compact.run_compaction();
        let pass = self
            .executor
            .run_tenant_pass(scope_key, &ducklake, run_compaction)
            .await;
        match &pass {
            Ok(_) => {
                if run_compaction {
                    self.compact.note_outcome(true);
                }
                crate::self_monitoring::record_maintenance();
            }
            Err(_) => {
                if run_compaction {
                    self.compact.note_outcome(false);
                }
            }
        }
        // Once per wake (first leased run): prune even if this tenant pass failed
        // so a sticky bad tenant cannot starve TTL.
        if self.compact.take_prune_pending() {
            self.executor.prune_dropdown_catalog().await;
        }
        pass.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freeze_survives_clock_advance_past_due_boundary() {
        let gate = CompactWakeGate::new(300, true);
        gate.set_last_compact(
            Instant::now()
                .checked_sub(Duration::from_secs(300))
                .unwrap(),
        );
        assert!(gate.freeze_for_wake(), "should freeze due=true");

        gate.set_last_compact(Instant::now());
        let would_be_due_now = compaction_due(gate.last_compact().elapsed(), 300);
        assert!(!would_be_due_now);
        assert!(
            gate.run_compaction(),
            "frozen flag must stay true mid-pass"
        );
    }

    #[test]
    fn compact_clock_advances_only_when_all_scopes_ok() {
        let gate = CompactWakeGate::new(300, true);
        gate.set_last_compact(
            Instant::now()
                .checked_sub(Duration::from_secs(300))
                .unwrap(),
        );
        let before = gate.last_compact();
        gate.note_outcome(true); // tenant A ok
        gate.note_outcome(false); // tenant B err
        gate.freeze_for_wake(); // must NOT advance
        assert_eq!(gate.last_compact(), before);
        assert!(
            gate.run_compaction(),
            "still due after failed wake so B can retry"
        );

        gate.note_outcome(true);
        gate.note_outcome(true);
        let mid = gate.last_compact();
        gate.freeze_for_wake(); // all ok → advance
        assert!(gate.last_compact() > mid);
        assert!(
            !gate.run_compaction(),
            "not due immediately after all-ok wake"
        );
    }

    #[test]
    fn prune_pending_once_per_wake_across_scopes() {
        let gate = CompactWakeGate::new(300, true);
        gate.freeze_for_wake();
        assert!(gate.take_prune_pending(), "first leased scope prunes");
        assert!(
            !gate.take_prune_pending(),
            "second scope must not prune again"
        );
        assert!(!gate.take_prune_pending());
        // Next wake re-arms.
        gate.freeze_for_wake();
        assert!(gate.take_prune_pending());
    }

    #[test]
    fn prune_still_armed_after_compact_outcome_err() {
        let gate = CompactWakeGate::new(300, true);
        gate.freeze_for_wake();
        gate.note_outcome(false);
        assert!(
            gate.take_prune_pending(),
            "prune must run even when first tenant pass fails"
        );
    }

    #[test]
    fn lookup_cached_scope_miss_and_hit() {
        let cached = vec![(
            "t1".into(),
            DuckLakeConfig {
                metadata_schema: "s1".into(),
                ..DuckLakeConfig::default()
            },
        )];
        assert!(lookup_cached_scope(&cached, "t1").is_some());
        assert!(
            lookup_cached_scope(&cached, "new-tenant").is_none(),
            "miss must fail loud in run()"
        );
    }

    #[test]
    fn lock_mutex_recovers_from_poison() {
        let m = Mutex::new(1u32);
        let _ = std::panic::catch_unwind(|| {
            let _g = m.lock().unwrap();
            panic!("poison");
        });
        assert!(m.lock().is_err(), "mutex should be poisoned");
        *lock_mutex(&m) = 2;
        assert_eq!(*lock_mutex(&m), 2);
    }
}
