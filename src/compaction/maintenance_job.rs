//! Maintenance as a shared [`Job`] — one job_name `maintenance` per tenant scope.

use crate::async_jobs::Job;
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::scheduler::compaction_due;
use crate::config::DuckLakeConfig;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub struct MaintenanceJob {
    executor: MaintenanceExecutor,
    /// Shared wake interval (min of metadata / compaction intervals).
    wake: Duration,
    compact_interval_secs: u64,
    compaction_enabled: bool,
    /// Last time a full compact wake completed with every attempted scope Ok.
    last_compact: Mutex<Instant>,
    /// Frozen in [`scope_keys`] for the whole wake — never recomputed mid-pass.
    compact_this_wake: Mutex<bool>,
    /// Outcome of compact runs this wake: `None` = none yet, `Some(true)` = all Ok,
    /// `Some(false)` = at least one Err. Applied to `last_compact` on the next freeze.
    compact_wake_ok: Mutex<Option<bool>>,
    /// Scopes from the latest [`scope_keys`] call (avoids a second registry list).
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
}

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
            compact_wake_ok: Mutex::new(None),
            cached_scopes: Mutex::new(Vec::new()),
        }
    }

    fn freeze_compaction_for_wake(&self) {
        // Close out prior wake: only advance the TWCS clock if every compact
        // attempt succeeded. A mid-wake Err leaves last_compact unchanged so
        // the next wake is still due for remaining tenants.
        if matches!(lock_mutex(&self.compact_wake_ok).take(), Some(true)) {
            *lock_mutex(&self.last_compact) = Instant::now();
        }
        let due = self.compaction_enabled
            && compaction_due(
                lock_mutex(&self.last_compact).elapsed(),
                self.compact_interval_secs,
            );
        *lock_mutex(&self.compact_this_wake) = due;
    }

    fn note_compact_outcome(&self, ok: bool) {
        let mut outcome = lock_mutex(&self.compact_wake_ok);
        match *outcome {
            None => *outcome = Some(ok),
            Some(true) if !ok => *outcome = Some(false),
            Some(_) => {}
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
        self.freeze_compaction_for_wake();
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let ducklake = {
            let found = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key);
            match found {
                Some(dk) => dk,
                None => {
                    // Defensive: refresh if cache and runner list ever diverge.
                    let scopes = self.executor.maintenance_scopes().await?;
                    let dk = lookup_cached_scope(&scopes, scope_key)
                        .ok_or_else(|| anyhow!("unknown maintenance scope {scope_key}"))?;
                    *lock_mutex(&self.cached_scopes) = scopes;
                    dk
                }
            }
        };
        let run_compaction = *lock_mutex(&self.compact_this_wake);
        let pass = self
            .executor
            .run_tenant_pass(scope_key, &ducklake, run_compaction)
            .await;
        match &pass {
            Ok(_) => {
                if run_compaction {
                    self.note_compact_outcome(true);
                }
                crate::self_monitoring::record_maintenance();
            }
            Err(_) => {
                if run_compaction {
                    self.note_compact_outcome(false);
                }
            }
        }
        // Idempotent and global: always attempt prune after a leased pass, even
        // when the tenant pass failed (so a sticky bad tenant cannot starve TTL).
        self.executor.prune_dropdown_catalog().await;
        pass.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in for compact_this_wake freeze semantics without a full executor.
    struct FreezeProbe {
        compact_this_wake: Mutex<bool>,
        last_compact: Mutex<Instant>,
        compact_wake_ok: Mutex<Option<bool>>,
        interval_secs: u64,
    }

    impl FreezeProbe {
        fn freeze(&self) {
            if matches!(lock_mutex(&self.compact_wake_ok).take(), Some(true)) {
                *lock_mutex(&self.last_compact) = Instant::now();
            }
            let due = compaction_due(lock_mutex(&self.last_compact).elapsed(), self.interval_secs);
            *lock_mutex(&self.compact_this_wake) = due;
        }

        fn note(&self, ok: bool) {
            let mut outcome = lock_mutex(&self.compact_wake_ok);
            match *outcome {
                None => *outcome = Some(ok),
                Some(true) if !ok => *outcome = Some(false),
                Some(_) => {}
            }
        }

        fn read_frozen(&self) -> bool {
            *lock_mutex(&self.compact_this_wake)
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
            compact_wake_ok: Mutex::new(None),
            interval_secs: 300,
        };
        probe.freeze();
        assert!(probe.read_frozen(), "should freeze due=true");

        *lock_mutex(&probe.last_compact) = Instant::now();
        let would_be_due_now = compaction_due(
            lock_mutex(&probe.last_compact).elapsed(),
            probe.interval_secs,
        );
        assert!(!would_be_due_now);
        assert!(probe.read_frozen(), "frozen flag must stay true mid-pass");
    }

    #[test]
    fn compact_clock_advances_only_when_all_scopes_ok() {
        let probe = FreezeProbe {
            compact_this_wake: Mutex::new(false),
            last_compact: Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(300))
                    .unwrap(),
            ),
            compact_wake_ok: Mutex::new(None),
            interval_secs: 300,
        };
        let before = *lock_mutex(&probe.last_compact);
        probe.note(true); // tenant A ok
        probe.note(false); // tenant B err
        probe.freeze(); // must NOT advance
        assert_eq!(*lock_mutex(&probe.last_compact), before);
        assert!(
            probe.read_frozen(),
            "still due after failed wake so B can retry"
        );

        probe.note(true);
        probe.note(true);
        let mid = *lock_mutex(&probe.last_compact);
        probe.freeze(); // all ok → advance
        assert!(*lock_mutex(&probe.last_compact) > mid);
        assert!(
            !probe.read_frozen(),
            "not due immediately after all-ok wake"
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
            "miss must trigger refresh path in run()"
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
