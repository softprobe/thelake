//! Shared async job runner and cross-replica leases.
//!
//! Shared runner and lease store for jobs that require singleton execution.
//! Dirty-row reducers use their queue claims instead of this runner.

mod job;
mod lease;

#[cfg(test)]
mod tests;

pub use job::Job;
pub use lease::{LeaseStore, LeaseToken, MemoryLeaseStore, PostgresLeaseStore};

use crate::config::AsyncJobsConfig;
use crate::self_monitoring;
use futures::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Stops the heartbeat task on drop (incl. panic unwind from `job.run`).
struct HeartbeatStopGuard(Option<oneshot::Sender<()>>);

impl Drop for HeartbeatStopGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// In-process due clock: skip `Job::run` until `last_attempt + interval`.
/// Wake stays `min(interval)`; long-interval jobs do not execute every wake.
///
/// Stamped with the wake-start `Instant` (right after `ticker.tick()`), not run
/// completion — so wake==interval does not skip from multi-second passes.
/// When a pass **overruns** `interval`, success re-stamps to `now` so Tokio
/// `MissedTickBehavior::Delay` catch-up ticks do not busy-loop. Failures
/// [`Self::clear`] so the next wake retries.
struct DueTracker {
    last_attempt: HashMap<(String, String), Instant>,
}

impl DueTracker {
    fn new() -> Self {
        Self {
            last_attempt: HashMap::new(),
        }
    }

    fn is_due(&self, job: &str, scope: &str, interval: Duration) -> bool {
        match self.last_attempt.get(&(job.to_string(), scope.to_string())) {
            None => true,
            // Small skew: wake-start Instant::now() is slightly after the ticker
            // deadline, so the next aligned wake can be a hair under `interval`.
            Some(at) => at.elapsed() + Self::due_skew(interval) >= interval,
        }
    }

    fn due_skew(interval: Duration) -> Duration {
        Duration::from_millis(5)
            .min(interval / 10)
            .max(Duration::from_millis(1))
    }

    fn mark_attempt(&mut self, job: &str, scope: &str, at: Instant) {
        self.last_attempt
            .insert((job.to_string(), scope.to_string()), at);
    }

    /// When a wake overruns the shared ticker period, bump every scope still
    /// stamped with `wake_started` so a Tokio Delay catch-up tick does not
    /// re-run earlier work (any job/scope, Ok or after a later Err/panic).
    fn restamp_wake(&mut self, wake_started: Instant, now: Instant) {
        for at in self.last_attempt.values_mut() {
            if *at == wake_started {
                *at = now;
            }
        }
    }

    fn clear(&mut self, job: &str, scope: &str) {
        self.last_attempt
            .remove(&(job.to_string(), scope.to_string()));
    }
}

/// Spawn the shared wake loop. Returns `None` when `jobs` is empty.
///
/// Each wake: for every job/scope, if due → fenced acquire → if win, `run` with
/// heartbeat → always **release**. `Job::interval` sets both the shared wake
/// floor (`min` across jobs) and per-job due gating. Configure
/// `lease_ttl_seconds` well above `heartbeat_seconds` and typical pass latency.
/// A heartbeat failure cancels the current job future; fenced jobs should also
/// check lease loss between major side effects.
pub fn spawn_runner(
    config: &AsyncJobsConfig,
    leases: Arc<dyn LeaseStore>,
    jobs: Vec<Arc<dyn Job>>,
) -> Option<JoinHandle<()>> {
    if jobs.is_empty() {
        return None;
    }
    let holder_id = config.resolved_instance_id();
    let lease_ttl = Duration::from_secs(config.lease_ttl_seconds.max(1));
    let heartbeat_every = Duration::from_secs(config.heartbeat_seconds.max(1));
    let wake = jobs
        .iter()
        .map(|j| j.interval())
        .min()
        .unwrap_or(Duration::from_secs(60));
    // Floor keeps a buggy `Job::interval` of 0 from busy-spinning; tests may use
    // sub-second wakes via configurable maintenance intervals (Duration).
    let wake = wake.max(Duration::from_millis(50));

    info!(
        holder_id = %holder_id,
        wake_ms = wake.as_millis() as u64,
        jobs = jobs.len(),
        "async job runner starting"
    );
    self_monitoring::set_async_jobs_wake_ms(wake.as_millis() as u64);

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(wake);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut due = DueTracker::new();

        loop {
            ticker.tick().await;
            let wake_started = Instant::now();
            for job in &jobs {
                let scopes = match job.scope_keys().await {
                    Ok(s) => s,
                    Err(err) => {
                        warn!(job = job.name(), "scope_keys failed: {err}");
                        // Sentinel scope label (not a tenant_id) — filter in dashboards.
                        self_monitoring::record_job_error(job.name(), "_scopes");
                        continue;
                    }
                };
                // Sequential per scope by design: one SQL maintenance pass at a time
                // avoids merge/expire races and unbounded
                // task fan-out. Cross-tenant parallelism is a later stage if needed.
                for scope in scopes {
                    if !due.is_due(job.name(), &scope, job.interval()) {
                        self_monitoring::record_job_skip(job.name(), &scope, "not_due");
                        continue;
                    }
                    let token = match leases
                        .acquire_lease(job.name(), &scope, &holder_id, lease_ttl)
                        .await
                    {
                        Ok(Some(token)) => {
                            self_monitoring::record_lease_acquire(job.name(), &scope, "win");
                            token
                        }
                        Ok(None) => {
                            self_monitoring::record_lease_acquire(job.name(), &scope, "lose");
                            continue;
                        }
                        Err(err) => {
                            warn!(
                                job = job.name(),
                                scope = %scope,
                                "lease acquire failed: {err}"
                            );
                            self_monitoring::record_lease_acquire(job.name(), &scope, "error");
                            continue;
                        }
                    };

                    let hb_leases = Arc::clone(&leases);
                    let hb_job = job.name().to_string();
                    let hb_scope = scope.clone();
                    let hb_token = token.clone();
                    let hb_ttl = lease_ttl;
                    let (lost_tx, lost_rx) = watch::channel(false);
                    let (hb_stop_tx, mut hb_stop_rx) = oneshot::channel::<()>();
                    let hb_task = tokio::spawn(async move {
                        let mut hb_ticker = tokio::time::interval(heartbeat_every);
                        hb_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        // Skip the immediate first tick so we don't HB before work starts.
                        hb_ticker.tick().await;
                        loop {
                            tokio::select! {
                                _ = &mut hb_stop_rx => break,
                                _ = hb_ticker.tick() => {
                                    if let Err(err) = hb_leases
                                        .heartbeat_lease(&hb_job, &hb_scope, &hb_token, hb_ttl)
                                        .await
                                    {
                                        warn!(
                                            job = %hb_job,
                                            scope = %hb_scope,
                                            "lease heartbeat failed: {err}"
                                        );
                                        self_monitoring::record_lease_heartbeat_failure(
                                            &hb_job, &hb_scope,
                                        );
                                        let _ = lost_tx.send(true);
                                        break;
                                    }
                                }
                            }
                        }
                    });

                    // RAII: stop HB even if `job.run` panics.
                    let _hb_guard = HeartbeatStopGuard(Some(hb_stop_tx));
                    // Wake-start stamp when under the shared wake period; if this
                    // wake already overran `wake`, stamp `now` so later work is
                    // not due on a Delay catch-up tick.
                    let stamp = if wake_started.elapsed() >= wake {
                        Instant::now()
                    } else {
                        wake_started
                    };
                    due.mark_attempt(job.name(), &scope, stamp);
                    let run_started = Instant::now();
                    let run_result = AssertUnwindSafe(job.run_fenced(&scope, &token, lost_rx))
                        .catch_unwind()
                        .await;
                    let run_elapsed = run_started.elapsed();
                    drop(_hb_guard);
                    let _ = hb_task.await;

                    let status = match &run_result {
                        Ok(Ok(())) => "ok",
                        Ok(Err(_)) => "error",
                        Err(_) => "panic",
                    };
                    self_monitoring::record_job_duration(job.name(), &scope, status, run_elapsed);

                    match &run_result {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            due.clear(job.name(), &scope);
                            warn!(job = job.name(), scope = %scope, "job failed: {err}");
                            self_monitoring::record_job_error(job.name(), &scope);
                        }
                        Err(_) => {
                            due.clear(job.name(), &scope);
                            warn!(job = job.name(), scope = %scope, "job panicked");
                            self_monitoring::record_job_error(job.name(), &scope);
                        }
                    }
                    // After any outcome: if the wake overran the ticker period,
                    // restamp every scope still on wake_started (cross-job).
                    if wake_started.elapsed() >= wake {
                        due.restamp_wake(wake_started, Instant::now());
                    }

                    if let Err(err) = leases.release_lease(job.name(), &scope, &token).await {
                        warn!(
                            job = job.name(),
                            scope = %scope,
                            "lease release failed: {err}"
                        );
                    }
                }
            }
        }
    });

    Some(handle)
}

#[cfg(test)]
mod due_tracker_tests {
    use super::DueTracker;
    use std::time::{Duration, Instant};

    #[test]
    fn never_ran_is_due() {
        let due = DueTracker::new();
        assert!(due.is_due("j", "s", Duration::from_millis(200)));
    }

    #[test]
    fn wake_aligned_stamp_due_after_interval_minus_skew() {
        let mut due = DueTracker::new();
        let interval = Duration::from_millis(200);
        // Simulate wake-start stamp slightly "late" vs the next wake's elapsed.
        let stamped = Instant::now() - (interval - Duration::from_millis(3));
        due.mark_attempt("j", "s", stamped);
        assert!(
            due.is_due("j", "s", interval),
            "skew must keep wake==interval due on the next aligned wake"
        );
    }

    #[test]
    fn not_due_shortly_after_stamp() {
        let mut due = DueTracker::new();
        let interval = Duration::from_millis(200);
        due.mark_attempt("j", "s", Instant::now());
        assert!(!due.is_due("j", "s", interval));
    }

    #[test]
    fn restamp_wake_updates_all_jobs_with_same_stamp() {
        let mut due = DueTracker::new();
        let wake = Instant::now() - Duration::from_secs(10);
        due.mark_attempt("j", "a", wake);
        due.mark_attempt("j", "b", wake);
        due.mark_attempt("other", "a", wake);
        let fresh = Instant::now();
        due.mark_attempt("fresh", "x", fresh);
        let now = Instant::now();
        due.restamp_wake(wake, now);
        assert!(!due.is_due("j", "a", Duration::from_secs(3600)));
        assert!(!due.is_due("j", "b", Duration::from_secs(3600)));
        assert!(!due.is_due("other", "a", Duration::from_secs(3600)));
        assert!(!due.is_due("fresh", "x", Duration::from_secs(3600)));
    }

    #[test]
    fn clear_makes_due_again() {
        let mut due = DueTracker::new();
        due.mark_attempt("j", "s", Instant::now());
        due.clear("j", "s");
        assert!(due.is_due("j", "s", Duration::from_secs(3600)));
    }
}
