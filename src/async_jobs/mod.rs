//! Shared async job runner and cross-replica leases.
//!
//! One runner loop, one [`LeaseStore`] trait, one [`Job`] trait — used by
//! maintenance today and session-summary later. Do not add a second timer or lock.

mod job;
mod lease;

#[cfg(test)]
mod tests;

pub use job::Job;
pub use lease::{LeaseStore, MemoryLeaseStore, PostgresLeaseStore};

use crate::config::AsyncJobsConfig;
use crate::self_monitoring;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
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

/// Spawn the shared wake loop. Returns `None` when `jobs` is empty.
///
/// Each wake: for every job/scope, `try_acquire` → if win, `run` with heartbeat →
/// always **release**. `Job::interval` only sets the wake period (min across
/// jobs). Configure `lease_ttl_seconds` well above `heartbeat_seconds` and
/// typical pass latency. Heartbeat failures are logged; they do not abort
/// `job.run`.
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

        loop {
            ticker.tick().await;
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
                // Sequential per scope by design (matches pre-lease maintenance): one
                // TWCS/metadata pass at a time avoids compact∥expire races and unbounded
                // task fan-out. Cross-tenant parallelism is a later stage if needed.
                for scope in scopes {
                    match leases
                        .try_acquire(job.name(), &scope, &holder_id, lease_ttl)
                        .await
                    {
                        Ok(true) => {
                            self_monitoring::record_lease_acquire(job.name(), &scope, "win");
                        }
                        Ok(false) => {
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
                    }

                    let hb_leases = Arc::clone(&leases);
                    let hb_job = job.name().to_string();
                    let hb_scope = scope.clone();
                    let hb_holder = holder_id.clone();
                    let hb_ttl = lease_ttl;
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
                                        .heartbeat(&hb_job, &hb_scope, &hb_holder, hb_ttl)
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
                                    }
                                }
                            }
                        }
                    });

                    // RAII: stop HB even if `job.run` panics.
                    let _hb_guard = HeartbeatStopGuard(Some(hb_stop_tx));
                    let run_started = std::time::Instant::now();
                    let run_result = AssertUnwindSafe(job.run(&scope)).catch_unwind().await;
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
                            warn!(job = job.name(), scope = %scope, "job failed: {err}");
                            self_monitoring::record_job_error(job.name(), &scope);
                        }
                        Err(_) => {
                            warn!(job = job.name(), scope = %scope, "job panicked");
                            self_monitoring::record_job_error(job.name(), &scope);
                        }
                    }

                    if let Err(err) = leases.release(job.name(), &scope, &holder_id).await {
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
