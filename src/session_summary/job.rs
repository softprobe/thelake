//! Unleased dirty-row reduction and leased session-summary rebuild.

use crate::async_jobs::Job;
use crate::compaction::MaintenanceEngine;
use crate::config::{Config, SessionSummaryConfig};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;

pub(crate) const WORKSPACE_SESSION_SUMMARY_REBUILD_JOB: &str = "workspace_session_summary_rebuild";

pub async fn start_session_summary_reducer(
    engines: std::sync::Arc<crate::runtime_engine::RuntimeEngineManager>,
) -> Result<JoinHandle<()>> {
    let maintenance = engines.maintenance_engine().await?;
    Ok(spawn_reduce_loop(maintenance, engines.config()))
}

/// Drain durable dirty rows without taking a workspace lease. PostgreSQL row
/// claims distribute batches across replicas; failures back off until next tick.
fn spawn_reduce_loop(maintenance: MaintenanceEngine, config: &Config) -> JoinHandle<()> {
    let cfg = config.session_summary.clone();
    let interval = Duration::from_millis(cfg.reducer_interval_ms.max(50));
    tokio::spawn(async move {
        loop {
            let scopes = match maintenance.workspace_scope_keys().await {
                Ok(scopes) => scopes,
                Err(err) => {
                    warn!(error = %err, "session-summary reducer could not list workspaces");
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };
            let mut worked = false;
            let mut failed = false;
            for scope in scopes {
                match maintenance
                    .reduce_session_summary_for_key(
                        &scope,
                        cfg.max_sessions_per_reduce,
                        cfg.max_reduce_span_seconds,
                    )
                    .await
                {
                    Ok(n) => worked |= n > 0,
                    Err(err) => {
                        failed = true;
                        warn!(scope = %scope, error = %err, "session-summary reduce failed");
                    }
                }
            }
            if !worked || failed {
                tokio::time::sleep(interval).await;
            }
        }
    })
}

/// Leased rebuild: window `[now - max_reduce_span, now]` → lake aggregate → UPSERT.
pub struct SessionSummaryRebuildJob {
    maintenance: MaintenanceEngine,
    cfg: SessionSummaryConfig,
    interval: Duration,
}

impl SessionSummaryRebuildJob {
    pub(crate) fn new(maintenance: MaintenanceEngine, config: &Config) -> Self {
        let cfg = config.session_summary.clone();
        let interval = Duration::from_millis(cfg.rebuild_interval_ms.max(50));
        Self {
            maintenance,
            cfg,
            interval,
        }
    }
}

#[async_trait]
impl Job for SessionSummaryRebuildJob {
    fn name(&self) -> &'static str {
        WORKSPACE_SESSION_SUMMARY_REBUILD_JOB
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        self.maintenance.workspace_scope_keys().await
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        // Periodic safety-net rebuild (rebuild_interval_ms). Due-gating alone
        // keeps idle cost near zero; do not gate on dirty_depth — empty dirty is
        // the healthy steady state when this job should still re-aggregate.
        let to = Utc::now();
        let from = to - ChronoDuration::seconds(self.cfg.max_reduce_span_seconds as i64);
        self.maintenance
            .rebuild_session_summary_for_key(scope_key, from, to, self.cfg.max_reduce_span_seconds)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn session_summary_jobs_use_key_facades_only() {
        let src = include_str!("job.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        for needle in [
            "PhysicalScope",
            "DuckLakeScopeResolver",
            "MaintenanceScope",
            "reduce_session_summary(&",
            "rebuild_session_summary(&",
            ".resolve_scope(",
            ".pool()",
            "session_summary_dirty_depth",
        ] {
            assert!(
                !production.contains(needle),
                "session_summary jobs must not reference {needle}"
            );
        }
        assert!(
            production.contains("reduce_session_summary_for_key")
                && production.contains("rebuild_session_summary_for_key")
                && production.contains("workspace_scope_keys"),
            "jobs must use MaintenanceEngine key façades only"
        );
    }
}
