//! Leased `session_summary.reduce` / `session_summary.rebuild` on the shared runner.
//!
//! Jobs list workspace keys and call [`MaintenanceEngine`] façades only —
//! physical scope / pool identity stays inside the engine.

use crate::async_jobs::Job;
use crate::compaction::MaintenanceEngine;
use crate::config::{Config, SessionSummaryConfig};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use std::time::Duration;

pub(crate) const WORKSPACE_SESSION_SUMMARY_REDUCE_JOB: &str = "workspace_session_summary_reduce";
pub(crate) const WORKSPACE_SESSION_SUMMARY_REBUILD_JOB: &str = "workspace_session_summary_rebuild";

/// Leased reducer: claim dirty → lake aggregate → UPSERT summary → ack.
pub struct SessionSummaryReduceJob {
    maintenance: MaintenanceEngine,
    cfg: SessionSummaryConfig,
    interval: Duration,
}

impl SessionSummaryReduceJob {
    pub(crate) fn new(maintenance: MaintenanceEngine, config: &Config) -> Self {
        let cfg = config.session_summary.clone();
        let interval = Duration::from_millis(cfg.reducer_interval_ms.max(50));
        Self {
            maintenance,
            cfg,
            interval,
        }
    }
}

#[async_trait]
impl Job for SessionSummaryReduceJob {
    fn name(&self) -> &'static str {
        WORKSPACE_SESSION_SUMMARY_REDUCE_JOB
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        self.maintenance.workspace_scope_keys().await
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let scope = self.maintenance.resolve_scope(scope_key).await?;
        self.maintenance
            .reduce_session_summary(
                &scope,
                self.cfg.max_sessions_per_reduce,
                self.cfg.max_reduce_span_seconds,
            )
            .await?;
        Ok(())
    }
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
        let to = Utc::now();
        let from = to - ChronoDuration::seconds(self.cfg.max_reduce_span_seconds as i64);
        let scope = self.maintenance.resolve_scope(scope_key).await?;
        self.maintenance
            .rebuild_session_summary(&scope, from, to, self.cfg.max_reduce_span_seconds)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn session_summary_jobs_do_not_touch_physical_scope() {
        let src = include_str!("job.rs");
        let production = src.split("#[cfg(test)]").next().expect("production");
        for needle in [
            "PhysicalScope",
            "DuckLakeScopeResolver",
            "default_physical_scope",
            ".pool()",
            "cached_scopes",
            "lookup_cached_scope",
            "list_summary_scopes",
        ] {
            assert!(
                !production.contains(needle),
                "session_summary jobs must not reference {needle}"
            );
        }
        assert!(
            production.contains("workspace_scope_keys")
                && production.contains("resolve_scope")
                && production.contains("reduce_session_summary")
                && production.contains("rebuild_session_summary"),
            "jobs must use MaintenanceEngine façades only"
        );
    }
}
