//! Leased `session_summary.reduce` / `session_summary.rebuild` on the shared runner.

use crate::async_jobs::Job;
use crate::compaction::MaintenanceEngine;
use crate::config::{Config, DuckLakeConfig, SessionSummaryConfig};
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::workspace_scope::DEFAULT_WORKSPACE_ID;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

pub(crate) const WORKSPACE_SESSION_SUMMARY_REDUCE_JOB: &str = "workspace_session_summary_reduce";
pub(crate) const WORKSPACE_SESSION_SUMMARY_REBUILD_JOB: &str = "workspace_session_summary_rebuild";

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

/// Shared scope listing for reduce + rebuild (one registry walk pattern).
async fn list_summary_scopes(
    scope_registry: &DuckLakeScopeResolver,
    default_ducklake: &DuckLakeConfig,
    #[cfg(test)] fixed_scopes: &Option<Vec<(String, DuckLakeConfig)>>,
) -> Result<Vec<(String, DuckLakeConfig)>> {
    #[cfg(test)]
    if let Some(fixed) = fixed_scopes {
        return Ok(fixed.clone());
    }
    let mut scopes = Vec::new();
    let default = default_ducklake.clone();
    let mut saw_default = false;
    for (scope_id, scope) in scope_registry.list_scopes().await? {
        let mut dk = default.clone();
        dk.metadata_path = scope.metadata_path;
        dk.metadata_schema = scope.metadata_schema;
        dk.data_path = scope.data_path;
        dk.catalog_alias = scope.catalog_alias;
        if dk.metadata_schema == default.metadata_schema && dk.data_path == default.data_path {
            saw_default = true;
        }
        scopes.push((scope_id, dk));
    }
    if !saw_default {
        scopes.insert(0, (DEFAULT_WORKSPACE_ID.to_string(), default));
    }
    Ok(scopes)
}

/// Leased reducer: claim dirty → lake aggregate → UPSERT summary → ack.
pub struct SessionSummaryReduceJob {
    maintenance: MaintenanceEngine,
    scope_registry: DuckLakeScopeResolver,
    default_ducklake: DuckLakeConfig,
    cfg: SessionSummaryConfig,
    interval: Duration,
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
    #[cfg(test)]
    fixed_scopes: Option<Vec<(String, DuckLakeConfig)>>,
}

impl SessionSummaryReduceJob {
    pub(crate) fn new(scope_registry: DuckLakeScopeResolver, config: Config) -> Self {
        let cfg = config.session_summary.clone();
        let default_ducklake = config.ducklake.clone();
        let interval = Duration::from_millis(cfg.reducer_interval_ms.max(50));
        Self {
            maintenance: MaintenanceEngine::from_config(&config, scope_registry.clone()),
            scope_registry,
            default_ducklake,
            cfg,
            interval,
            cached_scopes: Mutex::new(Vec::new()),
            #[cfg(test)]
            fixed_scopes: None,
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
        let scopes = list_summary_scopes(
            &self.scope_registry,
            &self.default_ducklake,
            #[cfg(test)]
            &self.fixed_scopes,
        )
        .await?;
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let _ducklake = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key)
            .ok_or_else(|| anyhow!("unknown session_summary scope {scope_key}"))?;
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
    scope_registry: DuckLakeScopeResolver,
    default_ducklake: DuckLakeConfig,
    cfg: SessionSummaryConfig,
    interval: Duration,
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
    #[cfg(test)]
    fixed_scopes: Option<Vec<(String, DuckLakeConfig)>>,
}

impl SessionSummaryRebuildJob {
    pub(crate) fn new(scope_registry: DuckLakeScopeResolver, config: Config) -> Self {
        let cfg = config.session_summary.clone();
        let default_ducklake = config.ducklake.clone();
        let interval = Duration::from_millis(cfg.rebuild_interval_ms.max(50));
        Self {
            maintenance: MaintenanceEngine::from_config(&config, scope_registry.clone()),
            scope_registry,
            default_ducklake,
            cfg,
            interval,
            cached_scopes: Mutex::new(Vec::new()),
            #[cfg(test)]
            fixed_scopes: None,
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
        let scopes = list_summary_scopes(
            &self.scope_registry,
            &self.default_ducklake,
            #[cfg(test)]
            &self.fixed_scopes,
        )
        .await?;
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let _ducklake = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key)
            .ok_or_else(|| anyhow!("unknown session_summary scope {scope_key}"))?;
        let to = Utc::now();
        let from = to - ChronoDuration::seconds(self.cfg.max_reduce_span_seconds as i64);
        let scope = self.maintenance.resolve_scope(scope_key).await?;
        self.maintenance
            .rebuild_session_summary(&scope, from, to, self.cfg.max_reduce_span_seconds)
            .await?;
        Ok(())
    }
}
