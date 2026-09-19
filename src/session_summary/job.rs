//! `session_summary.reduce` leased job on the shared async runner.

use crate::async_jobs::Job;
use crate::config::{DuckLakeConfig, SessionSummaryConfig};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::session_summary::hot_attrs::ensure_product_hot_attrs_for_scope;
use crate::session_summary::reduce::reduce_tenant;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

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

/// Leased reducer: claim dirty → lake aggregate → UPSERT summary → ack.
pub struct SessionSummaryReduceJob {
    pool: Pool,
    scope_registry: Option<DuckLakeScopeResolver>,
    default_ducklake: DuckLakeConfig,
    cfg: SessionSummaryConfig,
    interval: Duration,
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
    #[cfg(test)]
    fixed_scopes: Option<Vec<(String, DuckLakeConfig)>>,
}

impl SessionSummaryReduceJob {
    pub fn new(
        pool: Pool,
        scope_registry: Option<DuckLakeScopeResolver>,
        default_ducklake: DuckLakeConfig,
        cfg: SessionSummaryConfig,
    ) -> Self {
        let interval = Duration::from_millis(cfg.reducer_interval_ms.max(50));
        Self {
            pool,
            scope_registry,
            default_ducklake,
            cfg,
            interval,
            cached_scopes: Mutex::new(Vec::new()),
            #[cfg(test)]
            fixed_scopes: None,
        }
    }

    #[cfg(test)]
    pub fn with_scopes(
        pool: Pool,
        default_ducklake: DuckLakeConfig,
        cfg: SessionSummaryConfig,
        scopes: Vec<(String, DuckLakeConfig)>,
    ) -> Self {
        let mut job = Self::new(pool, None, default_ducklake, cfg);
        job.fixed_scopes = Some(scopes);
        job
    }

    async fn list_scopes(&self) -> Result<Vec<(String, DuckLakeConfig)>> {
        #[cfg(test)]
        if let Some(fixed) = &self.fixed_scopes {
            return Ok(fixed.clone());
        }
        let mut scopes = Vec::new();
        let default = self.default_ducklake.clone();
        let mut saw_default = false;
        if let Some(registry) = &self.scope_registry {
            for (scope_id, scope) in registry.list_scopes().await? {
                let mut dk = default.clone();
                dk.metadata_schema = scope.metadata_schema;
                dk.data_path = scope.data_path;
                if dk.metadata_schema == default.metadata_schema
                    && dk.data_path == default.data_path
                {
                    saw_default = true;
                }
                scopes.push((scope_id, dk));
            }
        }
        if !saw_default {
            scopes.insert(0, ("_default".to_string(), default));
        }
        Ok(scopes)
    }
}

#[async_trait]
impl Job for SessionSummaryReduceJob {
    fn name(&self) -> &'static str {
        "session_summary.reduce"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        let scopes = self.list_scopes().await?;
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let ducklake = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key)
            .ok_or_else(|| anyhow!("unknown session_summary scope {scope_key}"))?;
        if let Some(registry) = &self.scope_registry {
            let scope = DuckLakeScope {
                metadata_schema: ducklake.metadata_schema.clone(),
                data_path: ducklake.data_path.clone(),
            };
            ensure_product_hot_attrs_for_scope(registry, &scope).await?;
        }
        reduce_tenant(
            &self.pool,
            &ducklake.metadata_schema,
            scope_key,
            &ducklake,
            self.cfg.max_sessions_per_reduce,
            self.cfg.max_reduce_span_seconds,
        )
        .await?;
        Ok(())
    }
}
