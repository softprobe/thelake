//! Physical-scope maintenance as a shared [`Job`].
//! Each pass runs enabled metadata cleanup and TWCS together (TWCS no-ops when
//! the lake has nothing to merge).

use crate::async_jobs::Job;
#[cfg(test)]
use crate::compaction::executor::deduplicate_physical_scopes;
use crate::compaction::executor::MaintenanceEngine;
use crate::config::DuckLakeConfig;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
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

pub const PHYSICAL_SCOPE_MAINTENANCE_JOB: &str = "physical_scope_maintenance";

pub struct PhysicalScopeMaintenanceJob {
    executor: MaintenanceEngine,
    interval: Duration,
    compaction_enabled: bool,
    cached_scopes: Mutex<Vec<(String, DuckLakeConfig)>>,
    /// When set, `scope_keys` returns these instead of asking the registry
    /// (multi-tenant fixtures without Postgres).
    #[cfg(test)]
    fixed_scopes: Option<Vec<(String, DuckLakeConfig)>>,
}

impl PhysicalScopeMaintenanceJob {
    pub fn new(executor: MaintenanceEngine, interval: Duration, compaction_enabled: bool) -> Self {
        Self {
            executor,
            interval: interval.max(Duration::from_millis(50)),
            compaction_enabled,
            cached_scopes: Mutex::new(Vec::new()),
            #[cfg(test)]
            fixed_scopes: None,
        }
    }

    /// Pin tenant scopes for tests (avoids needing a Postgres scope registry).
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn with_scopes(
        executor: MaintenanceEngine,
        interval: Duration,
        compaction_enabled: bool,
        scopes: Vec<(String, DuckLakeConfig)>,
    ) -> Self {
        let mut job = Self::new(executor, interval, compaction_enabled);
        job.fixed_scopes = Some(scopes);
        job
    }
}

#[async_trait]
impl Job for PhysicalScopeMaintenanceJob {
    fn name(&self) -> &'static str {
        PHYSICAL_SCOPE_MAINTENANCE_JOB
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        #[cfg(test)]
        let scopes = match &self.fixed_scopes {
            Some(fixed) => deduplicate_physical_scopes(fixed.clone()),
            None => self.executor.physical_scopes().await?,
        };
        #[cfg(not(test))]
        let scopes = self.executor.physical_scopes().await?;
        let ids: Vec<String> = scopes.iter().map(|(id, _)| id.clone()).collect();
        *lock_mutex(&self.cached_scopes) = scopes;
        Ok(ids)
    }

    async fn run(&self, scope_key: &str) -> Result<()> {
        let ducklake = lookup_cached_scope(&lock_mutex(&self.cached_scopes), scope_key)
            .ok_or_else(|| anyhow!("unknown maintenance scope {scope_key}"))?;
        self.executor
            .ensure_physical_scope_bootstrap(&ducklake)
            .await?;
        let pass = self
            .executor
            .run_physical_scope_pass(scope_key, &ducklake, self.compaction_enabled)
            .await;
        if pass.is_ok() {
            crate::self_monitoring::record_maintenance();
        }
        pass.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(lookup_cached_scope(&cached, "new-tenant").is_none());
    }

    #[test]
    fn lock_mutex_recovers_from_poison() {
        let m = Mutex::new(1u32);
        let _ = std::panic::catch_unwind(|| {
            let _g = m.lock().unwrap();
            panic!("poison");
        });
        assert!(m.lock().is_err());
        *lock_mutex(&m) = 2;
        assert_eq!(*lock_mutex(&m), 2);
    }

    #[tokio::test]
    async fn fixed_workspace_bindings_share_one_physical_job_key() {
        let config = crate::config::Config::default();
        let shared = crate::config::DuckLakeConfig {
            metadata_schema: "shared_scope".into(),
            data_path: "s3://warehouse/shared".into(),
            ..config.ducklake.clone()
        };
        let resolver = crate::runtime_engine::DuckLakeScopeResolver::connect(&config)
            .await
            .expect("connect resolver");
        let job = PhysicalScopeMaintenanceJob::with_scopes(
            MaintenanceEngine::new(&config, resolver)
                .await
                .expect("engine"),
            Duration::from_secs(60),
            false,
            vec![
                ("workspace-a".into(), shared.clone()),
                ("workspace-b".into(), shared),
            ],
        );

        let keys = job.scope_keys().await.expect("scope keys");
        assert_eq!(keys.len(), 1);
        assert!(keys[0].starts_with("ducklake:"));
        assert_eq!(job.name(), PHYSICAL_SCOPE_MAINTENANCE_JOB);
    }
}
