//! Process-wide registry of [`CatalogPool`]s, owned by `WorkspaceManager`.
//!
//! Shared mode serves one physical catalog, so it holds exactly one pool and
//! rejects any other scope. Isolated mode keeps a weak pool per scope: the
//! workspace facades hold the strong references, so invalidating a workspace
//! drops its catalog's sessions.

use crate::config::Config;
use crate::storage::ducklake::physical_scope::ScopeId;
use crate::storage::ducklake::{CatalogPool, DevReset, PhysicalScope};
use crate::workspace::DuckLakeScopeResolver;
use crate::workspace_scope::WorkspaceScopeMode;
use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};

enum Registry {
    Shared(OnceLock<Arc<CatalogPool>>),
    Isolated(Mutex<HashMap<ScopeId, Weak<CatalogPool>>>),
}

struct Inner {
    config: Arc<Config>,
    resolver: DuckLakeScopeResolver,
    registry: Registry,
    creating: tokio::sync::Mutex<()>,
    /// Scopes whose dev reset already ran on this registry. A later pool open
    /// must not wipe the lake again.
    reset_done: Mutex<HashSet<ScopeId>>,
}

#[derive(Clone)]
pub(crate) struct CatalogSessions {
    inner: Arc<Inner>,
}

impl CatalogSessions {
    pub(crate) fn new(config: Arc<Config>, resolver: DuckLakeScopeResolver) -> Self {
        let registry = match config.ducklake.workspace_scope_mode {
            WorkspaceScopeMode::Shared => Registry::Shared(OnceLock::new()),
            WorkspaceScopeMode::Isolated => Registry::Isolated(Mutex::new(HashMap::new())),
        };
        Self {
            inner: Arc::new(Inner {
                config,
                resolver,
                registry,
                creating: tokio::sync::Mutex::new(()),
                reset_done: Mutex::new(HashSet::new()),
            }),
        }
    }

    pub(crate) async fn pool_for(&self, scope: &PhysicalScope) -> Result<Arc<CatalogPool>> {
        match &self.inner.registry {
            Registry::Shared(cell) => {
                if scope.id() != self.inner.resolver.default_physical_scope().id() {
                    bail!(
                        "shared workspace scope mode serves one physical scope; {scope} is not it"
                    );
                }
                if let Some(pool) = cell.get() {
                    return Ok(Arc::clone(pool));
                }
                let _creating = self.inner.creating.lock().await;
                if let Some(pool) = cell.get() {
                    return Ok(Arc::clone(pool));
                }
                let pool = self.create(scope).await?;
                Ok(Arc::clone(cell.get_or_init(|| pool)))
            }
            Registry::Isolated(pools) => {
                let id = scope.id();
                if let Some(pool) = live_pool(pools, &id) {
                    return Ok(pool);
                }
                let _creating = self.inner.creating.lock().await;
                if let Some(pool) = live_pool(pools, &id) {
                    return Ok(pool);
                }
                let pool = self.create(scope).await?;
                let mut pools = pools.lock().unwrap_or_else(|p| p.into_inner());
                pools.retain(|_, weak| weak.strong_count() > 0);
                pools.insert(id, Arc::downgrade(&pool));
                Ok(pool)
            }
        }
    }

    /// The dev reset belongs to the first pool open on this registry. A second
    /// workspace, and a later reopen after the last facade drops the pool,
    /// must not wipe the lake. A new `CatalogSessions` still resets once.
    async fn create(&self, scope: &PhysicalScope) -> Result<Arc<CatalogPool>> {
        let already_reset = self
            .inner
            .reset_done
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&scope.id());
        let reset = if CatalogPool::dev_reset_requested() && !already_reset {
            let schema = scope.pg_namespace();
            Some(DevReset {
                telemetry: self
                    .inner
                    .resolver
                    .load_active_telemetry_columns_manifests_for_scope(schema)
                    .await?,
                business: self
                    .inner
                    .resolver
                    .load_active_business_table_manifests_for_scope(schema)
                    .await?,
            })
        } else {
            None
        };
        let pool = CatalogPool::open(&self.inner.config, scope, reset).await?;
        if CatalogPool::dev_reset_requested() {
            self.inner
                .reset_done
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(scope.id());
        }
        Ok(pool)
    }

    fn live_pools(&self) -> Vec<Arc<CatalogPool>> {
        match &self.inner.registry {
            Registry::Shared(cell) => cell.get().cloned().into_iter().collect(),
            Registry::Isolated(pools) => pools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .filter_map(Weak::upgrade)
                .collect(),
        }
    }

    pub(crate) fn pool_count_for_test(&self) -> usize {
        self.live_pools().len()
    }

    pub(crate) fn session_count_for_test(&self) -> usize {
        self.live_pools()
            .iter()
            .map(|pool| pool.session_count_for_test())
            .sum()
    }
}

fn live_pool(
    pools: &Mutex<HashMap<ScopeId, Weak<CatalogPool>>>,
    id: &ScopeId,
) -> Option<Arc<CatalogPool>> {
    pools
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(id)
        .and_then(Weak::upgrade)
}
