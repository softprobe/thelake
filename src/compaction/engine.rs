//! Physical-scope maintenance facade.

use crate::compaction::maint_conn_pool::MaintenanceConnPool;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::storage::ducklake::PhysicalScope;
use crate::workspace_scope::DEFAULT_WORKSPACE_ID;
use anyhow::{anyhow, Result};
use chrono::Utc;
use deadpool_postgres::Pool;
use duckdb::Connection;
use std::sync::Arc;
use tokio::sync::watch;

fn ensure_maintenance_lease_active(lease_lost: &watch::Receiver<bool>) -> Result<()> {
    if *lease_lost.borrow() {
        anyhow::bail!("maintenance lease lost")
    }
    Ok(())
}

/// Full ordered maintenance table list (traces / logs / scores).
pub fn maintenance_table_names() -> Vec<&'static str> {
    vec!["traces", "logs", "scores"]
}

#[derive(Clone)]
/// Physical-scope maintenance facade.
///
/// This is the only production owner of DuckDB connections used for
/// compaction, metadata cleanup, and session-summary reduction.
pub struct MaintenanceEngine {
    config: Config,
    default_physical: PhysicalScope,
    scope_registry: DuckLakeScopeResolver,
    conn_pool: Arc<MaintenanceConnPool>,
}

/// Opaque physical scope selected by `MaintenanceEngine`.
#[derive(Clone)]
pub(crate) struct MaintenanceScope {
    scope_key: String,
    physical: PhysicalScope,
    pool: Pool,
}

impl MaintenanceEngine {
    pub(crate) fn from_config(config: &Config, scope_registry: DuckLakeScopeResolver) -> Self {
        Self {
            config: config.clone(),
            default_physical: scope_registry.default_physical_scope().clone(),
            scope_registry,
            conn_pool: Arc::new(MaintenanceConnPool::new(config)),
        }
    }

    pub(crate) async fn new(
        config: &Config,
        scope_registry: DuckLakeScopeResolver,
    ) -> Result<Self> {
        Ok(Self::from_config(config, scope_registry))
    }

    /// Validate every physical scope before the service reports readiness.
    pub(crate) async fn validate_startup(&self) -> Result<()> {
        for (scope_key, physical) in self.physical_scopes().await? {
            self.conn_pool
                .with_conn(&physical, validate_newer_than_extension)
                .map_err(|error| anyhow!("maintenance open failed for {scope_key}: {error}"))?;
        }
        Ok(())
    }

    pub async fn run_once(&self) -> Result<()> {
        self.run_pass().await
    }

    /// Run the SQL maintenance script for each physical scope.
    pub async fn run_pass(&self) -> Result<()> {
        crate::self_monitoring::record_maintenance();
        let mut ensure_active = || Ok(());
        for (scope_key, physical) in self.physical_scopes().await? {
            self.run_physical_scope_pass_with_fence(
                &scope_key,
                &physical,
                None,
                None,
                &mut ensure_active,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn resolve_scope(&self, scope_key: &str) -> Result<MaintenanceScope> {
        let scope_key = crate::workspace_scope::effective_workspace_id(scope_key).to_string();
        // Shared mode weakly binds any workspace to the process default physical
        // scope. Isolated mode is fail-closed on the durable registry. The
        // synthetic `_default` key always maps to the process default warehouse.
        let physical = if scope_key == DEFAULT_WORKSPACE_ID {
            self.default_physical.clone()
        } else {
            self.scope_registry
                .resolve_scope_without_tables(&scope_key)
                .await
                .map_err(|_| anyhow!("unknown maintenance scope {scope_key}"))?
        };
        Ok(MaintenanceScope {
            scope_key,
            physical,
            pool: self.scope_registry.pool().clone(),
        })
    }

    pub async fn reduce_session_summary_for_key(
        &self,
        scope_key: &str,
        max_sessions: u64,
    ) -> Result<usize> {
        let scope = self.resolve_scope(scope_key).await?;
        self.reduce_session_summary(&scope, max_sessions).await
    }

    pub async fn rebuild_session_summary_for_key(
        &self,
        scope_key: &str,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        let scope = self.resolve_scope(scope_key).await?;
        self.rebuild_session_summary(&scope, from, to, max_reduce_span_seconds)
            .await
    }

    pub(crate) async fn reduce_session_summary(
        &self,
        scope: &MaintenanceScope,
        max_sessions: u64,
    ) -> Result<usize> {
        crate::session_summary::reduce_tenant(
            &scope.pool,
            scope.physical.pg_namespace(),
            &scope.scope_key,
            &self.config,
            &scope.physical,
            Arc::clone(&self.conn_pool),
            max_sessions,
        )
        .await
    }

    pub(crate) async fn rebuild_session_summary(
        &self,
        scope: &MaintenanceScope,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        crate::session_summary::rebuild_tenant_window(
            &scope.pool,
            scope.physical.pg_namespace(),
            &self.config,
            &scope.physical,
            Arc::clone(&self.conn_pool),
            &scope.scope_key,
            from,
            to,
            max_reduce_span_seconds,
        )
        .await
    }

    pub(crate) async fn workspace_scopes(&self) -> Result<Vec<(String, PhysicalScope)>> {
        let mut scopes = Vec::new();
        let default = self.default_physical.clone();
        let mut saw_default = false;
        for (scope_id, scope) in self.scope_registry.list_scopes().await? {
            if scope.same_warehouse_as(&default) {
                saw_default = true;
            }
            scopes.push((scope_id, scope));
        }
        if !saw_default {
            scopes.insert(0, (DEFAULT_WORKSPACE_ID.to_string(), default));
        }
        Ok(scopes)
    }

    /// Workspace ids for per-tenant jobs (session_summary). Not warehouse-deduped.
    pub async fn workspace_scope_keys(&self) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .workspace_scopes()
            .await?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        // Local anonymous mode intentionally has no workspace registry or
        // provisioning API. Its configured workspace still needs per-workspace
        // jobs (session-summary reduction/rebuild) to consume its dirty rows.
        if let Some(workspace_id) = crate::runtime_api::local_anonymous_workspace_id()
            .map_err(|status| anyhow!("invalid local anonymous workspace: {status}"))?
        {
            if !keys.contains(&workspace_id) {
                keys.push(workspace_id);
            }
        }
        Ok(keys)
    }

    pub(crate) async fn physical_scopes(&self) -> Result<Vec<(String, PhysicalScope)>> {
        Ok(deduplicate_physical_scopes(self.workspace_scopes().await?))
    }

    /// Deduped physical-scope keys for lake maintenance (one pass per warehouse).
    pub async fn maintenance_scope_keys(&self) -> Result<Vec<String>> {
        Ok(self
            .physical_scopes()
            .await?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    async fn lookup_physical_scope(&self, scope_key: &str) -> Result<PhysicalScope> {
        Ok(self.resolve_scope(scope_key).await?.physical)
    }

    /// Resolve by maintenance key, then run one SQL maintenance pass.
    pub async fn run_pass_for_key(&self, scope_key: &str) -> Result<()> {
        let physical = self.lookup_physical_scope(scope_key).await?;
        let mut ensure_active = || Ok(());
        self.run_physical_scope_pass_with_fence(
            scope_key,
            &physical,
            None,
            None,
            &mut ensure_active,
        )
        .await
    }

    pub(crate) async fn run_pass_for_key_fenced(
        &self,
        scope_key: &str,
        lease_token: &crate::async_jobs::LeaseToken,
        lease_lost: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ensure_active = || ensure_maintenance_lease_active(&lease_lost);
        let script_lease_lost = lease_lost.clone();
        ensure_active()?;
        let physical = self.lookup_physical_scope(scope_key).await?;
        ensure_active()?;
        self.run_physical_scope_pass_with_fence(
            scope_key,
            &physical,
            Some(lease_token),
            Some(script_lease_lost),
            &mut ensure_active,
        )
        .await
    }

    /// One SQL maintenance script for a physical scope.
    async fn run_physical_scope_pass_with_fence(
        &self,
        scope_key: &str,
        physical: &PhysicalScope,
        lease_token: Option<&crate::async_jobs::LeaseToken>,
        lease_lost: Option<watch::Receiver<bool>>,
        ensure_active: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let started = std::time::Instant::now();
        ensure_active()?;
        let lease_epoch = lease_token.map(|token| token.epoch).unwrap_or(0);
        let sql = crate::sql::maintenance::render_maintenance_sql(
            &crate::sql::maintenance::MaintenanceSqlParams {
                registry_schema: self.scope_registry.registry_schema(),
                scope_key,
                lease_epoch,
            },
        );
        let ((), cold, open_elapsed) = self.conn_pool.with_conn(physical, |conn| {
            let lease_job = lease_token.map(|_| crate::compaction::PHYSICAL_SCOPE_MAINTENANCE_JOB);
            let lease_holder = lease_token.map(|token| token.holder_id.as_str());
            conn.execute(
                &crate::sql::maintenance::scope_config_upsert_sql(
                    self.scope_registry.registry_schema(),
                ),
                duckdb::params![
                    scope_key,
                    physical.attach_alias(),
                    physical.pg_namespace(),
                    self.config.maintenance.enabled,
                    self.config.maintenance.metadata_enabled,
                    self.config.maintenance.reader_safety_grace_seconds as i64,
                    lease_job,
                    lease_holder,
                    lease_epoch,
                ],
            )?;
            execute_maintenance_script(conn, &sql, lease_lost.as_ref())?;
            Ok(())
        })?;
        crate::self_monitoring::record_maintenance_step(
            scope_key,
            if cold {
                crate::self_monitoring::maintenance_step::OPEN_ATTACH
            } else {
                crate::self_monitoring::maintenance_step::OPEN_ATTACH_WARM
            },
            None,
            open_elapsed,
        );
        ensure_active()?;
        crate::self_monitoring::record_maintenance_step(
            scope_key,
            crate::self_monitoring::maintenance_step::PASS_TOTAL,
            None,
            started.elapsed(),
        );
        Ok(())
    }
}

fn execute_maintenance_script(
    conn: &Connection,
    sql: &str,
    lease_lost: Option<&watch::Receiver<bool>>,
) -> Result<()> {
    let Some(lease_lost) = lease_lost else {
        return crate::sql::execute_maintenance_script(conn, sql);
    };

    let interrupt = conn.interrupt_handle();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let lease_lost = lease_lost.clone();
    let watcher_lease_lost = lease_lost.clone();
    let watcher = std::thread::spawn(move || loop {
        if *watcher_lease_lost.borrow() {
            interrupt.interrupt();
            break;
        }
        match finished_rx.recv_timeout(std::time::Duration::from_millis(25)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    });

    let result = crate::sql::execute_maintenance_script(conn, sql);
    let _ = finished_tx.send(());
    let _ = watcher.join();
    if *lease_lost.borrow() {
        anyhow::bail!("maintenance lease lost");
    }
    result
}

fn validate_newer_than_extension(conn: &Connection) -> Result<()> {
    let supports_newer_than: bool = conn.query_row(
        crate::sql::maintenance::newer_than_capability_sql(),
        [],
        |row| row.get(0),
    )?;
    if !supports_newer_than {
        anyhow::bail!("loaded DuckLake extension lacks ducklake_merge_adjacent_files(newer_than)");
    }
    Ok(())
}

pub(crate) fn deduplicate_physical_scopes(
    workspace_scopes: Vec<(String, PhysicalScope)>,
) -> Vec<(String, PhysicalScope)> {
    let mut seen = std::collections::HashSet::new();
    let mut physical = Vec::new();
    for (_workspace_id, scope) in workspace_scopes {
        let key = scope.id().registry_token().to_string();
        if seen.insert(key.clone()) {
            physical.push((key, scope));
        }
    }
    physical
}
