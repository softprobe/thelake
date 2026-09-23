// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity appears only at auth -> RuntimeEngine mapping.
// Operational APIs MUST NOT accept tenant_id or scope parameters.
// ============================================================================

//! Per-tenant [`RuntimeEngine`] cache.

use crate::authn::TenantInfo;
use crate::config::{Config, DuckLakeConfig};
use crate::control_plane::ControlPlaneRuntime;
use crate::ingest_engine::{AdminEngine, IngestEngine, IngestPipeline};
use crate::promotion::{
    business_manifest_from_row, business_spec_activation, ensure_promotion_metadata_tables,
    load_active_telemetry_columns_manifests, run_business_apply, run_telemetry_apply,
    telemetry_spec_activation, BusinessApplyError, BusinessTableManifest, PromotionSpecActivation,
    PromotionSpecLoadError, TelemetryColumnsManifest,
};
use crate::query::{self as query_mod, QueryEngine};
use crate::workspace_scope::{PhysicalScope, WorkspaceBinding, WorkspaceScopeMode};
use anyhow::{anyhow, bail, Context, Result};
use dashmap::DashMap;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

pub type TenantId = String;

const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;

fn validate_metadata_schema_name(schema: &str) -> Result<()> {
    if schema.len() > POSTGRES_IDENTIFIER_MAX_BYTES {
        bail!(
            "ducklake metadata schema exceeds PostgreSQL's {}-byte identifier limit",
            POSTGRES_IDENTIFIER_MAX_BYTES
        );
    }
    Ok(())
}

/// One tenant's canonical bound ingest and query surfaces.
pub struct RuntimeEngine {
    tenant_id: String,
    binding: WorkspaceBinding,
    catalog_pool: Pool,
    ingest: Arc<IngestEngine>,
    admin: Arc<AdminEngine>,
    query: Arc<QueryEngine>,
}

/// Internal summary-query capability. It carries only what the summary
/// module needs; callers do not receive the tenant's physical binding.
pub(crate) struct TenantSummaryScope {
    pub(crate) pool: Pool,
    pub(crate) metadata_schema: String,
    pub(crate) workspace_id: Option<String>,
}

impl RuntimeEngine {
    /// Stable logical identity of this tenant-bound runtime.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Tenant-scoped write API. Storage scope and buffering are fixed when the
    /// runtime is built and are not part of this operation's contract.
    pub async fn add_spans(
        &self,
        items: Vec<crate::models::Span>,
        request_size: usize,
    ) -> Result<()> {
        self.ingest.add_spans(items, request_size).await
    }

    pub async fn execute_query(&self, sql: &str) -> Result<crate::query::duckdb::QueryResult> {
        self.query.execute_query(sql).await
    }

    pub(crate) async fn execute_trusted(
        &self,
        query: crate::sql::trusted::TrustedSql,
    ) -> Result<crate::query::duckdb::QueryResult> {
        self.query.execute_trusted(query).await
    }

    pub async fn add_logs(
        &self,
        items: Vec<crate::models::Log>,
        request_size: usize,
    ) -> Result<()> {
        self.ingest.add_logs(items, request_size).await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.ingest.force_flush_logs().await
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.ingest.force_flush_spans().await
    }

    pub async fn add_scores(&self, items: Vec<crate::models::Score>) -> Result<()> {
        self.ingest.add_scores(items).await
    }

    pub async fn score_exists(&self, score_id: &str) -> Result<bool> {
        self.ingest.score_exists(score_id).await
    }

    pub async fn list_score_configs(&self) -> Result<Vec<crate::models::ScoreConfig>> {
        self.ingest.list_score_configs().await
    }

    pub async fn add_score_configs(&self, items: Vec<crate::models::ScoreConfig>) -> Result<()> {
        self.ingest.add_score_configs(items).await
    }

    pub async fn score_config_exists(&self, config_id: &str) -> Result<bool> {
        self.ingest.score_config_exists(config_id).await
    }

    pub async fn get_score_config(
        &self,
        config_id: &str,
    ) -> Result<Option<crate::models::ScoreConfig>> {
        self.ingest.get_score_config(config_id).await
    }

    pub async fn apply_telemetry_promotion(
        &self,
        manifest_yaml: &str,
        spec: &TelemetryColumnsManifest,
        target_tables: &[String],
    ) -> Result<String> {
        self.admin
            .apply_and_record_telemetry_promotion(manifest_yaml, spec, target_tables)
            .await
    }

    pub async fn apply_business_promotion(
        &self,
        manifest_yaml: &str,
        spec: &BusinessTableManifest,
    ) -> std::result::Result<String, BusinessApplyError> {
        self.admin
            .apply_business_promotion_guarded(manifest_yaml, spec)
            .await
    }

    /// Internal query capability for protocol adapters and trusted query
    /// builders. Callers cannot inspect the bound physical scope.
    pub(crate) fn query_engine(&self) -> Arc<QueryEngine> {
        self.query.clone()
    }

    pub(crate) fn session_summary_scope(&self) -> TenantSummaryScope {
        TenantSummaryScope {
            pool: self.catalog_pool.clone(),
            metadata_schema: self.binding.physical_scope.metadata_schema.clone(),
            workspace_id: (self.binding.mode == WorkspaceScopeMode::Shared)
                .then(|| self.binding.workspace_id.clone()),
        }
    }
}

/// Global cache: `tenantId` -> tenant-bound runtime (unbounded until restart).
pub struct RuntimeEngineManager {
    config: Arc<Config>,
    engines: DashMap<String, Arc<RuntimeEngine>>,
    creation_locks: DashMap<String, Arc<Mutex<()>>>,
    control_plane: Option<ControlPlaneRuntime>,
    scope_registry: DuckLakeScopeResolver,
    #[cfg(test)]
    build_counter: AtomicUsize,
}

impl RuntimeEngineManager {
    /// Connect the catalog registry and build the process-wide engine cache.
    pub async fn connect(
        config: Arc<Config>,
        control_plane: Option<ControlPlaneRuntime>,
    ) -> Result<Self> {
        let scope_registry = DuckLakeScopeResolver::connect(config.as_ref()).await?;
        Ok(Self {
            config,
            engines: DashMap::new(),
            creation_locks: DashMap::new(),
            control_plane,
            scope_registry,
            #[cfg(test)]
            build_counter: AtomicUsize::new(0),
        })
    }

    pub fn control_plane(&self) -> Option<&ControlPlaneRuntime> {
        self.control_plane.as_ref()
    }

    pub fn config(&self) -> &Config {
        self.config.as_ref()
    }

    /// Crate-internal registry handle for ingest/maintenance composition.
    pub(crate) fn scope_registry(&self) -> &DuckLakeScopeResolver {
        &self.scope_registry
    }

    /// Idempotently create or verify a workspace → physical-scope binding.
    pub async fn provision_scope(
        &self,
        request: ScopeProvisioningRequest,
    ) -> Result<PhysicalScope> {
        self.scope_registry.provision_scope(request).await
    }

    /// Resolve an existing workspace binding's physical scope.
    pub(crate) async fn resolve_scope(&self, scope_id: &str) -> Result<PhysicalScope> {
        self.scope_registry.resolve_scope(scope_id).await
    }

    pub fn list_cached_tenant_ids(&self) -> Vec<String> {
        self.engines.iter().map(|e| e.key().clone()).collect()
    }

    /// Return a cached engine without building (for opportunistic gauges).
    pub fn cached_engine(&self, tenant_id: &str) -> Option<Arc<RuntimeEngine>> {
        self.engines.get(tenant_id).map(|e| e.clone())
    }

    /// Drop cached engine (e.g. after provisioning changes scope).
    pub fn invalidate(&self, tenant_id: &str) {
        self.engines.remove(tenant_id);
    }

    #[cfg(test)]
    pub fn build_count(&self) -> usize {
        self.build_counter.load(Ordering::Relaxed)
    }

    /// Resolve registry scope (when configured) and return or construct a cached [`RuntimeEngine`].
    pub async fn engine_for(&self, tenant_id: &str) -> Result<Arc<RuntimeEngine>> {
        if let Some(r) = self.engines.get(tenant_id) {
            return Ok(r.clone());
        }
        let lock = self
            .creation_locks
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _hold = lock.lock().await;
        if let Some(r) = self.engines.get(tenant_id) {
            return Ok(r.clone());
        }
        let engine = self.build_engine(tenant_id).await?;
        self.engines.insert(tenant_id.to_string(), engine.clone());
        Ok(engine)
    }

    pub async fn engine_for_tenant(&self, tenant: &TenantInfo) -> Result<Arc<RuntimeEngine>> {
        self.engine_for(&tenant.tenant_id).await
    }

    async fn build_engine(&self, tenant_id: &str) -> Result<Arc<RuntimeEngine>> {
        #[cfg(test)]
        self.build_counter.fetch_add(1, Ordering::Relaxed);

        // The unauthenticated local/default runtime has no external tenant ID, but
        // workspace-bound engines still need a non-empty identity for their access
        // contract. Keep the empty ID only for resolving the configured default scope.
        let bound_tenant_id = if tenant_id.trim().is_empty() {
            "_default"
        } else {
            tenant_id
        };

        let resolver = &self.scope_registry;
        let binding = resolver.resolve_or_create_binding(tenant_id).await?;
        let counts_toward_liveness = true;

        let ingest = IngestPipeline::build_tenant_ingest(
            self.config.as_ref(),
            resolver.clone(),
            binding.clone(),
        )
        .await?;
        ingest.ensure_shared_schema().await?;
        let query = Arc::new(
            query_mod::create_query_engine_for_scope_with_liveness(
                self.config.as_ref(),
                &binding.physical_scope,
                counts_toward_liveness,
                bound_tenant_id,
            )
            .await?,
        );
        let admin = Arc::new(AdminEngine::from_ingest(&ingest));
        Ok(Arc::new(RuntimeEngine {
            tenant_id: bound_tenant_id.to_string(),
            binding,
            catalog_pool: resolver.pool().clone(),
            ingest,
            admin,
            query,
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeProvisioningRequest {
    pub scope_id: String,
    pub metadata_schema: String,
    pub data_path: String,
}

/// Process-wide Postgres registry for workspace → physical-scope bindings.
///
/// Owned only by [`RuntimeEngineManager`]. Not part of the public crate API —
/// request handlers use [`RuntimeEngineManager::engine_for`]; admin/ops use the
/// manager facades (`provision_scope`, `catalog_pool`, …).
#[derive(Clone)]
pub(crate) struct DuckLakeScopeResolver {
    pool: Pool,
    registry_schema: String,
    default_physical_scope: PhysicalScope,
    workspace_scope_mode: WorkspaceScopeMode,
}

impl DuckLakeScopeResolver {
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    pub(crate) fn registry_schema(&self) -> &str {
        &self.registry_schema
    }

    /// Always connects on the normal runtime path: `metadata_path` is the
    /// Postgres connection string for both the control-plane registry and the
    /// DuckLake catalog. There is no config flag to opt out.
    pub(crate) async fn connect(config: &Config) -> Result<Self> {
        let dl = &config.ducklake;
        let resolver = Self::build_pool(dl)?;
        resolver.ensure_registry().await?;
        resolver.ensure_scope().await?;
        Ok(resolver)
    }

    fn build_pool(dl: &DuckLakeConfig) -> Result<Self> {
        let mut pg = tokio_postgres::Config::new();
        parse_postgres_kv_config(&mut pg, &dl.metadata_path)?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(8).build()?;
        Ok(Self {
            pool,
            registry_schema: dl.metadata_schema.clone(),
            default_physical_scope: PhysicalScope::from_ducklake(dl),
            workspace_scope_mode: dl.workspace_scope_mode,
        })
    }

    async fn ensure_scope(&self) -> Result<()> {
        self.ensure_scope_tables(&self.default_physical_scope).await
    }

    async fn ensure_registry(&self) -> Result<()> {
        let mut client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.scope_registry (
  scope_id TEXT PRIMARY KEY,
  ducklake_metadata_schema TEXT NOT NULL UNIQUE,
  data_path TEXT NOT NULL,
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.physical_scope (
  physical_scope_id TEXT PRIMARY KEY,
  metadata_path TEXT NOT NULL,
  ducklake_metadata_schema TEXT NOT NULL,
  data_path TEXT NOT NULL,
  catalog_alias TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (metadata_path, catalog_alias,
          ducklake_metadata_schema, data_path)
);"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    "ALTER TABLE {}.physical_scope DROP COLUMN IF EXISTS catalog_type;",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.workspace_scope_binding (
  workspace_id TEXT PRIMARY KEY,
  physical_scope_id TEXT NOT NULL REFERENCES {}.physical_scope(physical_scope_id),
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);"#,
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.thelake_job_lease (
  job_name TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  holder_id TEXT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_name, scope_key)
);"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE INDEX IF NOT EXISTS thelake_job_lease_until
ON {}.thelake_job_lease (lease_until);"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        let transaction = client.transaction().await?;
        self.migrate_legacy_scope_registry(&transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn migrate_legacy_scope_registry<C>(&self, client: &C) -> Result<()>
    where
        C: deadpool_postgres::GenericClient + Sync,
    {
        let rows = client
            .query(
                &format!(
                    "SELECT scope_id, ducklake_metadata_schema, data_path FROM {}.scope_registry;",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        for row in rows {
            let workspace_id: String = row.get(0);
            let mut physical = self.default_physical_scope.clone();
            physical.metadata_schema = row.get(1);
            physical.data_path = row.get(2);
            self.insert_physical_scope(client, &physical).await?;
            let physical_scope_id = physical.key();
            client
                .execute(
                    &format!(
                        "INSERT INTO {}.workspace_scope_binding (workspace_id, physical_scope_id) \
                         VALUES ($1, $2) ON CONFLICT (workspace_id) DO NOTHING;",
                        quote_pg_ident(&self.registry_schema)
                    ),
                    &[&workspace_id, &physical_scope_id],
                )
                .await?;
        }
        Ok(())
    }

    async fn insert_physical_scope(
        &self,
        client: &impl deadpool_postgres::GenericClient,
        physical: &PhysicalScope,
    ) -> Result<()> {
        let physical_scope_id = physical.key();
        client
            .execute(
                &format!(
                    r#"INSERT INTO {}.physical_scope
  (physical_scope_id, metadata_path, ducklake_metadata_schema, data_path, catalog_alias)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (physical_scope_id) DO UPDATE SET updated_at = NOW();"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[
                    &physical_scope_id,
                    &physical.metadata_path,
                    &physical.metadata_schema,
                    &physical.data_path,
                    &physical.catalog_alias,
                ],
            )
            .await?;
        Ok(())
    }

    async fn ensure_scope_tables(&self, scope: &PhysicalScope) -> Result<()> {
        let client = self.pool.get().await?;
        ensure_promotion_metadata_tables(&client, &scope.metadata_schema).await?;
        if self.workspace_scope_mode == WorkspaceScopeMode::Shared {
            crate::session_summary::ensure_shared_session_summary_tables(
                &client,
                &scope.metadata_schema,
            )
            .await?;
            crate::session_summary::validate_shared_session_summary_tables(
                &client,
                &scope.metadata_schema,
            )
            .await?;
        } else {
            crate::session_summary::ensure_session_summary_tables(&client, &scope.metadata_schema)
                .await?;
        }
        drop(client);
        // Product-hot activation is physical-scope-scoped and idempotent. Run
        // it at scope initialization so summary jobs remain correct even when
        // the periodic maintenance job is disabled; maintenance repeats the
        // same guarded operation for recovery after restarts.
        crate::session_summary::ensure_product_hot_attrs_for_scope(self, scope).await?;
        Ok(())
    }

    /// Resolve the DuckLake scope for `scope_id` from the durable registry.
    ///
    /// When `scope_id` is empty, returns the process-default scope.
    pub async fn resolve_or_create(&self, scope_id: &str) -> Result<PhysicalScope> {
        let binding = self.resolve_or_create_binding(scope_id).await?;
        Ok(binding.physical_scope)
    }

    /// Resolve a workspace binding from the durable registry.
    pub async fn resolve_or_create_binding(&self, workspace_id: &str) -> Result<WorkspaceBinding> {
        if workspace_id.trim().is_empty() {
            self.ensure_scope().await?;
            return WorkspaceBinding::new(
                "_default",
                self.default_physical_scope.clone(),
                self.workspace_scope_mode,
            )
            .map_err(Into::into);
        }
        let binding = self.resolve_binding(workspace_id).await?;
        self.ensure_scope_tables(&binding.physical_scope).await?;
        Ok(binding)
    }

    async fn resolve_binding(&self, workspace_id: &str) -> Result<WorkspaceBinding> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT ps.metadata_path, ps.ducklake_metadata_schema, ps.data_path, \
                            ps.catalog_alias \
                     FROM {}.workspace_scope_binding wb \
                     JOIN {}.physical_scope ps ON ps.physical_scope_id = wb.physical_scope_id \
                     WHERE wb.workspace_id = $1;",
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema)
                ),
                &[&workspace_id],
            )
            .await?;
        let Some(row) = row else {
            bail!("unknown scope: {workspace_id}");
        };
        let physical = PhysicalScope {
            metadata_path: row.get(0),
            metadata_schema: row.get(1),
            data_path: row.get(2),
            catalog_alias: row.get(3),
        };
        WorkspaceBinding::new(workspace_id, physical, self.workspace_scope_mode).map_err(Into::into)
    }

    async fn resolve_scope_legacy(&self, scope_id: &str) -> Result<PhysicalScope> {
        if scope_id.trim().is_empty() {
            self.ensure_scope().await?;
            return Ok(self.default_physical_scope.clone());
        }
        Ok(self.resolve_binding(scope_id).await?.physical_scope)
    }

    /// Idempotently create or verify a scope registry entry and its metadata tables.
    pub async fn provision_scope(
        &self,
        request: ScopeProvisioningRequest,
    ) -> Result<PhysicalScope> {
        if request.scope_id.trim().is_empty() {
            bail!("scope_id is required");
        }
        if request.metadata_schema.trim().is_empty() {
            bail!("ducklake metadata schema is required");
        }
        validate_metadata_schema_name(&request.metadata_schema)?;
        if request.data_path.trim().is_empty() {
            bail!("ducklake data path is required");
        }

        let mut scope = self.default_physical_scope.clone();
        scope.metadata_schema = request.metadata_schema;
        scope.data_path = request.data_path;
        if self.workspace_scope_mode == WorkspaceScopeMode::Shared
            && (scope.metadata_schema != self.default_physical_scope.metadata_schema
                || scope.data_path != self.default_physical_scope.data_path)
        {
            bail!("shared scope provisioning conflicts with configured physical scope");
        }
        let mut client = self.pool.get().await?;
        let physical = scope.clone();
        let physical_scope_id = physical.key();
        let transaction = client.transaction().await?;
        self.insert_physical_scope(&transaction, &physical).await?;
        let row = transaction
            .query_opt(
                &format!(
                    r#"INSERT INTO {}.workspace_scope_binding
  (workspace_id, physical_scope_id)
VALUES ($1, $2)
ON CONFLICT (workspace_id) DO UPDATE SET
  updated_at = NOW()
WHERE {}.workspace_scope_binding.physical_scope_id = EXCLUDED.physical_scope_id
RETURNING workspace_id;"#,
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema)
                ),
                &[&request.scope_id, &physical_scope_id],
            )
            .await?;
        if row.is_none() {
            bail!("scope conflict for scope {}", request.scope_id);
        }
        transaction.commit().await?;

        self.ensure_scope_tables(&scope).await?;
        Ok(scope)
    }

    /// Resolve an existing scope registry entry.
    pub async fn resolve_scope(&self, scope_id: &str) -> Result<PhysicalScope> {
        let scope = self.resolve_scope_legacy(scope_id).await?;
        self.ensure_scope_tables(&scope).await?;
        Ok(scope)
    }

    /// List all provisioned DuckLake scopes from the registry (for maintenance).
    /// Returns `(scope_id, scope)` — `scope_id` is the tenant id used for ops labels.
    pub async fn list_scopes(&self) -> Result<Vec<(String, PhysicalScope)>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT wb.workspace_id, ps.metadata_path, \
                            ps.ducklake_metadata_schema, ps.data_path, ps.catalog_alias \
                     FROM {}.workspace_scope_binding wb \
                     JOIN {}.physical_scope ps ON ps.physical_scope_id = wb.physical_scope_id \
                     ORDER BY wb.workspace_id;",
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    PhysicalScope {
                        metadata_path: row.get(1),
                        metadata_schema: row.get(2),
                        data_path: row.get(3),
                        catalog_alias: row.get(4),
                    },
                )
            })
            .collect())
    }

    /// Resolve scope and load active telemetry column manifests from Postgres.
    pub async fn load_active_telemetry_columns_manifests(
        &self,
        scope_id: &str,
    ) -> Result<(PhysicalScope, Vec<TelemetryColumnsManifest>)> {
        let scope = if scope_id.is_empty() {
            self.resolve_or_create(scope_id).await?
        } else {
            self.resolve_scope(scope_id).await?
        };
        let client = self.pool.get().await?;
        let manifests = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
            .await
            .map_err(map_spec_load_error)?;
        Ok((scope, manifests))
    }

    /// Load active telemetry promotion manifests for an already bound scope.
    pub async fn load_active_telemetry_columns_manifests_for_scope(
        &self,
        scope: &PhysicalScope,
    ) -> Result<Vec<TelemetryColumnsManifest>> {
        let client = self.pool.get().await?;
        let manifests = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
            .await
            .map_err(map_spec_load_error)?;
        Ok(manifests)
    }

    async fn activate_spec_tx(
        tx: &deadpool_postgres::Transaction<'_>,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        activation: &PromotionSpecActivation,
    ) -> Result<String> {
        let schema = scope.metadata_schema.replace('"', "\"\"");
        tx.execute(
            &format!(
                // Supersede only the same (target_kind, target_tables) pair so distinct
                // telemetry_columns specs (e.g. traces vs logs) can both stay active.
                r#"UPDATE "{schema}".promotion_specs
SET status = 'inactive'
WHERE status = 'active'
  AND target_kind = $1
  AND target_tables = $2
  AND spec_id <> $3;"#
            ),
            &[
                &activation.target_kind,
                &activation.target_tables,
                &activation.spec_id,
            ],
        )
        .await?;
        tx.execute(
            &format!(
                r#"INSERT INTO "{schema}".promotion_specs
  (spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', $2, $3, $4, $5, 'active')
ON CONFLICT (spec_id) DO UPDATE SET
  target_kind = EXCLUDED.target_kind,
  target_tables = EXCLUDED.target_tables,
  manifest_json = EXCLUDED.manifest_json,
  manifest_hash = EXCLUDED.manifest_hash,
  status = 'active',
  applied_at = NOW();"#
            ),
            &[
                &activation.spec_id,
                &activation.target_kind,
                &activation.target_tables,
                &manifest_yaml,
                &activation.manifest_hash,
            ],
        )
        .await?;
        Ok(activation.spec_id.clone())
    }

    async fn load_business_manifest_tx(
        tx: &deadpool_postgres::Transaction<'_>,
        scope: &PhysicalScope,
        table_name: &str,
    ) -> Result<Option<BusinessTableManifest>> {
        let schema = scope.metadata_schema.replace('"', "\"\"");
        let rows = tx
            .query(
                &format!(
                    r#"SELECT spec_id, manifest_json FROM "{schema}".promotion_specs
WHERE status = 'active' AND target_kind = 'business_table' AND target_tables = $1
ORDER BY applied_at DESC
LIMIT 1;"#
                ),
                &[&table_name],
            )
            .await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let spec_id: String = row.get(0);
        let manifest_json: String = row.get(1);
        business_manifest_from_row(&spec_id, &manifest_json).map_err(map_spec_load_error)
    }

    async fn lock_promotion_tx(
        tx: &deadpool_postgres::Transaction<'_>,
        scope: &PhysicalScope,
        lock_suffix: &str,
    ) -> Result<()> {
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0));",
            &[&format!("{}:{lock_suffix}", scope.metadata_schema)],
        )
        .await?;
        Ok(())
    }

    /// Low-level metadata operation used by resolver-isolation tests and maintenance tooling.
    /// Runtime apply uses [`Self::apply_telemetry_promotion_guarded`] instead.
    pub(crate) async fn record_active_telemetry_promotion_spec(
        &self,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        target_tables: &[String],
    ) -> Result<String> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        Self::lock_promotion_tx(&tx, scope, "telemetry_columns").await?;
        let activation = telemetry_spec_activation(manifest_yaml, target_tables);
        let spec_id = Self::activate_spec_tx(&tx, scope, manifest_yaml, &activation).await?;
        tx.commit().await?;
        Ok(spec_id)
    }

    /// Apply telemetry DDL and activation through the shared lifecycle under a Postgres lock.
    pub(crate) async fn apply_telemetry_promotion_guarded<F, Fut>(
        &self,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        target_tables: &[String],
        apply_ddl: F,
    ) -> Result<String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        Self::lock_promotion_tx(&tx, scope, "telemetry_columns").await?;
        let activation = telemetry_spec_activation(manifest_yaml, target_tables);
        let spec_id = run_telemetry_apply(apply_ddl, || async {
            Self::activate_spec_tx(&tx, scope, manifest_yaml, &activation).await
        })
        .await?;
        tx.commit().await?;
        Ok(spec_id)
    }

    /// Apply business load/validate/DDL/activation through the Postgres lifecycle.
    pub(crate) async fn apply_business_promotion_guarded<F, Fut>(
        &self,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        spec: &BusinessTableManifest,
        apply_ddl: F,
    ) -> std::result::Result<String, BusinessApplyError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let table_name = spec.target.table.as_str();
        let mut client = self.pool.get().await.map_err(anyhow_other)?;
        let tx = client.transaction().await.map_err(anyhow_other)?;
        Self::lock_promotion_tx(&tx, scope, &format!("business_table:{table_name}"))
            .await
            .map_err(BusinessApplyError::Other)?;
        let activation = business_spec_activation(table_name, manifest_yaml);
        let spec_id = run_business_apply(
            spec,
            || async { Self::load_business_manifest_tx(&tx, scope, table_name).await },
            apply_ddl,
            || async { Self::activate_spec_tx(&tx, scope, manifest_yaml, &activation).await },
        )
        .await?;
        tx.commit().await.map_err(anyhow_other)?;
        Ok(spec_id)
    }
}

fn anyhow_other<E: std::error::Error + Send + Sync + 'static>(err: E) -> BusinessApplyError {
    BusinessApplyError::Other(anyhow!(err))
}

fn map_spec_load_error(err: PromotionSpecLoadError) -> anyhow::Error {
    match err {
        PromotionSpecLoadError::Postgres(e) => anyhow!(e),
        PromotionSpecLoadError::Backend(e) => anyhow!(e),
        PromotionSpecLoadError::InvalidRowManifest { spec_id, source } => {
            anyhow!("promotion spec {spec_id} is invalid: {source}")
        }
    }
}

fn parse_postgres_kv_config(pg: &mut tokio_postgres::Config, metadata_path: &str) -> Result<()> {
    // Bare Postgres KV string only (`host=... port=...`). DuckLake attach adds
    // the `postgres:` scheme in one place; do not sniff schemes here.
    let conn_str = metadata_path.trim();
    for part in conn_str.split_whitespace() {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid postgres kv segment in metadata_path: {}", part))?;
        let v = v.trim_matches('\'');
        match k {
            "host" => {
                pg.host(v);
            }
            "port" => {
                pg.port(v.parse().context("metadata_path port")?);
            }
            "dbname" | "database" => {
                pg.dbname(v);
            }
            "user" | "username" => {
                pg.user(v);
            }
            "password" => {
                pg.password(v);
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn quote_pg_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::validate_metadata_schema_name;
    use super::{QueryEngine, RuntimeEngine};
    use std::sync::Arc;

    #[test]
    fn tenant_runtime_exposes_logical_accessors() {
        let _tenant_id: fn(&RuntimeEngine) -> &str = RuntimeEngine::tenant_id;
        let _query: fn(&RuntimeEngine) -> Arc<QueryEngine> = RuntimeEngine::query_engine;
    }

    #[test]
    fn metadata_schema_name_respects_postgres_identifier_limit() {
        assert!(validate_metadata_schema_name(&"a".repeat(63)).is_ok());
        assert!(validate_metadata_schema_name(&"a".repeat(64)).is_err());
        assert!(validate_metadata_schema_name(&"é".repeat(32)).is_err());
    }
}
