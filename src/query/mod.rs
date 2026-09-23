use crate::config::Config;
use crate::workspace_scope::{
    PhysicalScope, SharedScopeError, SharedScopeErrorCode, WorkspaceScopeMode,
};
use std::sync::Arc;

pub(crate) mod cache;
pub mod duckdb;
pub(crate) mod workspace_views;

#[derive(Clone)]
pub struct QueryEngine {
    duckdb: Arc<duckdb::DuckDBQueryEngine>,
    /// When false (ops/self-monitoring engine), skip process self-monitoring
    /// query instruments (anti-recursion).
    record_self_monitoring: bool,
    tenant_id: String,
}

pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let duckdb =
        Arc::new(duckdb::DuckDBQueryEngine::new_with_liveness(config, true, "_default").await?);

    Ok(QueryEngine {
        duckdb,
        record_self_monitoring: true,
        tenant_id: "_default".into(),
    })
}

/// Build a query engine for a bound physical scope with SelfHeal liveness participation.
pub(crate) async fn create_query_engine_for_scope_with_liveness(
    config: &Config,
    scope: &PhysicalScope,
    counts_toward_liveness: bool,
    tenant_id: &str,
) -> anyhow::Result<QueryEngine> {
    if scope.metadata_schema.trim().is_empty() && scope.data_path.trim().is_empty() {
        let duckdb = Arc::new(
            duckdb::DuckDBQueryEngine::new_with_liveness(config, counts_toward_liveness, tenant_id)
                .await?,
        );
        return Ok(QueryEngine {
            duckdb,
            record_self_monitoring: counts_toward_liveness,
            tenant_id: tenant_id.to_string(),
        });
    }
    let mut cfg = config.clone();
    cfg.ducklake.metadata_path = scope.metadata_path.clone();
    cfg.ducklake.metadata_schema = scope.metadata_schema.clone();
    cfg.ducklake.data_path = scope.data_path.clone();
    cfg.ducklake.catalog_alias = scope.catalog_alias.clone();
    let duckdb = Arc::new(
        duckdb::DuckDBQueryEngine::new_with_liveness(&cfg, counts_toward_liveness, tenant_id)
            .await?,
    );
    Ok(QueryEngine {
        duckdb,
        record_self_monitoring: counts_toward_liveness,
        tenant_id: tenant_id.to_string(),
    })
}

impl QueryEngine {
    /// DuckLake catalog alias used by this engine (e.g. `softprobe`).
    pub(crate) fn catalog_alias(&self) -> &str {
        self.duckdb.catalog_alias()
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub async fn execute_query(&self, query: &str) -> anyhow::Result<duckdb::QueryResult> {
        self.ensure_raw_sql_allowed()?;
        let _ = self.record_self_monitoring;
        self.duckdb.execute_query(query).await
    }

    /// Metadata / inventory SQL: dedicated connection, no self-monitoring.
    pub async fn execute_query_uninstrumented(
        &self,
        query: &str,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.ensure_raw_sql_allowed()?;
        self.duckdb.execute_query_uninstrumented(query).await
    }

    /// Execute SQL produced by an approved internal query builder.
    /// Execute the opaque result of an approved internal query builder.
    pub(crate) async fn execute_trusted(
        &self,
        query: crate::sql::trusted::TrustedSql,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb.execute_trusted(query).await
    }

    /// Several inventory SQLs on one open+attach (avoids per-query DuckDB init).
    pub(crate) async fn execute_queries_uninstrumented(
        &self,
        queries: Vec<&str>,
    ) -> anyhow::Result<Vec<anyhow::Result<duckdb::QueryResult>>> {
        self.ensure_raw_sql_allowed()?;
        self.duckdb.execute_queries_uninstrumented(queries).await
    }

    fn ensure_raw_sql_allowed(&self) -> anyhow::Result<()> {
        if self.duckdb.workspace_scope_mode() == WorkspaceScopeMode::Shared {
            return Err(anyhow::anyhow!(SharedScopeError::new(
                SharedScopeErrorCode::RawSqlForbidden,
                "raw SQL is disabled for shared workspace scope; use a typed query API",
            )));
        }
        Ok(())
    }
}
