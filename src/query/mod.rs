use crate::config::Config;
use crate::runtime_engine::DuckLakeScope;
use crate::storage::TieredStorage;
use std::sync::Arc;

pub mod cache;
pub mod duckdb;

#[derive(Clone)]
pub struct QueryEngine {
    duckdb: Arc<duckdb::DuckDBQueryEngine>,
    /// When false (ops/self-monitoring engine), skip process self-monitoring
    /// query instruments (anti-recursion).
    record_self_monitoring: bool,
    tenant_id: String,
}

pub async fn create_query_engine(
    config: &Config,
    tiered_storage: Arc<dyn TieredStorage>,
) -> anyhow::Result<QueryEngine> {
    let duckdb = Arc::new(
        duckdb::DuckDBQueryEngine::new_with_liveness(config, tiered_storage, true, "_default")
            .await?,
    );

    Ok(QueryEngine {
        duckdb,
        record_self_monitoring: true,
        tenant_id: "_default".into(),
    })
}

/// Build a query engine whose DuckLake attachment matches `scope` (same scope as tenant-bound ingest).
pub async fn create_query_engine_for_scope(
    config: &Config,
    tiered_storage: Arc<dyn TieredStorage>,
    scope: &DuckLakeScope,
) -> anyhow::Result<QueryEngine> {
    create_query_engine_for_scope_with_liveness(config, tiered_storage, scope, true, "_default")
        .await
}

/// Same as [`create_query_engine_for_scope`], with SelfHeal liveness participation.
pub async fn create_query_engine_for_scope_with_liveness(
    config: &Config,
    tiered_storage: Arc<dyn TieredStorage>,
    scope: &DuckLakeScope,
    counts_toward_liveness: bool,
    tenant_id: &str,
) -> anyhow::Result<QueryEngine> {
    if scope.metadata_schema.trim().is_empty() && scope.data_path.trim().is_empty() {
        let duckdb = Arc::new(
            duckdb::DuckDBQueryEngine::new_with_liveness(
                config,
                tiered_storage,
                counts_toward_liveness,
                tenant_id,
            )
            .await?,
        );
        return Ok(QueryEngine {
            duckdb,
            record_self_monitoring: counts_toward_liveness,
            tenant_id: tenant_id.to_string(),
        });
    }
    let mut cfg = config.clone();
    cfg.ducklake.metadata_schema = scope.metadata_schema.clone();
    cfg.ducklake.data_path = scope.data_path.clone();
    let duckdb = Arc::new(
        duckdb::DuckDBQueryEngine::new_with_liveness(
            &cfg,
            tiered_storage,
            counts_toward_liveness,
            tenant_id,
        )
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
    pub fn catalog_alias(&self) -> &str {
        self.duckdb.catalog_alias()
    }

    /// Schema-qualified layout prefix (`softprobe` or `softprobe.<tenant_schema>`).
    ///
    /// Must match ingest `layout_catalog_prefix` so Prom postings/sample SQL hits
    /// the same `metric_*` tables the writer populates.
    pub fn layout_catalog_prefix(&self) -> String {
        self.duckdb.layout_catalog_prefix()
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub async fn execute_query(&self, query: &str) -> anyhow::Result<duckdb::QueryResult> {
        let _ = self.record_self_monitoring;
        self.duckdb.execute_query(query).await
    }

    /// Metadata / inventory SQL: dedicated connection, no self-monitoring.
    pub async fn execute_query_uninstrumented(
        &self,
        query: &str,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb.execute_query_uninstrumented(query).await
    }

    /// Execute a DuckLake query against one tenant's resolved metadata schema.
    ///
    /// Runtime control endpoints are tenant-authenticated, so they must read from the same
    /// DuckLake scope that ingest used for that tenant instead of the process-level config schema.
    pub async fn execute_query_in_ducklake_scope(
        &self,
        query: &str,
        scope: &DuckLakeScope,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb
            .execute_query_in_ducklake_scope(query, scope)
            .await
    }
}
