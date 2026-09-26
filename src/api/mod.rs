// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

pub mod health;
pub mod ingestion;
pub mod llm;
pub mod query;
pub(crate) mod query_window;
pub(crate) mod sql_support;
pub mod telemetry;

use crate::authn::TenantInfo;
use crate::compat::loki::loki_routes;
use crate::compat::stubs::compat_stub_routes;
use crate::compat::tempo::tempo_routes;
use crate::config::Config;
use axum::{
    response::Html,
    routing::{get, post, MethodRouter},
    Json, Router,
};
use once_cell::sync::Lazy;
use regex::Regex;
use std::sync::Arc;

pub use crate::control_plane::ControlPlaneRuntime;
pub use crate::runtime_engine::{RuntimeEngine, RuntimeEngineManager};

/// DuckDB catalog miss for optional OTLP/score tables before first ingest.
static MISSING_OPTIONAL_TABLE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"Table with name (?:traces|logs|scores|score_configs) does not exist")
        .expect("valid missing-optional-table regex")
});

fn empty_query_result() -> crate::storage::duckdb::QueryResult {
    crate::storage::duckdb::QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
    }
}

fn map_missing_optional_table(
    err: anyhow::Error,
) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
    if MISSING_OPTIONAL_TABLE.is_match(&err.to_string()) {
        Ok(empty_query_result())
    } else {
        Err(err)
    }
}

/// Unified application state for Axum router
#[derive(Clone)]
pub struct AppState {
    pub engines: Arc<RuntimeEngineManager>,
}

impl AppState {
    pub async fn engine_for_tenant(
        &self,
        tenant: &TenantInfo,
    ) -> anyhow::Result<Arc<RuntimeEngine>> {
        self.engines.engine_for_tenant(tenant).await
    }

    pub async fn engine_for_id(&self, tenant_id: &str) -> anyhow::Result<Arc<RuntimeEngine>> {
        self.engines.engine_for(tenant_id).await
    }

    /// Execute SQL on the tenant-bound query engine (scope fixed at engine construction).
    pub(crate) async fn execute_tenant_scoped_sql(
        &self,
        tenant: Option<&TenantInfo>,
        sql: &str,
    ) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
        let tenant_id = tenant.map(|t| t.tenant_id.as_str()).unwrap_or("");
        let engine = self.engines.engine_for(tenant_id).await?;
        match engine.execute_query(sql).await {
            Ok(result) => Ok(result),
            Err(err) => map_missing_optional_table(err),
        }
    }

    /// Execute SQL emitted by an internal typed query builder.
    ///
    /// Shared mode exposes only tenant-filtered logical views to query workers;
    /// callers must therefore use the opaque trusted boundary instead of the
    /// arbitrary-SQL compatibility path.
    pub(crate) async fn execute_tenant_scoped_trusted_sql(
        &self,
        tenant: Option<&TenantInfo>,
        query: crate::sql::trusted::TrustedSql,
    ) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
        let tenant_id = tenant.map(|t| t.tenant_id.as_str()).unwrap_or("");
        let engine = self.engines.engine_for(tenant_id).await?;
        match engine.execute_trusted(query).await {
            Ok(result) => Ok(result),
            Err(err) => map_missing_optional_table(err),
        }
    }
}

/// HTTP router + [`AppState`]. Per-tenant DuckLake/query engines are created
/// lazily on first request via [`RuntimeEngineManager`].
pub async fn create_router(
    config: Arc<Config>,
    traces: MethodRouter<AppState>,
    control_plane: Option<ControlPlaneRuntime>,
) -> anyhow::Result<(Router, AppState)> {
    let shared_mode =
        config.ducklake.workspace_scope_mode == crate::workspace_scope::WorkspaceScopeMode::Shared;
    let runtime_engine_manager =
        Arc::new(RuntimeEngineManager::connect(config, control_plane.clone()).await?);
    let state = AppState {
        engines: runtime_engine_manager,
    };

    // Shared mode is a startup contract, not a lazy per-request feature flag.
    // Build the default bound engine before returning the router so ownership
    // schema incompatibility, DuckLake attach failures, and filtered-view
    // initialization prevent the service from becoming ready.
    if shared_mode {
        state
            .engines
            .engine_for("")
            .await
            .map_err(|error| anyhow::anyhow!("shared DuckLake startup gate: {error}"))?;
        state
            .engines
            .maintenance_engine()
            .await?
            .validate_startup()
            .await
            .map_err(|error| anyhow::anyhow!("shared maintenance startup gate: {error}"))?;
    }

    let router = Router::new()
        .route("/health", get(health::health_check))
        .route("/ready", get(health::ready_check))
        .route("/openapi.json", get(openapi_spec))
        .route("/swagger", get(swagger_ui))
        .route("/v1/traces", traces)
        .route("/v1/logs", post(ingestion::logs::ingest_logs))
        .route("/v1/llm/scores", post(llm::create_score))
        .route(
            "/v1/llm/score-configs",
            get(llm::list_score_configs).post(llm::create_score_config),
        )
        .route(
            "/v1/llm/observations/search",
            post(llm::query::search_observations),
        )
        .route(
            "/v1/llm/observations/{span_id}",
            get(llm::query::get_observation),
        )
        .route("/v1/llm/traces/{trace_id}", get(llm::query::get_trace))
        .route("/v1/llm/sessions/search", post(llm::query::search_sessions))
        .route(
            "/v1/llm/sessions/summary/rebuild",
            post(llm::query::rebuild_session_summary),
        )
        .route(
            "/v1/llm/sessions/{session_id}",
            get(llm::query::get_session),
        )
        .route(
            "/v1/llm/sessions/{session_id}/observations",
            get(llm::query::get_session_observations),
        )
        .route(
            "/v1/llm/sessions/{session_id}/recording",
            get(llm::query::get_session_recording),
        )
        .route("/v1/query/sql", post(query::execute_sql))
        .route("/v1/telemetry/search", post(telemetry::search))
        .route("/v1/telemetry/details", post(telemetry::details_post))
        .route("/v1/telemetry/fields", get(telemetry::fields))
        .route(
            "/v1/telemetry/fields/{field}/values",
            get(telemetry::field_values),
        )
        .route(
            "/v1/telemetry/sessions/{session_id}",
            get(telemetry::session_details),
        )
        .route(
            "/v1/telemetry/traces/{trace_id}",
            get(telemetry::trace_details),
        )
        .merge(loki_routes())
        .merge(tempo_routes())
        .merge(compat_stub_routes())
        .with_state(state.clone())
        // Landing page last: explicit `/`, `/styles.css`, `/script.js`, `/assets/*`
        // only — must not shadow `/v1/*`, `/health`, `/ready`, OpenAPI, or swagger.
        .merge(crate::website::routes());

    Ok((router, state))
}

async fn openapi_spec() -> Json<serde_json::Value> {
    static SPEC: Lazy<serde_json::Value> = Lazy::new(|| {
        let mut spec: serde_json::Value =
            serde_yaml::from_str(include_str!("../../docs/ingestion-openapi.yaml"))
                .expect("docs/ingestion-openapi.yaml must parse as OpenAPI JSON");
        if let Some(info) = spec.get_mut("info").and_then(|v| v.as_object_mut()) {
            info.insert(
                "version".to_string(),
                serde_json::Value::String(env!("CARGO_PKG_VERSION").to_string()),
            );
        }
        spec
    });
    Json(SPEC.clone())
}

async fn swagger_ui() -> Html<&'static str> {
    Html(
        r##"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>thelake API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" });
    </script>
  </body>
</html>"##,
    )
}

#[cfg(test)]
mod unit_tests;
