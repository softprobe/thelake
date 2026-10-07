// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept workspace_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

pub mod auth;
pub mod control;
pub mod debug_sql;
pub mod error;
pub mod fields;
pub mod health;
pub mod ingest;
pub(crate) mod mapping;
pub mod scores;
pub mod sessions;
pub mod traces;

use crate::authn::TenantInfo;
use crate::compat::loki::loki_routes;
use crate::compat::stubs::compat_stub_routes;
use crate::compat::tempo::tempo_routes;
use crate::config::Config;
use axum::{
    response::Html,
    routing::{get, post},
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

pub(crate) fn map_missing_optional_table(
    err: anyhow::Error,
) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
    if MISSING_OPTIONAL_TABLE.is_match(&err.to_string()) {
        Ok(empty_query_result())
    } else {
        Err(err)
    }
}

/// Apply [`map_missing_optional_table`] after a typed/`execute_trusted` lake read.
pub(crate) fn map_execute_result(
    result: anyhow::Result<crate::storage::duckdb::QueryResult>,
) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
    match result {
        Ok(rows) => Ok(rows),
        Err(err) => map_missing_optional_table(err),
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

    pub async fn engine_for_id(&self, workspace_id: &str) -> anyhow::Result<Arc<RuntimeEngine>> {
        self.engines.engine_for(workspace_id).await
    }

    /// Execute raw SQL on the tenant-bound query engine (debug `/v1/query/sql` only).
    pub(crate) async fn execute_tenant_scoped_sql(
        &self,
        tenant: Option<&TenantInfo>,
        sql: &str,
    ) -> anyhow::Result<crate::storage::duckdb::QueryResult> {
        let workspace_id = tenant.map(|t| t.workspace_id.as_str()).unwrap_or("");
        let engine = self.engines.engine_for(workspace_id).await?;
        map_execute_result(engine.execute_query(sql).await)
    }
}

/// HTTP router + [`AppState`]. Per-tenant DuckLake/query engines are created
/// lazily on first request via [`RuntimeEngineManager`].
pub async fn create_router(
    config: Arc<Config>,
    control_plane: Option<ControlPlaneRuntime>,
) -> anyhow::Result<(Router, AppState)> {
    let shared_mode =
        config.ducklake.workspace_scope_mode == crate::workspace_scope::WorkspaceScopeMode::Shared;
    let runtime_engine_manager =
        Arc::new(RuntimeEngineManager::connect(config, control_plane.clone()).await?);
    let state = AppState {
        engines: runtime_engine_manager,
    };

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
        .route("/v1/traces", post(ingest::ingest_traces))
        .route("/v1/traces/search", post(traces::search_traces))
        .route("/v1/traces/details", post(traces::trace_details_post))
        .route("/v1/traces/{trace_id}", get(traces::get_trace))
        .route("/v1/spans/search", post(traces::search_spans))
        .route("/v1/spans/{span_id}", get(traces::get_span))
        .route("/v1/sessions/search", post(sessions::search_sessions))
        .route("/v1/sessions/details", post(sessions::session_details_post))
        .route(
            "/v1/sessions/summary/rebuild",
            post(sessions::rebuild_session_summary),
        )
        .route("/v1/sessions/{session_id}", get(sessions::get_session))
        .route(
            "/v1/sessions/{session_id}/recording",
            get(sessions::get_session_recording),
        )
        .route("/v1/logs", post(ingest::ingest_logs))
        .route("/v1/scores", post(scores::create_score))
        .route(
            "/v1/score-configs",
            get(scores::list_score_configs).post(scores::create_score_config),
        )
        .route("/v1/fields", get(fields::fields))
        .route("/v1/fields/{field}/values", get(fields::field_values))
        .route("/v1/query/sql", post(debug_sql::execute_sql))
        // Explorer SPA aliases
        .route("/v1/llm/scores", post(scores::create_score))
        .route(
            "/v1/llm/score-configs",
            get(scores::list_score_configs).post(scores::create_score_config),
        )
        .route("/v1/llm/spans/search", post(traces::search_spans))
        .route("/v1/llm/spans/{span_id}", get(traces::get_span))
        .route("/v1/llm/traces/{trace_id}", get(traces::get_trace))
        .route("/v1/llm/sessions/search", post(sessions::search_sessions))
        .route("/v1/llm/sessions/{session_id}", get(sessions::get_session))
        .route(
            "/v1/llm/sessions/{session_id}/recording",
            get(sessions::get_session_recording),
        )
        .merge(control::runtime_control_routes())
        .merge(loki_routes())
        .merge(tempo_routes())
        .merge(compat_stub_routes())
        .with_state(state.clone())
        .merge(crate::website::explorer_routes())
        // Landing page last: explicit `/`, CSS/JS, robots/sitemap/llms, `/assets/*`
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
