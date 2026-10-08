mod engine;

use crate::config::Config;
use crate::models::ScoreConfig;
use crate::storage::ducklake::{DuckLakeAccess, PhysicalScope};
use crate::workspace_scope::{
    SharedScopeError, SharedScopeErrorCode, WorkspaceBinding, WorkspaceScopeMode,
    DEFAULT_WORKSPACE_ID,
};
use std::sync::Arc;

pub use crate::sql::lake_reads::{LogCountFilter, TraceCountFilter};
pub(crate) use engine::QueryEngineCore;
pub use engine::{
    self_heal_snapshot, set_self_heal_failures_for_test, QueryResult, SelfHealSnapshot,
};

#[derive(Clone)]
pub struct QueryEngine {
    core: Arc<QueryEngineCore>,
    /// When false (ops/self-monitoring engine), skip process self-monitoring
    /// query instruments (anti-recursion).
    record_self_monitoring: bool,
    workspace_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpSpan {
    pub request_method: Option<String>,
    pub request_path: Option<String>,
    pub request_headers: Option<String>,
    pub request_body: Option<String>,
    pub response_status_code: Option<i64>,
    pub response_headers: Option<String>,
    pub response_body: Option<String>,
}

pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let scope = crate::workspace::physical_scope_from_config(config);
    create_query_engine_for_scope_with_liveness(config, &scope, true, DEFAULT_WORKSPACE_ID).await
}

/// Build a query engine for a bound physical scope with SelfHeal liveness participation.
pub(crate) async fn create_query_engine_for_scope_with_liveness(
    config: &Config,
    scope: &PhysicalScope,
    counts_toward_liveness: bool,
    workspace_id: &str,
) -> anyhow::Result<QueryEngine> {
    let binding = WorkspaceBinding::new(
        workspace_id,
        scope.clone(),
        config.ducklake.workspace_scope_mode,
    )
    .map_err(|error| anyhow::anyhow!(error))?;
    let access = DuckLakeAccess::Workspace(binding);
    let core = Arc::new(
        QueryEngineCore::new_with_liveness(config, access, counts_toward_liveness, workspace_id)
            .await?,
    );
    Ok(QueryEngine {
        core,
        record_self_monitoring: counts_toward_liveness,
        workspace_id: workspace_id.to_string(),
    })
}

impl QueryEngine {
    /// DuckLake catalog alias used by this engine (e.g. `softprobe`).
    pub(crate) fn catalog_alias(&self) -> &str {
        self.core.catalog_alias()
    }

    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Count logs through the tenant-bound query contract.
    pub async fn count_logs(&self, filter: LogCountFilter) -> anyhow::Result<u64> {
        let query =
            crate::sql::lake_reads::count_logs(&filter).map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    /// Count traces through the tenant-bound query contract.
    pub async fn count_traces(&self, filter: TraceCountFilter) -> anyhow::Result<u64> {
        let query = crate::sql::lake_reads::count_traces(&filter)
            .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    /// Read the first HTTP-bearing span for a session.
    pub async fn find_http_span(
        &self,
        session_id: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<Option<HttpSpan>> {
        let query = crate::sql::lake_reads::find_http_span(session_id, time_window)
            .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        let Some(row) = result.rows.first() else {
            return Ok(None);
        };
        Ok(Some(HttpSpan {
            request_method: row
                .first()
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            request_path: row
                .get(1)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            request_headers: row
                .get(2)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            request_body: row
                .get(3)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            response_status_code: row.get(4).and_then(|value| value.as_i64()),
            response_headers: row
                .get(5)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            response_body: row
                .get(6)
                .and_then(|value| value.as_str())
                .map(str::to_owned),
        }))
    }

    /// Count calendar-day partitions visible for one session.
    pub async fn count_trace_days(
        &self,
        session_id: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<u64> {
        let query = crate::sql::lake_reads::count_trace_days(session_id, time_window)
            .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    /// Count traces whose typed attribute matches a value.
    pub async fn count_traces_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<u64> {
        let query =
            crate::sql::lake_reads::count_traces_by_attribute(session_id, key, value, time_window)
                .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    /// Count logs whose typed attribute matches a value.
    pub async fn count_logs_by_attribute(
        &self,
        key: &str,
        value: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<u64> {
        let query = crate::sql::lake_reads::count_logs_by_attribute(key, value, time_window)
            .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    /// Read the attribute bag for the first matching trace.
    pub async fn trace_attributes_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let query = crate::sql::lake_reads::trace_attributes_by_attribute(
            session_id,
            key,
            value,
            time_window,
        )
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(map_attributes_row(&result))
    }

    /// Read the attribute bag for a span selected by its stable identifier.
    pub async fn trace_attributes_for_span(
        &self,
        span_id: &str,
        time_window: crate::sql::QueryWindow,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let query = crate::sql::lake_reads::trace_attributes_for_span(span_id, time_window)
            .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(map_attributes_row(&result))
    }

    /// Execute the approved LLM span query built from a typed request.
    pub async fn search_spans(
        &self,
        request: &crate::sql::llm::search::SpanSearchRequest,
    ) -> anyhow::Result<QueryResult> {
        let query = crate::sql::llm::search_spans(request).map_err(anyhow::Error::msg)?;
        self.execute_trusted(query).await
    }

    /// Execute the approved telemetry-details query for a typed target.
    pub async fn telemetry_details_logs(
        &self,
        target: &crate::sql::telemetry::TelemetryDetailsTarget,
        time_range: &crate::sql::telemetry::TelemetryTimeRange,
        limit: usize,
    ) -> anyhow::Result<QueryResult> {
        let query = crate::sql::telemetry::details_logs(target, time_range, limit)
            .map_err(anyhow::Error::msg)?;
        self.execute_trusted(query).await
    }

    /// Check if a score exists by ID and timestamp.
    pub async fn score_exists(
        &self,
        score_id: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<bool> {
        let window = crate::sql::QueryWindow::try_new(timestamp, timestamp)
            .map_err(|e| anyhow::anyhow!(e))?;
        let query = crate::sql::lake_reads::score_exists(score_id, window)
            .map_err(|e| anyhow::anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    /// Check if a score configuration exists by ID.
    pub async fn score_config_exists(&self, config_id: &str) -> anyhow::Result<bool> {
        let query = crate::sql::lake_reads::score_config_exists(config_id)
            .map_err(|e| anyhow::anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    /// List all score configurations ordered by timestamp DESC.
    pub async fn list_score_configs(&self) -> anyhow::Result<Vec<ScoreConfig>> {
        let query = crate::sql::lake_reads::list_score_configs().map_err(|e| anyhow::anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .iter()
            .filter_map(|r| ScoreConfig::from_json_row(r))
            .collect())
    }

    /// Get a score configuration by ID.
    pub async fn get_score_config(&self, config_id: &str) -> anyhow::Result<Option<ScoreConfig>> {
        let query =
            crate::sql::lake_reads::get_score_config(config_id).map_err(|e| anyhow::anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(None),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| ScoreConfig::from_json_row(r)))
    }

    pub async fn execute_query(&self, query: &str) -> anyhow::Result<QueryResult> {
        self.ensure_raw_sql_allowed()?;
        let _ = self.record_self_monitoring;
        self.core.execute_query(query).await
    }

    /// Metadata / inventory SQL: dedicated connection, no self-monitoring.
    pub async fn execute_query_uninstrumented(&self, query: &str) -> anyhow::Result<QueryResult> {
        self.ensure_raw_sql_allowed()?;
        self.core.execute_query_uninstrumented(query).await
    }

    /// Execute the opaque result of an approved internal query builder.
    pub(crate) async fn execute_trusted(
        &self,
        query: crate::sql::trusted::TrustedSql,
    ) -> anyhow::Result<QueryResult> {
        self.core.execute_trusted(query).await
    }

    /// Several inventory SQLs on one open+attach (avoids per-query DuckDB init).
    pub(crate) async fn execute_queries_uninstrumented(
        &self,
        queries: Vec<&str>,
    ) -> anyhow::Result<Vec<anyhow::Result<QueryResult>>> {
        self.ensure_raw_sql_allowed()?;
        self.core.execute_queries_uninstrumented(queries).await
    }

    fn ensure_raw_sql_allowed(&self) -> anyhow::Result<()> {
        if self.core.workspace_scope_mode() == WorkspaceScopeMode::Shared {
            return Err(anyhow::anyhow!(SharedScopeError::new(
                SharedScopeErrorCode::RawSqlForbidden,
                "raw SQL is disabled for shared workspace scope; use a typed query API",
            )));
        }
        Ok(())
    }
}

fn first_count(result: &QueryResult) -> u64 {
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| value.as_i64())
        .unwrap_or_default() as u64
}

fn map_attributes_row(result: &QueryResult) -> Option<serde_json::Value> {
    let value = result.rows.first().and_then(|row| row.first())?;
    value
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .or_else(|| Some(value.clone()))
}
