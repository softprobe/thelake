use crate::config::Config;
use crate::workspace_scope::{
    PhysicalScope, SharedScopeError, SharedScopeErrorCode, WorkspaceScopeMode, DEFAULT_WORKSPACE_ID,
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

#[derive(Debug, Clone, Default)]
pub struct LogCountFilter {
    pub session_id: Option<String>,
    pub body: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TraceCountFilter {
    pub session_id: Option<String>,
    pub app_id: Option<String>,
    pub span_id: Option<String>,
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
    let duckdb = Arc::new(
        duckdb::DuckDBQueryEngine::new_with_liveness(config, true, DEFAULT_WORKSPACE_ID).await?,
    );

    Ok(QueryEngine {
        duckdb,
        record_self_monitoring: true,
        tenant_id: DEFAULT_WORKSPACE_ID.into(),
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

    /// Count logs through the tenant-bound query contract.
    pub async fn count_logs(&self, filter: LogCountFilter) -> anyhow::Result<u64> {
        let mut predicates = bounded_timestamp_predicates();
        if let Some(session_id) = filter.session_id {
            predicates.push(format!(
                "session_id = {}",
                crate::sql::literal::sql_string_literal(&session_id)
            ));
        }
        if let Some(body) = filter.body {
            predicates.push(format!(
                "body = {}",
                crate::sql::literal::sql_string_literal(&body)
            ));
        }
        if let Some(trace_id) = filter.trace_id {
            predicates.push(format!(
                "trace_id = {}",
                crate::sql::literal::sql_string_literal(&trace_id)
            ));
        }
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT COUNT(*)::BIGINT AS count FROM logs WHERE {}",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as u64)
    }

    /// Count traces through the tenant-bound query contract.
    pub async fn count_traces(&self, filter: TraceCountFilter) -> anyhow::Result<u64> {
        let mut predicates = bounded_timestamp_predicates();
        add_trace_filter_predicates(&mut predicates, filter);
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT COUNT(*)::BIGINT AS count FROM traces WHERE {}",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as u64)
    }

    /// Read the first HTTP-bearing span for a session.
    pub async fn find_http_span(&self, session_id: &str) -> anyhow::Result<Option<HttpSpan>> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "session_id = {}",
            crate::sql::literal::sql_string_literal(session_id)
        ));
        predicates.push("http_request_method IS NOT NULL".to_string());
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT http_request_method, http_request_path, http_request_headers, http_request_body, http_response_status_code, http_response_headers, http_response_body FROM traces WHERE {} LIMIT 1",
            predicates.join(" AND ")
        ))
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
    pub async fn count_trace_days(&self, session_id: &str) -> anyhow::Result<u64> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "session_id = {}",
            crate::sql::literal::sql_string_literal(session_id)
        ));
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT COUNT(*)::BIGINT FROM (SELECT strftime(timestamp, '%Y-%m-%d') FROM traces WHERE {} GROUP BY 1) days",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as u64)
    }

    /// Count traces whose typed attribute matches a value.
    pub async fn count_traces_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
    ) -> anyhow::Result<u64> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "session_id = {}",
            crate::sql::literal::sql_string_literal(session_id)
        ));
        predicates.push(format!(
            "{} = {}",
            crate::storage::schema::variant::variant_varchar("attributes", key),
            crate::sql::literal::sql_string_literal(value)
        ));
        self.count_rows("traces", predicates).await
    }

    /// Count logs whose typed attribute matches a value.
    pub async fn count_logs_by_attribute(&self, key: &str, value: &str) -> anyhow::Result<u64> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "{} = {}",
            crate::storage::schema::variant::variant_varchar("attributes", key),
            crate::sql::literal::sql_string_literal(value)
        ));
        self.count_rows("logs", predicates).await
    }

    /// Read the attribute bag for the first matching trace.
    pub async fn trace_attributes_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "session_id = {}",
            crate::sql::literal::sql_string_literal(session_id)
        ));
        predicates.push(format!(
            "{} = {}",
            crate::storage::schema::variant::variant_varchar("attributes", key),
            crate::sql::literal::sql_string_literal(value)
        ));
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT CAST(attributes AS JSON) FROM traces WHERE {} LIMIT 1",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        let Some(value) = result.rows.first().and_then(|row| row.first()) else {
            return Ok(None);
        };
        Ok(value
            .as_str()
            .and_then(|text| serde_json::from_str(text).ok())
            .or_else(|| Some(value.clone())))
    }

    /// Read the attribute bag for a span selected by its stable identifier.
    pub async fn trace_attributes_for_span(
        &self,
        span_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let mut predicates = bounded_timestamp_predicates();
        predicates.push(format!(
            "span_id = {}",
            crate::sql::literal::sql_string_literal(span_id)
        ));
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT CAST(attributes AS JSON) FROM traces WHERE {} LIMIT 1",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        let Some(value) = result.rows.first().and_then(|row| row.first()) else {
            return Ok(None);
        };
        Ok(value
            .as_str()
            .and_then(|text| serde_json::from_str(text).ok())
            .or_else(|| Some(value.clone())))
    }

    /// Execute the approved LLM observation query built from a typed request.
    pub async fn search_observations(
        &self,
        request: &crate::api::llm::query::ObservationSearchRequest,
    ) -> anyhow::Result<duckdb::QueryResult> {
        let sql =
            crate::sql::llm::compile_observation_search_sql(request).map_err(anyhow::Error::msg)?;
        let query =
            crate::sql::trusted::approved_query(sql).map_err(|error| anyhow::anyhow!(error))?;
        self.execute_trusted(query).await
    }

    /// Execute the approved telemetry-details query for a typed target.
    pub async fn telemetry_details_logs(
        &self,
        target: &crate::api::telemetry::TelemetryDetailsTarget,
        time_range: &crate::api::telemetry::TelemetryTimeRange,
        limit: usize,
    ) -> anyhow::Result<duckdb::QueryResult> {
        let sql = crate::api::telemetry::compile_details_sql(target, time_range, limit)
            .map_err(anyhow::Error::msg)?
            .logs;
        let query =
            crate::sql::trusted::approved_query(sql).map_err(|error| anyhow::anyhow!(error))?;
        self.execute_trusted(query).await
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

fn bounded_timestamp_predicates() -> Vec<String> {
    vec![
        "make_timestamp_ns(epoch_ns(timestamp)) >= '1970-01-01'::TIMESTAMP_NS".to_string(),
        "make_timestamp_ns(epoch_ns(timestamp)) <= '2100-01-01'::TIMESTAMP_NS".to_string(),
    ]
}

impl QueryEngine {
    async fn count_rows(&self, table: &str, predicates: Vec<String>) -> anyhow::Result<u64> {
        let query = crate::sql::trusted::approved_query(format!(
            "SELECT COUNT(*)::BIGINT FROM {table} WHERE {}",
            predicates.join(" AND ")
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        let result = self.execute_trusted(query).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as u64)
    }
}

fn add_trace_filter_predicates(predicates: &mut Vec<String>, filter: TraceCountFilter) {
    for (column, value) in [
        ("session_id", filter.session_id),
        ("app_id", filter.app_id),
        ("span_id", filter.span_id),
    ] {
        if let Some(value) = value {
            predicates.push(format!(
                "{column} = {}",
                crate::sql::literal::sql_string_literal(&value)
            ));
        }
    }
}
