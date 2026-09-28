use crate::api::sql_support::encode_cursor;
use crate::api::AppState;
use crate::async_jobs::LeaseStore;
use crate::authn::TenantInfo;
use crate::models::{Score, ScoreDataType, ScoreSource};
use crate::sql::llm::{
    clamp_limit, compile_scores_for_span_sql, compile_scores_for_trace_sql,
    compile_session_detail_sql, compile_session_detail_sql_without_events,
    compile_session_recording_sql, compile_span_detail_sql, compile_span_search_sql,
    compile_trace_spans_sql, compile_trace_summary_sql, DEFAULT_SEARCH_LIMIT,
    DEFAULT_SESSION_LIMIT, DEFAULT_TRACE_LIMIT,
};
// Production session search is served by RuntimeEngine::search_session_summary
// (Postgres session_summary). compile_session_search_sql remains unit-test only.
#[cfg(test)]
use crate::sql::llm::compile_session_search_sql;
use crate::storage::schema::variant::{parse_projected_json_value, variant_json_to_string_map};
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeSet, HashMap};
use tracing::warn;

type ApiError = (StatusCode, Json<Value>);

fn trusted_query(sql: impl Into<String>) -> Result<crate::sql::trusted::TrustedSql, ApiError> {
    crate::sql::trusted::approved_query(sql).map_err(|error| storage_error(anyhow::anyhow!(error)))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    #[serde(default)]
    pub span_types: Vec<String>,
    pub model_name: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub trace_id: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanSearchResponse {
    pub items: Vec<SpanSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanSummary {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub session_id: Option<String>,
    pub name: String,
    pub span_type: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status_code: Option<String>,
    pub model_name: Option<String>,
    pub model_provider: Option<String>,
    pub user_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanDetail {
    #[serde(flatten)]
    pub summary: SpanSummary,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub events: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scores: Vec<Score>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: String,
    pub session_id: Option<String>,
    pub name: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub span_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace: Trace,
    pub spans: Vec<SpanDetail>,
    pub scores: Vec<Score>,
    pub next_span_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub session_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub trace_count: i64,
    pub span_count: i64,
    #[serde(default)]
    pub user_ids: Vec<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    /// False only when DuckLake cannot read a catalog-inlined nested event
    /// row and the handler serves scalar session data without events.
    pub events_complete: bool,
    pub spans: Vec<SpanDetail>,
    pub scores: Vec<Score>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DetailQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    /// When set, only return spans belonging to this product session.
    pub session_id: Option<String>,
}

pub async fn search_spans(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<SpanSearchRequest>,
) -> Result<Json<SpanSearchResponse>, ApiError> {
    let sql = compile_span_search_sql(&request).map_err(bad_request)?;
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let result = state
        .execute_tenant_scoped_trusted_sql(tenant_ref, trusted_query(sql)?)
        .await
        .map_err(storage_error)?;

    let limit = clamp_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let mut summaries = result
        .rows
        .iter()
        .filter_map(|row| map_span_summary(&result.columns, row))
        .collect::<Vec<_>>();
    let next_cursor = next_cursor_from_spans(&mut summaries, limit);
    Ok(Json(SpanSearchResponse {
        items: summaries,
        next_cursor,
    }))
}

pub async fn get_span(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(span_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<SpanDetail>, ApiError> {
    if span_id.trim().is_empty() {
        return Err(bad_request("span_id is required".to_string()));
    }
    let sql = compile_span_detail_sql(&span_id, params.from, params.to).map_err(bad_request)?;
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let result = state
        .execute_tenant_scoped_trusted_sql(tenant_ref, trusted_query(sql)?)
        .await
        .map_err(storage_error)?;
    let row = result.rows.first().ok_or_else(not_found)?;
    let mut detail = map_span_detail(&result.columns, row).ok_or_else(not_found)?;
    detail.scores = query_scores(
        &state,
        tenant_ref,
        &compile_scores_for_span_sql(&span_id, params.from, params.to).map_err(bad_request)?,
    )
    .await?;
    Ok(Json(detail))
}

pub async fn get_trace(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(trace_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<TraceDetail>, ApiError> {
    if trace_id.trim().is_empty() {
        return Err(bad_request("trace_id is required".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let summary_sql = compile_trace_summary_sql(
        &trace_id,
        params.from,
        params.to,
        params.session_id.as_deref(),
    )
    .map_err(bad_request)?;
    let summary_result = state
        .execute_tenant_scoped_trusted_sql(tenant_ref, trusted_query(summary_sql)?)
        .await
        .map_err(storage_error)?;
    let summary_row = summary_result.rows.first().ok_or_else(not_found)?;
    let trace = map_trace(&summary_result.columns, summary_row).ok_or_else(not_found)?;

    let limit = clamp_limit(params.limit, DEFAULT_TRACE_LIMIT);
    let spans_sql = compile_trace_spans_sql(
        &trace_id,
        params.from,
        params.to,
        limit,
        params.cursor.as_deref(),
        params.session_id.as_deref(),
    )
    .map_err(bad_request)?;
    let spans_result = state
        .execute_tenant_scoped_trusted_sql(tenant_ref, trusted_query(spans_sql)?)
        .await
        .map_err(storage_error)?;
    let mut spans = spans_result
        .rows
        .iter()
        .filter_map(|row| map_span_detail(&spans_result.columns, row))
        .collect::<Vec<_>>();
    let next_span_cursor = next_cursor_from_span_details(&mut spans, limit);

    let scores = query_scores(
        &state,
        tenant_ref,
        &compile_scores_for_trace_sql(&trace_id, params.from, params.to).map_err(bad_request)?,
    )
    .await?;

    // Attach span-level scores to span details; keep the full set on the response.
    let mut by_span: HashMap<String, Vec<Score>> = HashMap::new();
    for score in &scores {
        if let Some(span_id) = &score.span_id {
            by_span
                .entry(span_id.clone())
                .or_default()
                .push(score.clone());
        }
    }
    for span in &mut spans {
        span.scores = by_span.remove(&span.summary.span_id).unwrap_or_default();
    }

    Ok(Json(TraceDetail {
        trace,
        spans,
        scores,
        next_span_cursor,
    }))
}

/// Resolve lake `QueryWindow` from Postgres `session_summary` only (D7, pad 0).
///
/// No query `from`/`to`. Missing row → 404.
async fn resolve_session_lake_window(
    state: &AppState,
    tenant_ref: Option<&TenantInfo>,
    session_id: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ApiError> {
    let tenant_id = tenant_ref.map(|t| t.tenant_id.as_str()).unwrap_or("");
    let engine = state
        .engines
        .engine_for(tenant_id)
        .await
        .map_err(storage_error)?;
    match engine.lookup_session_summary_window(session_id).await {
        Ok(Some((from, to))) => Ok((from, to)),
        Ok(None) => Err(not_found()),
        Err(crate::session_summary::SessionSummaryListError::BadRequest(msg)) => {
            Err(bad_request(msg))
        }
        Err(crate::session_summary::SessionSummaryListError::Storage(err)) => {
            Err(storage_error(err))
        }
    }
}

pub async fn get_session(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionDetail>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("session_id is required".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let (from, to) = resolve_session_lake_window(&state, tenant_ref, &session_id).await?;
    let detail_sql = compile_session_detail_sql(&session_id, from, to).map_err(bad_request)?;
    let fallback_sql =
        compile_session_detail_sql_without_events(&session_id, from, to).map_err(bad_request)?;
    let query_state = state.clone();
    let query_tenant = tenant_ref.cloned();
    let (detail_result, events_complete) =
        execute_session_detail_with_event_fallback(move |include_events| {
            let state = query_state.clone();
            let tenant = query_tenant.clone();
            let sql = if include_events {
                detail_sql.clone()
            } else {
                fallback_sql.clone()
            };
            async move {
                let query = crate::sql::trusted::approved_query(sql)
                    .map_err(|error| anyhow::anyhow!(error))?;
                let tenant_ref = tenant.as_ref();
                state
                    .execute_tenant_scoped_trusted_sql(tenant_ref, query)
                    .await
            }
        })
        .await
        .map_err(storage_error)?;
    if !events_complete {
        warn!(
            session_id = %session_id,
            "DuckLake nested event read failed; session detail is missing events"
        );
    }
    let detail_row = detail_result.rows.first().ok_or_else(not_found)?;
    let aggregate =
        map_session_aggregate(&detail_result.columns, detail_row).ok_or_else(not_found)?;
    if aggregate.span_count == 0 {
        return Err(not_found());
    }
    let spans = detail_result
        .rows
        .iter()
        .filter_map(|row| map_span_detail(&detail_result.columns, row))
        .collect::<Vec<_>>();

    let scores = map_session_scores(&detail_result.columns, detail_row);

    Ok(Json(SessionDetail {
        session_id,
        from,
        to,
        trace_count: aggregate.trace_count,
        span_count: aggregate.span_count,
        user_ids: aggregate.user_ids,
        input_tokens: aggregate.input_tokens,
        output_tokens: aggregate.output_tokens,
        total_tokens: aggregate.total_tokens,
        total_cost: aggregate.total_cost,
        events_complete,
        spans,
        scores,
    }))
}

async fn execute_session_detail_with_event_fallback<T, F, Fut>(
    mut execute: F,
) -> Result<(T, bool), anyhow::Error>
where
    F: FnMut(bool) -> Fut,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
{
    match execute(true).await {
        Ok(result) => Ok((result, true)),
        Err(error) if is_inlined_nested_event_failure(&error) => {
            execute(false).await.map(|result| (result, false))
        }
        Err(error) => Err(error),
    }
}

fn is_inlined_nested_event_failure(error: &anyhow::Error) -> bool {
    error
        .root_cause()
        .to_string()
        .trim_start()
        .lines()
        .next()
        .is_some_and(|line| {
            line.starts_with("INTERNAL Error: Attempted to access index 0 within vector of size 0")
        })
}

const RECORDING_EVENT_NAME: &str = "sp.recording.batch";
const RECORDING_EVENTS_ATTR: &str = "sp.recording.events";
const DEFAULT_RECORDING_LIMIT: usize = 50;

#[derive(Debug, Clone, Deserialize)]
pub struct RecordingQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingBatch {
    pub span_id: String,
    pub trace_id: String,
    pub start_time: DateTime<Utc>,
    pub batch_index: Option<i64>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    pub events: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecording {
    pub session_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// True when the batch LIMIT was hit; more recording spans may exist.
    #[serde(default)]
    pub truncated: bool,
    pub batches: Vec<RecordingBatch>,
    pub events: Vec<Value>,
}

/// Fetch web session recording batches for a session (`sp.observation.type=recording`).
pub async fn get_session_recording(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(session_id): Path<String>,
    Query(params): Query<RecordingQuery>,
) -> Result<Json<SessionRecording>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("session_id is required".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let (from, to) = resolve_session_lake_window(&state, tenant_ref, &session_id).await?;
    let limit = clamp_limit(params.limit, DEFAULT_RECORDING_LIMIT);
    let sql = compile_session_recording_sql(&session_id, from, to, limit).map_err(bad_request)?;
    let result = state
        .execute_tenant_scoped_trusted_sql(tenant_ref, trusted_query(sql)?)
        .await
        .map_err(storage_error)?;

    let mut batches = result
        .rows
        .iter()
        .filter_map(|row| map_recording_batch(&result.columns, row))
        .collect::<Vec<_>>();
    // Prefer producer batch_index; fall back to start_time / span_id.
    batches.sort_by(|a, b| {
        a.batch_index
            .unwrap_or(0)
            .cmp(&b.batch_index.unwrap_or(0))
            .then_with(|| a.start_time.cmp(&b.start_time))
            .then_with(|| a.span_id.cmp(&b.span_id))
    });

    let truncated = batches.len() >= limit;
    let mut events = Vec::new();
    for batch in &batches {
        events.extend(batch.events.iter().cloned());
    }
    events.sort_by(|a, b| {
        event_timestamp(a)
            .cmp(&event_timestamp(b))
            .then_with(|| event_index(a).cmp(&event_index(b)))
    });

    Ok(Json(SessionRecording {
        session_id,
        from,
        to,
        truncated,
        batches,
        events,
    }))
}

fn map_recording_batch(columns: &[String], row: &[Value]) -> Option<RecordingBatch> {
    let detail = map_span_detail(columns, row)?;
    let events = extract_recording_events(&detail.events);
    let batch_index = detail
        .attributes
        .get("sp.recording.batch_index")
        .and_then(|v| v.parse::<i64>().ok());
    Some(RecordingBatch {
        span_id: detail.summary.span_id,
        trace_id: detail.summary.trace_id,
        start_time: detail.summary.start_time,
        batch_index,
        attributes: detail.attributes,
        events,
    })
}

/// Pull rrweb event arrays out of `sp.recording.batch` span events.
pub fn extract_recording_events(span_events: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for event in span_events {
        let Some(obj) = event.as_object() else {
            continue;
        };
        let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name != RECORDING_EVENT_NAME {
            continue;
        }
        let Some(attrs) = obj.get("attributes") else {
            continue;
        };
        let raw = match attrs {
            Value::Object(map) => map.get(RECORDING_EVENTS_ATTR).cloned(),
            Value::String(text) => {
                serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|parsed| match parsed {
                        Value::Object(map) => map.get(RECORDING_EVENTS_ATTR).cloned(),
                        _ => None,
                    })
            }
            _ => None,
        };
        let Some(raw) = raw else {
            continue;
        };
        match raw {
            Value::Array(items) => out.extend(items),
            Value::String(text) => {
                if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&text) {
                    out.extend(items);
                }
            }
            _ => {}
        }
    }
    out
}

fn event_timestamp(event: &Value) -> i64 {
    event
        .get("timestamp")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

fn event_index(event: &Value) -> i64 {
    event
        .get("eventIndex")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

/// How a session list should be ordered.
///
/// Ordering happens in DuckDB over the whole time window. Doing it client-side
/// only ever sorts whatever page happened to be loaded, which is the wrong
/// answer to "show me the worst sessions today".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionOrderBy {
    #[default]
    StartTime,
    ErrorCount,
    Duration,
    TotalTokens,
    TotalCost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    #[default]
    Desc,
}

impl SortDirection {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// Keep only sessions containing at least one ERROR span.
    #[serde(default)]
    pub has_errors: Option<bool>,
    pub user_id: Option<String>,
    pub model_name: Option<String>,
    /// Match session-level `agent_name` (persisted column, `sp.agent.name`, else agent span name).
    pub agent_name: Option<String>,
    /// When true (default), hide legacy nested-only OpenCode child sessions.
    #[serde(default = "default_true")]
    pub roots_only: bool,
    #[serde(default)]
    pub order_by: SessionOrderBy,
    #[serde(default)]
    pub order: SortDirection,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub trace_count: i64,
    pub span_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub agent_name: Option<String>,
    pub user_ids: Vec<String>,
    pub models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSearchResponse {
    pub items: Vec<SessionSummary>,
    pub next_cursor: Option<String>,
    /// Cursor paging is only defined for `start_time` ordering; any other
    /// ordering returns a single ranked page. Stated explicitly so a client
    /// cannot mistake "no cursor" for "no more data".
    pub cursor_supported: bool,
}

/// Session list.
///
/// Always backed by Postgres `session_summary` (empty table → empty page).
/// Soft coalesce + dirty/reduce keep it current; list never falls back to a
/// lake scan.
///
/// Without this endpoint a client has to pull raw spans and group them
/// in memory, which makes every aggregate a per-page partial sum, breaks
/// paging (one session gets split across pages), and reduces "sessions with
/// errors" to "sessions with errors among the rows already fetched".
pub async fn search_sessions(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<SessionSearchRequest>,
) -> Result<Json<SessionSearchResponse>, ApiError> {
    let limit = clamp_limit(request.limit, DEFAULT_SESSION_LIMIT);
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);

    let tenant_id = tenant_ref.map(|t| t.tenant_id.as_str()).unwrap_or("");
    let engine = state
        .engines
        .engine_for(tenant_id)
        .await
        .map_err(storage_error)?;
    match engine.search_session_summary(&request, limit).await {
        Ok(response) => Ok(Json(response)),
        Err(crate::session_summary::SessionSummaryListError::BadRequest(msg)) => {
            Err(bad_request(msg))
        }
        Err(crate::session_summary::SessionSummaryListError::Storage(err)) => {
            Err(storage_error(err))
        }
    }
}

/// Ops: rebuild `session_summary` for an explicit `[from,to]` window (sync).
///
/// Rejects inverted / oversized windows.
/// Acquires `session_summary.rebuild` lease for the tenant scope, then runs the
/// shared lake aggregate → UPSERT path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummaryRebuildRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummaryRebuildResponse {
    pub sessions_upserted: usize,
}

fn session_summary_rebuild_holder_id(instance_id: &str) -> String {
    format!("ops-rebuild-{instance_id}-{}", uuid::Uuid::new_v4())
}

pub async fn rebuild_session_summary(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<SessionSummaryRebuildRequest>,
) -> Result<Json<SessionSummaryRebuildResponse>, ApiError> {
    let cfg = &state.engines.config().session_summary;

    crate::session_summary::validate_rebuild_window(
        request.from,
        request.to,
        cfg.max_reduce_span_seconds,
    )
    .map_err(bad_request)?;

    let tenant_id = tenant
        .as_ref()
        .map(|extension| extension.0.tenant_id.as_str())
        .unwrap_or("");
    // Lease key matches SessionSummaryRebuildJob: empty tenant → default workspace.
    let scope_key = crate::workspace_scope::effective_workspace_id(tenant_id);
    let leases = crate::async_jobs::PostgresLeaseStore::from_engines(&state.engines);
    let holder = session_summary_rebuild_holder_id(
        &state.engines.config().async_jobs.resolved_instance_id(),
    );
    let ttl =
        std::time::Duration::from_secs(state.engines.config().async_jobs.lease_ttl_seconds.max(1));
    let maintenance = state
        .engines
        .maintenance_engine()
        .await
        .map_err(storage_error)?;
    let token = leases
        .acquire_lease(
            crate::session_summary::WORKSPACE_SESSION_SUMMARY_REBUILD_JOB,
            scope_key,
            &holder,
            ttl,
        )
        .await
        .map_err(storage_error)?;
    let Some(token) = token else {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": "workspace_session_summary_rebuild lease held" })),
        ));
    };

    let (lost_tx, mut lost_rx) = tokio::sync::watch::channel(false);
    let hb_leases = leases.clone();
    let hb_job = crate::session_summary::WORKSPACE_SESSION_SUMMARY_REBUILD_JOB;
    let hb_scope = scope_key.to_string();
    let hb_token = token.clone();
    let heartbeat_every =
        std::time::Duration::from_secs(state.engines.config().async_jobs.heartbeat_seconds.max(1));
    let heartbeat = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(heartbeat_every);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(err) = hb_leases
                .heartbeat_lease(hb_job, &hb_scope, &hb_token, ttl)
                .await
            {
                warn!(scope = %hb_scope, error = %err, "session-summary rebuild lease heartbeat failed");
                crate::self_monitoring::record_lease_heartbeat_failure(hb_job, &hb_scope);
                let _ = lost_tx.send(true);
                break;
            }
        }
    });
    let result = tokio::select! {
        result = maintenance.rebuild_session_summary_for_key(
            scope_key,
            request.from,
            request.to,
            cfg.max_reduce_span_seconds,
        ) => result,
        changed = lost_rx.changed() => {
            let _ = changed;
            Err(anyhow::anyhow!("lease lost during session-summary rebuild"))
        }
    };
    heartbeat.abort();
    let _ = heartbeat.await;
    let _ = leases
        .release_lease(
            crate::session_summary::WORKSPACE_SESSION_SUMMARY_REBUILD_JOB,
            scope_key,
            &token,
        )
        .await;
    let sessions_upserted = result.map_err(storage_error)?;
    Ok(Json(SessionSummaryRebuildResponse { sessions_upserted }))
}

async fn query_scores(
    state: &AppState,
    tenant: Option<&TenantInfo>,
    sql: &str,
) -> Result<Vec<Score>, ApiError> {
    let result = state
        .execute_tenant_scoped_trusted_sql(tenant, trusted_query(sql.to_string())?)
        .await
        .map_err(storage_error)?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| map_score(&result.columns, row))
        .collect())
}

fn map_span_summary(columns: &[String], row: &[Value]) -> Option<SpanSummary> {
    Some(SpanSummary {
        trace_id: required_string(columns, row, "trace_id")?,
        span_id: required_string(columns, row, "span_id")?,
        parent_span_id: optional_string(columns, row, "parent_span_id"),
        session_id: optional_string(columns, row, "session_id"),
        name: required_string(columns, row, "name").unwrap_or_default(),
        span_type: required_string(columns, row, "span_type").unwrap_or_else(|| "span".to_string()),
        start_time: required_timestamp(columns, row, "start_time")?,
        end_time: optional_timestamp(columns, row, "end_time"),
        status_code: optional_string(columns, row, "status_code"),
        model_name: optional_string(columns, row, "model_name"),
        model_provider: optional_string(columns, row, "model_provider"),
        user_id: optional_string(columns, row, "user_id"),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
    })
}

fn map_span_detail(columns: &[String], row: &[Value]) -> Option<SpanDetail> {
    let summary = map_span_summary(columns, row)?;
    Some(SpanDetail {
        summary,
        attributes: map_string_map(column_value(columns, row, "attributes")),
        events: map_events(column_value(columns, row, "events")),
        scores: Vec::new(),
    })
}

fn map_trace(columns: &[String], row: &[Value]) -> Option<Trace> {
    Some(Trace {
        trace_id: required_string(columns, row, "trace_id")?,
        session_id: optional_string(columns, row, "session_id"),
        name: optional_string(columns, row, "name"),
        start_time: required_timestamp(columns, row, "start_time")?,
        end_time: required_timestamp(columns, row, "end_time")?,
        span_count: optional_i64(columns, row, "span_count").unwrap_or(0),
        error_count: optional_i64(columns, row, "error_count").unwrap_or(0),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
        user_id: optional_string(columns, row, "user_id"),
    })
}

struct SessionAggregate {
    trace_count: i64,
    span_count: i64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    total_tokens: Option<i64>,
    total_cost: Option<f64>,
    user_ids: Vec<String>,
}

/// Truncate `items` to `limit` and return an opaque cursor when the page was
/// actually cut short. Shared by the live search path ([`crate::session_summary::list`])
/// and covered here by unit tests against plain [`SessionSummary`] fixtures.
pub(crate) fn next_cursor_from_sessions(
    items: &mut Vec<SessionSummary>,
    limit: usize,
) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.session_id))
}

fn map_session_aggregate(columns: &[String], row: &[Value]) -> Option<SessionAggregate> {
    let user_ids = match column_value(columns, row, "session_user_ids") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        _ => Vec::new(),
    };
    Some(SessionAggregate {
        trace_count: optional_i64(columns, row, "session_trace_count").unwrap_or(0),
        span_count: optional_i64(columns, row, "session_span_count").unwrap_or(0),
        input_tokens: optional_i64(columns, row, "session_input_tokens"),
        output_tokens: optional_i64(columns, row, "session_output_tokens"),
        total_tokens: optional_i64(columns, row, "session_total_tokens"),
        total_cost: optional_f64(columns, row, "session_total_cost"),
        user_ids,
    })
}

fn map_session_scores(columns: &[String], row: &[Value]) -> Vec<Score> {
    let Some(value) = column_value(columns, row, "session_scores") else {
        return Vec::new();
    };
    let decoded = match value {
        Value::String(json) => serde_json::from_str::<Value>(json).ok(),
        value => Some(value.clone()),
    };
    let Some(Value::Array(mut items)) = decoded else {
        return Vec::new();
    };
    for item in &mut items {
        let Some(object) = item.as_object_mut() else {
            continue;
        };
        let Some(timestamp) = object.get("timestamp").and_then(Value::as_str) else {
            continue;
        };
        if let Some(timestamp) = parse_timestamp_text(timestamp) {
            object.insert(
                "timestamp".into(),
                Value::String(timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)),
            );
        }
    }
    serde_json::from_value::<Vec<Score>>(Value::Array(items)).unwrap_or_default()
}

fn map_score(columns: &[String], row: &[Value]) -> Option<Score> {
    let data_type = match optional_string(columns, row, "data_type")
        .unwrap_or_default()
        .as_str()
    {
        "numeric" => ScoreDataType::Numeric,
        "categorical" => ScoreDataType::Categorical,
        "boolean" => ScoreDataType::Boolean,
        "text" => ScoreDataType::Text,
        _ => return None,
    };
    let source = match optional_string(columns, row, "source")
        .unwrap_or_default()
        .as_str()
    {
        "api" => ScoreSource::Api,
        "user" => ScoreSource::User,
        "evaluator" => ScoreSource::Evaluator,
        "annotation" => ScoreSource::Annotation,
        _ => return None,
    };
    let timestamp = required_timestamp(columns, row, "timestamp")?;
    Some(Score {
        score_id: required_string(columns, row, "score_id")?,
        timestamp,
        trace_id: optional_string(columns, row, "trace_id"),
        span_id: optional_string(columns, row, "span_id"),
        session_id: optional_string(columns, row, "session_id"),
        name: required_string(columns, row, "name")?,
        data_type,
        numeric_value: optional_f64(columns, row, "numeric_value"),
        string_value: optional_string(columns, row, "string_value"),
        boolean_value: column_value(columns, row, "boolean_value").and_then(|v| v.as_bool()),
        source,
        comment: optional_string(columns, row, "comment"),
        config_id: optional_string(columns, row, "config_id"),
        author_id: optional_string(columns, row, "author_id"),
        metadata: map_string_map(column_value(columns, row, "metadata")),
        tenant_id: optional_string(columns, row, "tenant_id"),
    })
}

fn next_cursor_from_spans(items: &mut Vec<SpanSummary>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.span_id))
}

fn next_cursor_from_span_details(items: &mut Vec<SpanDetail>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.summary.start_time, &item.summary.span_id))
}

fn column_value<'a>(columns: &[String], row: &'a [Value], name: &str) -> Option<&'a Value> {
    let index = columns.iter().position(|column| column == name)?;
    row.get(index)
}

fn required_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    optional_string(columns, row, name).filter(|value| !value.is_empty())
}

fn optional_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn optional_i64(columns: &[String], row: &[Value], name: &str) -> Option<i64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|v| v as i64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn optional_f64(columns: &[String], row: &[Value], name: &str) -> Option<f64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn required_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    optional_timestamp(columns, row, name)
}

fn optional_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    let value = column_value(columns, row, name)?;
    parse_timestamp_value(value)
}

fn parse_timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::Null => None,
        Value::String(text) => parse_timestamp_text(text),
        Value::Number(number) => number
            .as_i64()
            .and_then(DateTime::from_timestamp_micros)
            .or_else(|| number.as_i64().and_then(DateTime::from_timestamp_millis)),
        _ => None,
    }
}

fn parse_timestamp_text(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt.with_timezone(&Utc));
    }
    // DuckDB JSON bridge can encode TIMESTAMP_NS as "Nanosecond:<epoch>".
    if let Some((unit, raw)) = text.split_once(':') {
        if let Ok(epoch) = raw.parse::<i64>() {
            return match unit {
                "Microsecond" | "\"Microsecond\"" => DateTime::from_timestamp_micros(epoch),
                "Millisecond" | "\"Millisecond\"" => DateTime::from_timestamp_millis(epoch),
                "Second" | "\"Second\"" => DateTime::from_timestamp(epoch, 0),
                "Nanosecond" | "\"Nanosecond\"" => DateTime::from_timestamp(
                    epoch.div_euclid(1_000_000_000),
                    epoch.rem_euclid(1_000_000_000) as u32,
                ),
                _ => None,
            };
        }
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    None
}

fn map_string_map(value: Option<&Value>) -> HashMap<String, String> {
    value.map(variant_json_to_string_map).unwrap_or_default()
}

fn map_events(value: Option<&Value>) -> Vec<Value> {
    let parsed = value.cloned().map(parse_projected_json_value);
    match parsed.as_ref() {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Object(map) => {
                    let mut normalized = Map::new();
                    for (key, value) in map {
                        let key = key.to_owned();
                        if key == "attributes" {
                            normalized.insert(
                                key,
                                Value::Object(
                                    map_string_map(Some(value))
                                        .into_iter()
                                        .map(|(k, v)| (k, Value::String(v)))
                                        .collect(),
                                ),
                            );
                        } else if key == "timestamp" {
                            normalized.insert(
                                key,
                                parse_timestamp_value(value)
                                    .map(|dt| {
                                        Value::String(
                                            dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                                        )
                                    })
                                    .unwrap_or_else(|| value.clone()),
                            );
                        } else {
                            normalized.insert(key, value.clone());
                        }
                    }
                    Value::Object(normalized)
                }
                other => other.clone(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn bad_request(message: String) -> ApiError {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message })))
}

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" })))
}

/// Map a storage-layer failure to a response the caller can act on.
///
/// This used to flatten every error into a bare 503 with `{"error": "query
/// unavailable"}`, keeping the real cause server-side only. That made an
/// operational fault (a dead query worker) indistinguishable from a malformed
/// SQL bug from the client's side, and cost a long debugging session to trace
/// back by elimination. Classify instead, and pass the detail through.
/// Classify a storage-layer failure without handing internals to the caller.
///
/// Two separate concerns, both learned the hard way:
///
/// 1. **Never echo the raw error.** DuckDB surfaces the full ATTACH target on
///    connection failure, and for a Postgres catalog that string is the DSN --
///    including the plaintext password. GCS HMAC secrets reach it the same way
///    via `CREATE SECRET` statement echo, and binder errors leak catalog names,
///    tenant metadata schemas and column lists. A tenant bearer token is enough
///    to trigger all of it. The caller gets a category; operators get the detail
///    from the log line, correlated by `error_id`.
///
/// 2. **Classify on the error, not on a substring of it.** DuckDB echoes the
///    offending statement, and that statement embeds caller-supplied literals
///    (`user_id`, `model_name`), so a client could previously flip a permanent
///    500 into a retryable 503 just by searching for a model named
///    "IO Error". Matching now happens only against the prefix DuckDB puts at
///    the very start of its message.
fn storage_error(error: anyhow::Error) -> ApiError {
    let raw = error.to_string();

    // Correlates the client-visible response with the full server-side detail.
    // Deterministic hash, not a UUID: no new dependency, and identical failures
    // collapse to the same id in the logs.
    let error_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        raw.hash(&mut h);
        format!("{:016x}", h.finish())
    };
    warn!("llm query failed [{}]: {}", error_id, raw);

    let kind = classify_storage_error_chain(&error);
    let status = if kind.retryable {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (
        status,
        Json(json!({
            "error": kind.code,
            "retryable": kind.retryable,
            "error_id": error_id,
        })),
    )
}

struct StorageErrorKind {
    code: &'static str,
    retryable: bool,
}

/// Classify using the full chain: our `.context()` prefixes must not hide DuckDB
/// `IO Error` / `HTTP Error` markers (reduce/rebuild wrap as `query aggregate: …`).
fn classify_storage_error_chain(error: &anyhow::Error) -> StorageErrorKind {
    let top = error.to_string();
    let top_kind = classify_storage_error(&top);
    if top_kind.code != "query_failed" {
        return top_kind;
    }
    let root = error.root_cause().to_string();
    if root != top {
        return classify_storage_error(&root);
    }
    top_kind
}

/// Match only on the leading marker DuckDB emits, so echoed SQL (which contains
/// caller-controlled literals) cannot influence the classification.
fn classify_storage_error(raw: &str) -> StorageErrorKind {
    let head = raw.trim_start();

    // Our own engine-construction and worker-pool failures: operational, worth
    // retrying. The connect/attach prefixes matter most -- they wrap the
    // underlying "IO Error: could not connect to Postgres", so anchoring on the
    // leading marker (correct, since DuckDB's echoed SQL must not reach it)
    // would otherwise report the single most likely production fault, a
    // Postgres blip, as a permanent defect telling the caller not to retry.
    for marker in [
        "DuckDB worker",
        "DuckDB query engine failed to start",
        "DuckDB open failed",
        "DuckDB ATTACH failed",
        "DuckLake attach failed",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    // A FATAL (invalidated database) error that escapes worker self-heal
    // means the rebuild failed mid-request; the next attempt gets a fresh
    // connection, so it is retryable. INTERNAL errors are NOT listed here:
    // the query that trips the assertion fails deterministically.
    if head.starts_with("FATAL Error") {
        return StorageErrorKind {
            code: "query_unavailable",
            retryable: true,
        };
    }

    // Object-store and network faults, including S3/MinIO throttling and 5xx.
    // "HTTP Error" was missing before, so every 429 and 502 from the object
    // store was reported as a permanent 500 -- exactly the case worth retrying.
    for marker in [
        "Connection Error",
        "IO Error",
        "HTTP Error",
        "Network Error",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    // Binder/Catalog/Parser/Conversion errors are defects: identical on retry.
    StorageErrorKind {
        code: "query_failed",
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::sql_support::{cursor_predicate, decode_cursor};

    #[test]
    fn rebuild_holder_id_is_unique_per_request() {
        let first = session_summary_rebuild_holder_id("replica-a");
        let second = session_summary_rebuild_holder_id("replica-a");
        assert_ne!(first, second);
        assert!(first.starts_with("ops-rebuild-replica-a-"));
    }

    #[test]
    fn storage_error_never_echoes_the_underlying_message() {
        // DuckDB puts the full ATTACH target in connection errors, and for a
        // Postgres catalog that string carries the plaintext password.
        let leaky = anyhow::anyhow!(
            "IO Error: Failed to attach DuckLake at path \"postgres:host=h dbname=d \
             user=u password=hunter2\": connection refused"
        );
        let (_, body) = storage_error(leaky);
        let rendered = body.0.to_string();
        assert!(!rendered.contains("hunter2"), "password leaked: {rendered}");
        assert!(!rendered.contains("postgres:"), "DSN leaked: {rendered}");
        assert!(
            rendered.contains("error_id"),
            "no correlation id: {rendered}"
        );
    }

    #[test]
    fn storage_error_ignores_client_controlled_text_in_echoed_sql() {
        // DuckDB echoes the offending statement, and that statement embeds
        // caller-supplied literals -- a model named "IO Error" must not be able
        // to turn a permanent failure into a retryable one.
        let (status, body) = storage_error(anyhow::anyhow!(
            "Binder Error: no such column\nLINE 1: ... model_name = 'IO Error injected'"
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_marks_invalidated_database_as_retryable() {
        // A FATAL that escapes worker self-heal means the rebuild failed
        // mid-request; the next attempt gets a fresh connection. The INTERNAL
        // error that *triggered* the invalidation stays non-retryable: that
        // query fails deterministically (2026-08-03 outage, ducklake inlined
        // data reader).
        let (status, body) = storage_error(anyhow::anyhow!(
            "FATAL Error: Failed: database has been invalidated because of a previous \
             fatal error. The database must be restarted prior to being used again."
        ));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));

        let (status, body) = storage_error(anyhow::anyhow!(
            "INTERNAL Error: Attempted to access index 0 within vector of size 0"
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_treats_object_store_faults_as_retryable() {
        // S3/MinIO throttling and 5xx arrive with an "HTTP Error" prefix; these
        // were previously reported as permanent 500s.
        let (status, body) = storage_error(anyhow::anyhow!(
            "HTTP Error: HTTP GET error reading 's3://w/x.parquet' (HTTP 503)"
        ));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));
    }

    #[test]
    fn storage_error_classifies_through_anyhow_context_prefix() {
        // reduce/rebuild wrap DuckDB faults as `query aggregate: …`. Matching
        // only the top-level string made every Parquet/GCS failure look like a
        // permanent binder defect (`query_failed`).
        let err =
            anyhow::anyhow!("HTTP Error: HTTP GET error reading 'gs://b/x.parquet' (HTTP 403)")
                .context("query aggregate");
        let (status, body) = storage_error(err);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["error"], serde_json::json!("query_unavailable"));
        assert_eq!(body.0["retryable"], serde_json::json!(true));
    }

    #[test]
    fn cursor_literal_keeps_microsecond_precision() {
        // start_time is a microsecond-precision TIMESTAMPTZ and the cursor
        // round-trips a real column value. Rendering it at millisecond
        // precision truncated the literal below the true value, so the
        // keyset predicate `start_time < cursor` silently dropped every row
        // sharing that millisecond: the last page came back empty with a null
        // next_cursor and no error. Verified on DuckDB 1.5.5 -- paging 8
        // sessions at limit=2 lost the final two.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00.123456Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(
            sql.contains("00.123456"),
            "cursor literal truncated below the true value, pages will drop rows: {sql}"
        );
    }

    #[test]
    fn session_search_rejects_cursor_with_ascending_order() {
        // cursor_predicate emits `<`; under ASC that pages backwards forever.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        request.order = SortDirection::Asc;
        assert!(compile_session_search_sql(&request, 50).is_err());
    }

    #[test]
    fn session_search_puts_cursor_predicate_outside_the_aggregate() {
        // start_time is MIN(timestamp): in the inner WHERE it neither binds
        // ("WHERE clause cannot contain aggregates") nor would be correct.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        let group_by = sql.find("GROUP BY session_id").expect("group by");
        let cursor_at = sql.rfind("start_time <").expect("cursor predicate");
        assert!(
            cursor_at > group_by,
            "cursor predicate must sit after the aggregation, got: {sql}"
        );
        assert!(
            !sql.contains("make_timestamp_ns(epoch_ns(start_time))"),
            "cursor must not wrap start_time: {sql}"
        );
    }

    #[test]
    fn storage_error_separates_transient_faults_from_defects() {
        // A dead query worker is operational and retryable
        let (status, body) = storage_error(anyhow::anyhow!("DuckDB worker channel closed"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));

        // A SQL defect will fail identically on retry -- 503 would tell the
        // caller to keep hammering a request that can never succeed
        let (status, body) = storage_error(anyhow::anyhow!("Binder Error: no such column"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_correlates_without_disclosing() {
        // Replaces an earlier test that asserted the cause was passed through
        // to the caller -- that behaviour is exactly what leaked the Postgres
        // password. Operators correlate via error_id in the log line instead.
        let (_, body) = storage_error(anyhow::anyhow!("open_connection: too many clients"));
        let rendered = body.0.to_string();
        assert!(
            !rendered.contains("too many clients"),
            "cause disclosed: {rendered}"
        );
        let id = body.0["error_id"].as_str().expect("error_id");
        assert_eq!(id.len(), 16, "error_id should be a stable 16-hex digest");
    }

    fn session_search_request() -> SessionSearchRequest {
        SessionSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-25T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            has_errors: None,
            user_id: None,
            model_name: None,
            agent_name: None,
            roots_only: true,
            order_by: SessionOrderBy::StartTime,
            order: SortDirection::Desc,
            limit: Some(50),
            cursor: None,
        }
    }

    #[test]
    fn session_search_prefers_persisted_agent_name_then_attr_and_filters_outer() {
        let sql = compile_session_search_sql(&session_search_request(), 50).expect("sql");
        let col_pos = sql
            .find("COALESCE(agent_name,")
            .expect("agent_name must prefer persisted column");
        let attr_pos = sql
            .find("sp.agent.name")
            .expect("agent_name must fall back to sp.agent.name");
        assert!(
            col_pos < attr_pos,
            "persisted agent_name must lead attr fallback: {sql}"
        );
        assert!(
            sql.contains("arg_min(message_type, timestamp) FILTER"),
            "agent_name must fall back to agent span name: {sql}"
        );

        let mut request = session_search_request();
        request.agent_name = Some("support-refund-agent".into());
        let filtered = compile_session_search_sql(&request, 50).expect("sql");
        assert!(
            filtered.contains("agent_name = 'support-refund-agent'"),
            "agent filter must apply on outer aggregate: {filtered}"
        );
    }

    #[test]
    fn session_search_aggregates_in_sql_and_bounds_time() {
        let sql = compile_session_search_sql(&session_search_request(), 50).expect("sql");
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        assert_sql_has_otlp_time_predicates(&sql);
        assert!(sql.contains("GROUP BY session_id"));
        // spans with no session id must not become a session row
        assert!(sql.contains("session_id IS NOT NULL AND session_id <> ''"));
        // recording spans share session_id but must not inflate LLM session rows
        assert!(sql.contains("<> 'recording'"));
        // one extra row is what tells us another page exists
        assert!(sql.contains("LIMIT 51"));
    }

    #[test]
    fn session_search_filters_errors_in_having_not_in_memory() {
        let mut request = session_search_request();
        request.has_errors = Some(true);
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("HAVING error_count > 0"));

        request.has_errors = Some(false);
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("HAVING error_count = 0"));

        request.has_errors = None;
        request.roots_only = false;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(!sql.contains("HAVING"));
    }

    #[test]
    fn session_search_roots_only_filters_legacy_child_sessions() {
        let request = session_search_request();
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(
            sql.contains("sp.metadata.opencode.parentSessionID"),
            "roots_only must inspect legacy parent metadata: {sql}"
        );

        let mut all = session_search_request();
        all.roots_only = false;
        let sql = compile_session_search_sql(&all, 50).expect("sql");
        assert!(
            !sql.contains("sp.metadata.opencode.parentSessionID"),
            "roots_only=false must not filter parent metadata: {sql}"
        );
    }

    #[test]
    fn session_search_orders_over_the_whole_window() {
        let mut request = session_search_request();
        request.order_by = SessionOrderBy::ErrorCount;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("ORDER BY error_count DESC"));

        request.order_by = SessionOrderBy::Duration;
        request.order = SortDirection::Asc;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("ORDER BY duration_ms ASC"));

        // NULL cost must not outrank a real one
        request.order_by = SessionOrderBy::TotalCost;
        request.order = SortDirection::Desc;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("total_cost DESC NULLS LAST"));
    }

    #[test]
    fn session_search_rejects_cursor_with_incompatible_ordering() {
        // The cursor encodes (start_time, session_id); reusing it under another
        // ordering would silently skip or repeat rows.
        let mut request = session_search_request();
        request.order_by = SessionOrderBy::ErrorCount;
        request.cursor = Some(encode_cursor(request.from, "sess-1"));
        let err = compile_session_search_sql(&request, 50).expect_err("must reject");
        assert!(err.contains("order_by=start_time"));

        request.order_by = SessionOrderBy::StartTime;
        assert!(compile_session_search_sql(&request, 50).is_ok());
    }

    #[test]
    fn session_search_escapes_filter_literals() {
        let mut request = session_search_request();
        request.user_id = Some("u'; DROP TABLE traces; --".to_string());
        request.model_name = Some("gpt-5.2'".to_string());
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        // The whole payload must land inside one literal with its quote doubled,
        // so the `;` never terminates a statement.
        assert!(sql.contains("'u''; DROP TABLE traces; --'"));
        assert!(sql.contains("'gpt-5.2'''"));
        // and the injected quote must never appear unescaped
        assert!(!sql.contains("'u'; "));
    }

    #[test]
    fn session_search_rejects_inverted_range() {
        let mut request = session_search_request();
        std::mem::swap(&mut request.from, &mut request.to);
        assert!(compile_session_search_sql(&request, 50).is_err());
    }

    #[test]
    fn session_cursor_only_emitted_when_a_page_was_actually_cut() {
        let make = |id: &str| SessionSummary {
            session_id: id.to_string(),
            start_time: DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            end_time: None,
            trace_count: 1,
            span_count: 1,
            error_count: 0,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            agent_name: None,
            user_ids: vec![],
            models: vec![],
        };

        let mut exact = vec![make("a"), make("b")];
        assert!(next_cursor_from_sessions(&mut exact, 2).is_none());
        assert_eq!(exact.len(), 2);

        let mut overflowing = vec![make("a"), make("b"), make("c")];
        let cursor = next_cursor_from_sessions(&mut overflowing, 2).expect("cursor");
        assert_eq!(overflowing.len(), 2);
        assert_eq!(decode_cursor(&cursor).expect("decode").id, "b");
    }

    #[test]
    fn search_sql_requires_time_bounds_and_escapes_literals() {
        let request = SpanSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            span_types: vec!["generation".to_string()],
            model_name: Some("gpt-4o'; DROP TABLE traces; --".to_string()),
            user_id: Some("user-1".to_string()),
            session_id: Some("sess-1".to_string()),
            trace_id: None,
            limit: Some(999),
            cursor: None,
        };
        let sql = compile_span_search_sql(&request).expect("sql");
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("LIMIT 201"));
        assert!(sql.contains("gpt-4o''; DROP TABLE traces; --"));
        assert!(sql.contains(&format!(
            "COALESCE({}, 'span') IN ('generation')",
            crate::sql::llm::expr_observation_type()
        )));
        assert!(sql.contains("ORDER BY timestamp DESC, span_id DESC"));
        // Prefer promoted column before bag path.
        let obs_pos = sql.find("observation_type").expect("observation_type");
        let bag_pos = sql
            .find("attributes['sp.observation.type']")
            .expect("bag fallback");
        assert!(
            obs_pos < bag_pos,
            "promoted observation_type must lead bag access"
        );
    }

    #[test]
    fn search_and_session_sql_prefer_promoted_hot_attrs() {
        let request = SpanSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            span_types: vec!["generation".to_string()],
            model_name: Some("gpt-4o".to_string()),
            user_id: Some("user-1".to_string()),
            session_id: None,
            trace_id: None,
            limit: Some(10),
            cursor: None,
        };
        let search = compile_span_search_sql(&request).expect("search");
        assert!(search.contains("COALESCE(observation_type,"));
        assert!(search.contains("COALESCE(model_name,"));
        assert!(search.contains("COALESCE(user_id,"));
        assert!(
            search.find("observation_type").unwrap()
                < search.find("attributes['sp.observation.type']").unwrap()
        );
        assert!(
            search.find("model_name").unwrap()
                < search.find("attributes['gen_ai.request.model']").unwrap()
        );

        let mut session = session_search_request();
        session.user_id = Some("user-1".into());
        session.model_name = Some("gpt-4o".into());
        let session_sql = compile_session_search_sql(&session, 10).expect("session");
        assert!(session_sql.contains("COALESCE(observation_type,"));
        assert!(session_sql.contains("COALESCE(model_name,"));
        assert!(
            session_sql.find("observation_type").unwrap()
                < session_sql
                    .find("attributes['sp.observation.type']")
                    .unwrap()
        );
    }

    #[test]
    fn search_sql_rejects_inverted_range() {
        let request = SpanSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            span_types: vec![],
            model_name: None,
            user_id: None,
            session_id: None,
            trace_id: None,
            limit: None,
            cursor: None,
        };
        assert!(compile_span_search_sql(&request).is_err());
    }

    #[test]
    fn cursor_round_trip_and_predicate() {
        let ts = DateTime::parse_from_rfc3339("2026-07-18T23:22:00.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let encoded = encode_cursor(ts, "span-1");
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded.id, "span-1");
        assert_eq!(decoded.t, ts);
        assert!(decode_cursor("%%%not-base64%%%").is_err());
        let predicate = cursor_predicate(&encoded, "timestamp", "span_id").unwrap();
        assert!(predicate.contains("timestamp <"));
        assert!(predicate.contains("span_id <"));
    }

    #[test]
    fn score_sql_requires_time_bounds() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = compile_scores_for_trace_sql("trace-1", from, to).expect("trace scores sql");
        assert!(sql.contains("trace_id = 'trace-1'"));
        assert!(sql.contains("span_id IN (SELECT"));
        assert!(sql.contains("::TIMESTAMP_NS"), "scores outer clock: {sql}");
        assert!(
            sql.contains("::TIMESTAMP_NS"),
            "traces subquery clock: {sql}"
        );
        assert!(!sql.contains("make_timestamp_ns(epoch_ns("));
        assert!(!sql.contains("record_date"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));
        assert!(compile_scores_for_trace_sql("trace-1", to, from).is_err());
    }

    #[test]
    fn session_detail_sql_includes_scores_and_both_event_time_bounds() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = compile_session_detail_sql("sess-1", from, to).expect("session detail sql");
        assert!(sql.contains("session_scores AS MATERIALIZED"));
        assert!(sql.contains("session_id = 'sess-1'"));
        assert!(sql.contains("trace_id IN (SELECT DISTINCT trace_id FROM session_spans)"));
        assert!(sql.contains("span_id IN (SELECT span_id FROM session_spans)"));
        assert!(sql.contains("\"timestamp\" := timestamp"));
        assert!(sql.contains("first(score_aggregate.session_scores) AS session_scores"));
        assert!(sql.contains("::TIMESTAMP_NS"), "scores clock: {sql}");
        assert!(!sql.contains("make_timestamp_ns(epoch_ns("));

        assert!(compile_session_detail_sql("sess-1", to, from).is_err());
    }

    #[test]
    fn session_detail_event_fallback_does_not_read_nested_events() {
        let from = Utc::now() - chrono::Duration::hours(1);
        let to = Utc::now();
        let sql =
            crate::sql::llm::compile_session_detail_sql_without_events("sess-1", from, to).unwrap();
        let events_column = crate::storage::schema::variant::variant_as_json("events");
        assert!(
            !sql.contains(&events_column),
            "fallback must not touch the unreadable nested events column: {sql}"
        );
        assert!(sql.contains("'[]' AS events"));
        assert!(
            sql.contains(&crate::storage::schema::variant::variant_as_json(
                "attributes"
            )),
            "fallback preserves span attributes"
        );
    }

    #[test]
    fn session_detail_fallback_only_matches_known_nested_event_failure() {
        assert!(is_inlined_nested_event_failure(&anyhow::anyhow!(
            "INTERNAL Error: Attempted to access index 0 within vector of size 0\nLINE 1: SELECT events"
        )));
        assert!(!is_inlined_nested_event_failure(&anyhow::anyhow!(
            "IO Error: object store unavailable"
        )));
        assert!(!is_inlined_nested_event_failure(&anyhow::anyhow!(
            "Binder Error: missing column"
        )));
        assert!(!is_inlined_nested_event_failure(&anyhow::anyhow!(
            "FATAL Error: database has been invalidated"
        )));
    }

    #[tokio::test]
    async fn session_detail_fallback_retries_only_the_known_error_and_marks_partial() {
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_attempts = attempts.clone();
        let (result, events_complete) = execute_session_detail_with_event_fallback(move |events| {
            let attempts = observed_attempts.clone();
            async move {
                attempts.lock().unwrap().push(events);
                if events {
                    Err(anyhow::anyhow!(
                        "INTERNAL Error: Attempted to access index 0 within vector of size 0"
                    ))
                } else {
                    Ok("scalar detail")
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, "scalar detail");
        assert!(!events_complete);
        assert_eq!(*attempts.lock().unwrap(), [true, false]);

        let attempts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_attempts = attempts.clone();
        let error = execute_session_detail_with_event_fallback(move |events| {
            let attempts = observed_attempts.clone();
            async move {
                attempts.lock().unwrap().push(events);
                Err::<&str, anyhow::Error>(anyhow::anyhow!("IO Error: object store unavailable"))
            }
        })
        .await
        .unwrap_err();
        assert!(error.to_string().starts_with("IO Error"));
        assert_eq!(*attempts.lock().unwrap(), [true]);
    }

    #[test]
    fn session_detail_serializes_incomplete_event_status() {
        let now = Utc::now();
        let detail = SessionDetail {
            session_id: "session-1".to_string(),
            from: now,
            to: now,
            trace_count: 1,
            span_count: 1,
            user_ids: Vec::new(),
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            events_complete: false,
            spans: Vec::new(),
            scores: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(detail).unwrap()["events_complete"],
            false,
            "partial session responses must explicitly report omitted events"
        );
    }

    #[test]
    fn maps_span_summary_from_row() {
        let columns = vec![
            "trace_id".into(),
            "span_id".into(),
            "parent_span_id".into(),
            "session_id".into(),
            "name".into(),
            "span_type".into(),
            "start_time".into(),
            "end_time".into(),
            "status_code".into(),
            "model_name".into(),
            "model_provider".into(),
            "user_id".into(),
            "input_tokens".into(),
            "output_tokens".into(),
            "total_tokens".into(),
            "total_cost".into(),
        ];
        let row = vec![
            json!("trace-1"),
            json!("span-1"),
            Value::Null,
            json!("sess-1"),
            json!("chat"),
            json!("generation"),
            json!("Microsecond:1721349720000000"),
            Value::Null,
            json!("OK"),
            json!("gpt-4o"),
            json!("openai"),
            json!("user-1"),
            json!(10),
            json!(20),
            json!(30),
            json!(0.01),
        ];
        let summary = map_span_summary(&columns, &row).expect("summary");
        assert_eq!(summary.span_type, "generation");
        assert_eq!(summary.input_tokens, Some(10));
        assert_eq!(summary.model_name.as_deref(), Some("gpt-4o"));
    }

    #[test]
    fn maps_scores_from_holistic_session_row() {
        let columns = vec!["session_scores".to_string()];
        let row = vec![Value::String(
            json!([{
                "score_id": "score-1",
                "timestamp": "2026-09-01T00:00:02Z",
                "trace_id": "trace-1",
                "span_id": "span-1",
                "session_id": null,
                "name": "quality",
                "data_type": "numeric",
                "numeric_value": 0.8,
                "string_value": null,
                "boolean_value": null,
                "source": "evaluator",
                "comment": null,
                "config_id": null,
                "author_id": null,
                "metadata": {"suite": "integration"}
            }])
            .to_string(),
        )];
        let scores = map_session_scores(&columns, &row);
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].score_id, "score-1");
        assert_eq!(scores[0].numeric_value, Some(0.8));
        assert_eq!(
            scores[0].metadata.get("suite").map(String::as_str),
            Some("integration")
        );
    }

    #[test]
    fn maps_duckdb_timestamp_in_holistic_score_json() {
        let conn = duckdb::Connection::open_in_memory().expect("in-memory DuckDB");
        conn.execute_batch("SET TimeZone = 'America/Los_Angeles'")
            .expect("set non-UTC session timezone");
        conn.execute_batch(
            "CREATE TABLE traces (\
                trace_id VARCHAR, span_id VARCHAR, parent_span_id VARCHAR, session_id VARCHAR, \
                message_type VARCHAR, observation_type VARCHAR, timestamp TIMESTAMP_NS, \
                end_timestamp TIMESTAMP_NS, status_code VARCHAR, model_name VARCHAR, \
                model_provider VARCHAR, user_id VARCHAR, input_tokens BIGINT, \
                output_tokens BIGINT, total_tokens BIGINT, total_cost DOUBLE, \
                attributes JSON, events JSON\
            );\
            INSERT INTO traces VALUES (\
                'trace-1', 'span-1', NULL, 'sess-1', 'chat', 'generation', \
                TIMESTAMP_NS '2026-09-28 01:00:00', TIMESTAMP_NS '2026-09-28 01:01:00', \
                'OK', 'model', 'provider', 'user', 10, 20, 30, 0.01, '{}', '[]'\
            );\
            CREATE TABLE scores (\
                score_id VARCHAR, timestamp TIMESTAMP_NS, trace_id VARCHAR, span_id VARCHAR, \
                session_id VARCHAR, name VARCHAR, data_type VARCHAR, numeric_value DOUBLE, \
                string_value VARCHAR, boolean_value BOOLEAN, source VARCHAR, comment VARCHAR, \
                config_id VARCHAR, author_id VARCHAR, metadata MAP(VARCHAR, VARCHAR)\
            );\
            INSERT INTO scores VALUES (\
                'score-1', TIMESTAMP_NS '2026-09-28 01:02:03.123456789', \
                'trace-1', 'span-1', 'sess-1', 'quality', 'numeric', 0.91, \
                NULL, NULL, 'evaluator', NULL, NULL, NULL, MAP(['suite'], ['integration'])\
            );",
        )
        .expect("create detail fixture tables");
        let from = DateTime::parse_from_rfc3339("2026-09-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-09-28T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let detail_sql = compile_session_detail_sql("sess-1", from, to).unwrap();
        assert!(
            detail_sql.contains("CAST(metadata AS JSON) AS metadata"),
            "session detail must serialize score metadata before aggregation: {detail_sql}"
        );
        let encoded: String = conn
            .query_row(
                &format!("SELECT session_scores FROM ({detail_sql}) AS detail LIMIT 1"),
                [],
                |row| row.get(0),
            )
            .expect("execute holistic session detail SQL");
        let scores = map_session_scores(
            &["session_scores".to_string()],
            &[Value::String(encoded.clone())],
        );
        assert_eq!(scores.len(), 1, "score JSON returned by DuckDB: {encoded}");
        assert_eq!(scores[0].score_id, "score-1");
        assert_eq!(
            scores[0].timestamp,
            DateTime::parse_from_rfc3339("2026-09-28T01:02:03.123456789Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn maps_negative_duckdb_nanosecond_timestamp() {
        let timestamp = parse_timestamp_text("Nanosecond:-1").expect("valid pre-epoch instant");
        assert_eq!(timestamp.timestamp(), -1);
        assert_eq!(timestamp.timestamp_subsec_nanos(), 999_999_999);
        assert_eq!(
            timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            "1969-12-31T23:59:59.999999999Z"
        );
    }

    #[test]
    fn recording_sql_filters_observation_type_and_orders_ascending() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = compile_session_recording_sql("sess-1", from, to, 50).expect("sql");
        assert!(sql.contains("session_id = "));
        assert!(sql.contains("sess-1"));
        assert!(sql.contains("recording"));
        assert!(sql.contains("ORDER BY timestamp ASC, span_id ASC"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("events"));
    }

    #[test]
    fn recording_sql_rejects_inverted_range() {
        let from = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(compile_session_recording_sql("sess-1", from, to, 10).is_err());
    }

    #[test]
    fn span_query_predicates_use_timestamp_ns() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00.987654321Z")
            .unwrap()
            .with_timezone(&Utc);
        let assert_ns = |sql: String| {
            assert!(sql.contains("::TIMESTAMP_NS"), "{sql}");
            assert!(!sql.contains("::TIMESTAMPTZ"), "{sql}");
        };

        assert_ns(compile_session_recording_sql("sess-1", from, to, 10).unwrap());
        assert_ns(compile_session_detail_sql("sess-1", from, to).unwrap());
        assert_ns(compile_span_detail_sql("span-1", from, to).unwrap());
        assert_ns(compile_trace_summary_sql("trace-1", from, to, None).unwrap());
        assert_ns(compile_trace_spans_sql("trace-1", from, to, 10, None, None).unwrap());

        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        assert_sql_has_otlp_time_predicates(&compile_span_detail_sql("span-1", from, to).unwrap());
        assert_sql_has_otlp_time_predicates(
            &compile_trace_summary_sql("trace-1", from, to, None).unwrap(),
        );
        assert_sql_has_otlp_time_predicates(
            &compile_trace_spans_sql("trace-1", from, to, 10, None, None).unwrap(),
        );
        assert_sql_has_otlp_time_predicates(
            &compile_scores_for_trace_sql("trace-1", from, to).unwrap(),
        );

        let request = SpanSearchRequest {
            from,
            to,
            span_types: vec![],
            model_name: None,
            user_id: None,
            session_id: Some("sess-1".to_string()),
            trace_id: None,
            limit: Some(10),
            cursor: None,
        };
        assert_ns(compile_span_search_sql(&request).unwrap());

        let mut session_request = session_search_request();
        session_request.from = from;
        session_request.to = to;
        assert_ns(compile_session_search_sql(&session_request, 10).unwrap());
    }

    #[test]
    fn session_detail_pad_zero_sql_literals_match_window() {
        // Pad 0: summary (t0,t1) → SQL literals equal (t0,t1) for detail compilers.
        let t0 = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t1 = DateTime::parse_from_rfc3339("2026-03-01T12:05:00Z")
            .unwrap()
            .with_timezone(&Utc);
        for sql in [
            compile_session_detail_sql("sess-1", t0, t1).expect("session detail"),
            compile_session_recording_sql("sess-1", t0, t1, 10).expect("rec"),
        ] {
            assert!(sql.contains("'2026-03-01T12:00:00"), "{sql}");
            assert!(sql.contains("'2026-03-01T12:05:00"), "{sql}");
            assert!(!sql.contains("2026-02-28"), "{sql}");
            assert!(!sql.contains("2026-03-02"), "{sql}");
        }
    }

    #[test]
    fn holistic_session_sql_returns_full_span_payload_and_session_totals() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = crate::sql::llm::compile_session_detail_sql("sess-1", from, to)
            .expect("session detail SQL");

        assert!(
            sql.contains("AS MATERIALIZED"),
            "shared session scan: {sql}"
        );
        assert!(
            sql.contains("COUNT(DISTINCT trace_id)"),
            "trace total: {sql}"
        );
        assert!(sql.contains("COUNT(*)"), "span total: {sql}");
        assert!(
            sql.contains("AS session_span_count"),
            "span total name: {sql}"
        );
        assert!(sql.contains("attributes"), "full span payload: {sql}");
        assert!(sql.contains("events"), "full span events: {sql}");
        assert!(
            !sql.contains("LIMIT "),
            "session response is complete: {sql}"
        );
        assert!(!sql.contains("next_cursor"));
    }

    #[test]
    fn session_detail_includes_span_attributes_and_events() {
        // Explorer ProductSessionDetailView builds trajectory from sp.input /
        // sp.output — payload must be present in the holistic session response.
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let list = compile_session_detail_sql("sess-1", from, to).unwrap();
        let payload = crate::storage::schema::variant::variant_as_json("attributes");
        let events = crate::storage::schema::variant::variant_as_json("events");
        assert!(
            list.contains(&payload),
            "session detail must project span attributes: {list}"
        );
        assert!(
            list.contains(&events),
            "session detail must project events as JSON: {list}"
        );
        assert!(
            !list.contains(", events"),
            "session detail must not return nested events through Arrow: {list}"
        );
        let detail = compile_span_detail_sql("span-1", from, to).unwrap();
        assert!(detail.contains(&payload), "detail keeps payload: {detail}");
        assert!(
            detail.contains(&events),
            "detail keeps payload as JSON: {detail}"
        );
    }

    #[test]
    fn span_detail_serializes_empty_attributes_and_events() {
        let skinny = SpanDetail {
            summary: SpanSummary {
                span_id: "sp-1".into(),
                trace_id: "tr-1".into(),
                parent_span_id: None,
                session_id: Some("ses-1".into()),
                name: "chat".into(),
                span_type: "span".into(),
                start_time: Utc::now(),
                end_time: None,
                status_code: None,
                model_name: None,
                model_provider: None,
                user_id: None,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                total_cost: None,
            },
            attributes: HashMap::new(),
            events: Vec::new(),
            scores: Vec::new(),
        };
        let json = serde_json::to_value(&skinny).expect("serialize");
        assert_eq!(json.get("attributes"), Some(&json!({})));
        assert_eq!(json.get("events"), Some(&json!([])));
        let fat = SpanDetail {
            attributes: HashMap::from([("k".into(), "v".into())]),
            events: vec![serde_json::json!({"name": "x"})],
            ..skinny.clone()
        };
        let fat_json = serde_json::to_value(&fat).expect("serialize fat");
        assert_eq!(fat_json["attributes"]["k"], "v");
        assert!(fat_json["events"].is_array());
    }

    #[test]
    fn one_day_session_fetch_predicates_do_not_name_unrelated_days() {
        // One-day window → timestamp bound only (no day/DATE column predicates).
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        let from = DateTime::parse_from_rfc3339("2026-09-10T16:05:15Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-09-10T16:45:48Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = compile_session_detail_sql("sess-1", from, to).unwrap();
        assert_sql_has_otlp_time_predicates(&sql);
        assert!(
            sql.contains("timestamp") && !sql.contains("record_date"),
            "{sql}"
        );
        assert!(!sql.contains("2026-09-09"), "{sql}");
        assert!(!sql.contains("2026-09-11"), "{sql}");
    }

    #[test]
    fn session_llm_sql_excludes_recording_observation_type() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let detail = compile_session_detail_sql("sess-1", from, to).expect("session detail");
        assert!(detail.contains("<> 'recording'"));
    }

    #[test]
    fn extract_recording_events_parses_json_string_payload() {
        let events_json =
            r#"[{"type":4,"timestamp":100},{"type":2,"timestamp":200,"isCompressed":true}]"#;
        let span_events = vec![serde_json::json!({
            "name": "sp.recording.batch",
            "timestamp": "2026-07-18T00:00:01.000Z",
            "attributes": {
                "sp.recording.events": events_json
            }
        })];
        let events = extract_recording_events(&span_events);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], 4);
        assert_eq!(events[1]["isCompressed"], true);
    }

    #[test]
    fn map_events_parses_json_projection_text() {
        let projected = serde_json::json!(
            "[{\"name\":\"sp.recording.batch\",\"timestamp\":\"2026-07-18T00:00:01Z\",\"attributes\":{\"sp.recording.events\":\"[]\"}}]"
        );
        let events = map_events(Some(&projected));
        assert_eq!(events.len(), 1, "projected event JSON was dropped");
        assert_eq!(events[0]["name"], "sp.recording.batch");
    }

    #[test]
    fn extract_recording_events_ignores_other_event_names() {
        let span_events = vec![serde_json::json!({
            "name": "gen_ai.content.prompt",
            "attributes": { "content": "hi" }
        })];
        assert!(extract_recording_events(&span_events).is_empty());
    }

    #[test]
    fn all_llm_lake_compilers_emit_otlp_day_and_timestamp() {
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Single inventory: every pub lake compile_* must appear here (AC2).
        const LAKE_COMPILE_FNS: &[&str] = &[
            "compile_session_recording_sql",
            "compile_session_search_sql",
            "compile_span_search_sql",
            "compile_span_detail_sql",
            "compile_trace_summary_sql",
            "compile_trace_spans_sql",
            "compile_session_detail_sql",
            "compile_session_detail_sql_without_events",
            "compile_scores_for_span_sql",
            "compile_scores_for_trace_sql",
        ];
        let src = include_str!("../../sql/llm/mod.rs");
        for name in LAKE_COMPILE_FNS {
            assert!(
                src.contains(&format!("pub fn {name}")),
                "inventory stale: missing pub fn {name}"
            );
            // No Option<DateTime> time args on lake scanners (signature line + following lines).
            let start = src.find(&format!("pub fn {name}")).expect(name);
            let sig_end = src[start..].find('{').expect("fn body") + start;
            let sig = &src[start..sig_end];
            assert!(
                !sig.contains("Option<DateTime"),
                "{name} must not take Option<DateTime> for lake scans: {sig}"
            );
        }
        // No extra pub compile_* for traces/scores slipped in without joining the inventory.
        for line in src.lines() {
            let trimmed = line.trim_start();
            if let Some(rest) = trimmed.strip_prefix("pub fn compile_") {
                let name_end = rest.find('(').unwrap_or(rest.len());
                let short = &rest[..name_end];
                let full = format!("compile_{short}");
                if full.contains("session_summary") {
                    continue;
                }
                assert!(
                    LAKE_COMPILE_FNS.contains(&full.as_str())
                        || full == "compile_session_summary_upsert_sql",
                    "new pub fn {full} must be added to LAKE_COMPILE_FNS inventory test"
                );
            }
        }

        let sqls = vec![
            compile_session_recording_sql("s", from, to, 10).unwrap(),
            compile_session_search_sql(&session_search_request(), 10).unwrap(),
            compile_span_search_sql(&SpanSearchRequest {
                from,
                to,
                span_types: vec![],
                model_name: None,
                user_id: None,
                session_id: None,
                trace_id: None,
                limit: Some(10),
                cursor: None,
            })
            .unwrap(),
            compile_span_detail_sql("span", from, to).unwrap(),
            compile_trace_summary_sql("tr", from, to, None).unwrap(),
            compile_trace_spans_sql("tr", from, to, 10, None, None).unwrap(),
            compile_session_detail_sql("s", from, to).unwrap(),
            crate::sql::llm::compile_session_detail_sql_without_events("s", from, to).unwrap(),
            compile_scores_for_span_sql("span", from, to).unwrap(),
            compile_scores_for_trace_sql("tr", from, to).unwrap(),
        ];
        assert_eq!(sqls.len(), LAKE_COMPILE_FNS.len());
        for sql in &sqls {
            assert_sql_has_otlp_time_predicates(sql);
        }

        // Nested scores: outer scores scan AND traces subquery each need day bounds.
        let trace_scores = compile_scores_for_trace_sql("tr", from, to).unwrap();
        assert!(
            trace_scores.matches("timestamp").count() >= 2,
            "scores-for-trace needs outer + subquery day bounds: {trace_scores}"
        );
    }
}
