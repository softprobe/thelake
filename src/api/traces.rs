use crate::api::error::{bad_request, not_found, storage_error, ApiError};
use crate::api::mapping::{
    column_value, map_events, map_string_map, optional_f64, optional_i64, optional_string,
    optional_timestamp, required_string, required_timestamp, resolve_workspace,
};
use crate::api::{map_execute_result, AppState};
use crate::authn::WorkspaceAuth;
use crate::models::{Score, ScoreDataType, ScoreSource};
use crate::sql::llm::{
    clamp_limit, scores_for_span, scores_for_trace, search_spans as compile_search_spans,
    span_detail, trace_spans, trace_summary, DEFAULT_SEARCH_LIMIT, DEFAULT_TRACE_LIMIT,
};
use crate::sql::paging::encode_cursor;
use crate::workspace::WorkspaceContext;
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

pub use crate::sql::llm::search::{
    SpanDetail, SpanSearchRequest, SpanSearchResponse, SpanSummary, Trace, TraceDetail,
};

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
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<SpanSearchRequest>,
) -> Result<Json<SpanSearchResponse>, ApiError> {
    let sql = compile_search_spans(&request).map_err(bad_request)?;
    let auth_ref = tenant.as_ref().map(|extension| &extension.0);
    let ws = resolve_workspace(&state, auth_ref).await?;
    let result =
        map_execute_result(ws.query().execute_trusted(sql).await).map_err(storage_error)?;

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
    tenant: Option<Extension<WorkspaceAuth>>,
    Path(span_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<SpanDetail>, ApiError> {
    if span_id.trim().is_empty() {
        return Err(bad_request("missing span_id".to_string()));
    }
    let sql = span_detail(&span_id, params.from, params.to).map_err(bad_request)?;
    let auth_ref = tenant.as_ref().map(|extension| &extension.0);
    let ws = resolve_workspace(&state, auth_ref).await?;
    let result =
        map_execute_result(ws.query().execute_trusted(sql).await).map_err(storage_error)?;
    let row = result.rows.first().ok_or_else(not_found)?;
    let mut detail = map_span_detail(&result.columns, row).ok_or_else(not_found)?;
    detail.scores = query_scores(
        &ws,
        scores_for_span(&span_id, params.from, params.to).map_err(bad_request)?,
    )
    .await?;
    Ok(Json(detail))
}

pub async fn get_trace(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Path(trace_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<TraceDetail>, ApiError> {
    if trace_id.trim().is_empty() {
        return Err(bad_request("missing trace_id".to_string()));
    }
    let auth_ref = tenant.as_ref().map(|extension| &extension.0);
    let ws = resolve_workspace(&state, auth_ref).await?;
    let summary_sql = trace_summary(
        &trace_id,
        params.from,
        params.to,
        params.session_id.as_deref(),
    )
    .map_err(bad_request)?;
    let summary_result =
        map_execute_result(ws.query().execute_trusted(summary_sql).await).map_err(storage_error)?;
    let summary_row = summary_result.rows.first().ok_or_else(not_found)?;
    let trace = map_trace(&summary_result.columns, summary_row).ok_or_else(not_found)?;

    let limit = clamp_limit(params.limit, DEFAULT_TRACE_LIMIT);
    let spans_sql = trace_spans(
        &trace_id,
        params.from,
        params.to,
        limit,
        params.cursor.as_deref(),
        params.session_id.as_deref(),
    )
    .map_err(bad_request)?;
    let spans_result =
        map_execute_result(ws.query().execute_trusted(spans_sql).await).map_err(storage_error)?;
    let mut spans = spans_result
        .rows
        .iter()
        .filter_map(|row| map_span_detail(&spans_result.columns, row))
        .collect::<Vec<_>>();
    let next_span_cursor = next_cursor_from_span_details(&mut spans, limit);

    let scores = query_scores(
        &ws,
        scores_for_trace(&trace_id, params.from, params.to).map_err(bad_request)?,
    )
    .await?;

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

pub(crate) async fn query_scores(
    ws: &WorkspaceContext,
    sql: crate::sql::trusted::TrustedSql,
) -> Result<Vec<Score>, ApiError> {
    let result =
        map_execute_result(ws.query().execute_trusted(sql).await).map_err(storage_error)?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| map_score(&result.columns, row))
        .collect())
}

pub(crate) fn map_span_summary(columns: &[String], row: &[Value]) -> Option<SpanSummary> {
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
        agent_name: optional_string(columns, row, "agent_name"),
        user_id: optional_string(columns, row, "user_id"),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
    })
}

pub(crate) fn map_span_detail(columns: &[String], row: &[Value]) -> Option<SpanDetail> {
    let summary = map_span_summary(columns, row)?;
    Some(SpanDetail {
        summary,
        attributes: map_string_map(column_value(columns, row, "attributes")),
        events: map_events(column_value(columns, row, "events")),
        scores: Vec::new(),
    })
}

pub(crate) fn map_trace(columns: &[String], row: &[Value]) -> Option<Trace> {
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

pub(crate) fn map_score(columns: &[String], row: &[Value]) -> Option<Score> {
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
        workspace_id: optional_string(columns, row, "workspace_id"),
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

pub async fn search_traces(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<crate::sql::telemetry::TelemetrySearchRequest>,
) -> Result<Json<Value>, ApiError> {
    let res = crate::api::mapping::execute_dynamic_search(
        &state,
        tenant.as_ref().map(|e| &e.0),
        &request,
    )
    .await?;
    Ok(Json(res))
}

pub async fn trace_details_post(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<crate::api::mapping::EntityDetailsRequest>,
) -> Result<Json<Value>, ApiError> {
    let trace_id = request
        .trace_id
        .or_else(|| request.target.as_ref().map(|t| t.id.clone()))
        .ok_or_else(|| bad_request("missing trace_id"))?;
    let target = crate::sql::telemetry::TelemetryDetailsTarget {
        kind: "trace".to_string(),
        id: trace_id,
    };
    let res = crate::api::mapping::details_for_target(
        state,
        tenant.as_ref().map(|e| &e.0),
        target,
        request.time_range,
        request.limit.unwrap_or(1000),
    )
    .await?;
    Ok(Json(res))
}
