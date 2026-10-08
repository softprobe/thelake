use crate::api::error::{bad_request, not_found, storage_error, ApiError};
use crate::api::mapping::{
    column_value, optional_f64, optional_i64, parse_timestamp_text, resolve_workspace,
};
use crate::api::traces::map_span_detail;
use crate::api::{map_execute_result, AppState};
use crate::async_jobs::LeaseStore;
use crate::authn::WorkspaceAuth;
use crate::models::Score;
use crate::sql::llm::{clamp_limit, session_detail, session_recording, DEFAULT_SESSION_LIMIT};
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use tracing::warn;

pub use crate::session_summary::list_query::{
    SessionOrderBy, SessionSearchRequest, SessionSearchResponse, SessionSummary, SortDirection,
};
pub use crate::sql::llm::search::SessionDetail;

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

/// Resolve the lake window from the Postgres session summary. Missing row → 404.
async fn resolve_session_lake_window(
    state: &AppState,
    tenant_ref: Option<&WorkspaceAuth>,
    session_id: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ApiError> {
    let workspace_id = tenant_ref.map(|t| t.workspace_id.as_str()).unwrap_or("");
    let ws = state
        .workspaces
        .workspace_for(workspace_id)
        .await
        .map_err(storage_error)?;
    match ws.lookup_session_summary_window(session_id).await {
        Ok(Some(window)) => Ok(window),
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
    tenant: Option<Extension<WorkspaceAuth>>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("missing session_id"));
    }

    if let (Some(from), Some(to)) = (params.get("from"), params.get("to")) {
        let target = crate::sql::telemetry::TelemetryDetailsTarget {
            kind: "session".to_string(),
            id: session_id,
        };
        let time_range = Some(crate::sql::telemetry::TelemetryTimeRange {
            from: from.clone(),
            to: to.clone(),
        });
        let res = crate::api::mapping::details_for_target(
            state,
            tenant.as_ref().map(|e| &e.0),
            target,
            time_range,
            1000,
        )
        .await?;
        return Ok(Json(res));
    }

    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let tenant_label = tenant_ref.map(|t| t.workspace_id.as_str()).unwrap_or("");
    let total_start = std::time::Instant::now();

    let pg_start = std::time::Instant::now();
    let window = resolve_session_lake_window(&state, tenant_ref, &session_id).await;
    let pg_elapsed = pg_start.elapsed();
    crate::self_monitoring::record_session_detail_stage(
        tenant_label,
        crate::self_monitoring::session_detail_stage::PG_WINDOW,
        pg_elapsed,
    );
    let (from, to) = window?;

    let detail_sql = session_detail(&session_id, from, to).map_err(bad_request)?;
    let lake_start = std::time::Instant::now();
    let ws = resolve_workspace(&state, tenant_ref).await?;
    let lake = map_execute_result(ws.query().execute_trusted(detail_sql).await);
    let lake_elapsed = lake_start.elapsed();
    crate::self_monitoring::record_session_detail_stage(
        tenant_label,
        crate::self_monitoring::session_detail_stage::LAKE_SQL,
        lake_elapsed,
    );
    let detail_result = lake.map_err(storage_error)?;

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

    let total_elapsed = total_start.elapsed();
    crate::self_monitoring::record_session_detail_stage(
        tenant_label,
        crate::self_monitoring::session_detail_stage::TOTAL,
        total_elapsed,
    );
    if total_elapsed >= std::time::Duration::from_millis(500) {
        tracing::warn!(
            session_id = %session_id,
            pg_window_ms = pg_elapsed.as_millis() as u64,
            lake_sql_ms = lake_elapsed.as_millis() as u64,
            total_ms = total_elapsed.as_millis() as u64,
            span_rows = detail_result.rows.len(),
            span_count = aggregate.span_count,
            "slow session detail"
        );
    }

    Ok(Json(
        serde_json::to_value(SessionDetail {
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
            spans,
            scores,
        })
        .unwrap(),
    ))
}

pub async fn session_details_post(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<crate::api::mapping::EntityDetailsRequest>,
) -> Result<Json<Value>, ApiError> {
    let session_id = request
        .session_id
        .or_else(|| request.target.as_ref().map(|t| t.id.clone()))
        .ok_or_else(|| bad_request("missing session_id"))?;
    let target = crate::sql::telemetry::TelemetryDetailsTarget {
        kind: "session".to_string(),
        id: session_id,
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

/// Fetch web session recording batches for a session (`sp.observation.type=recording`).
pub async fn get_session_recording(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Path(session_id): Path<String>,
    Query(params): Query<RecordingQuery>,
) -> Result<Json<SessionRecording>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("missing session_id".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let (from, to) = resolve_session_lake_window(&state, tenant_ref, &session_id).await?;
    let limit = clamp_limit(params.limit, DEFAULT_RECORDING_LIMIT);
    let sql = session_recording(&session_id, from, to, limit).map_err(bad_request)?;
    let ws = resolve_workspace(&state, tenant_ref).await?;
    let result =
        map_execute_result(ws.query().execute_trusted(sql).await).map_err(storage_error)?;

    let mut batches = result
        .rows
        .iter()
        .filter_map(|row| map_recording_batch(&result.columns, row))
        .collect::<Vec<_>>();
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

/// Session list backed by Postgres `session_summary`.
pub async fn search_sessions(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<SessionSearchRequest>,
) -> Result<Json<SessionSearchResponse>, ApiError> {
    let limit = clamp_limit(request.limit, DEFAULT_SESSION_LIMIT);
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);

    let workspace_id = tenant_ref.map(|t| t.workspace_id.as_str()).unwrap_or("");
    let ws = state
        .workspaces
        .workspace_for(workspace_id)
        .await
        .map_err(storage_error)?;
    match ws.search_session_summary(&request, limit).await {
        Ok(response) => Ok(Json(response)),
        Err(crate::session_summary::SessionSummaryListError::BadRequest(msg)) => {
            Err(bad_request(msg))
        }
        Err(crate::session_summary::SessionSummaryListError::Storage(err)) => {
            Err(storage_error(err))
        }
    }
}

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
    tenant: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<SessionSummaryRebuildRequest>,
) -> Result<Json<SessionSummaryRebuildResponse>, ApiError> {
    let cfg = &state.workspaces.config().session_summary;

    crate::session_summary::validate_rebuild_window(
        request.from,
        request.to,
        cfg.max_reduce_span_seconds,
    )
    .map_err(bad_request)?;

    let workspace_id = tenant
        .as_ref()
        .map(|extension| extension.0.workspace_id.as_str())
        .unwrap_or("");
    let scope_key = crate::workspace_scope::effective_workspace_id(workspace_id);
    let leases = crate::async_jobs::PostgresLeaseStore::from_workspaces(&state.workspaces);
    let holder = session_summary_rebuild_holder_id(
        &state.workspaces.config().async_jobs.resolved_instance_id(),
    );
    let ttl = std::time::Duration::from_secs(
        state
            .workspaces
            .config()
            .async_jobs
            .lease_ttl_seconds
            .max(1),
    );
    let maintenance = state
        .workspaces
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
    let heartbeat_every = std::time::Duration::from_secs(
        state
            .workspaces
            .config()
            .async_jobs
            .heartbeat_seconds
            .max(1),
    );
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

struct SessionAggregate {
    trace_count: i64,
    span_count: i64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    total_tokens: Option<i64>,
    total_cost: Option<f64>,
    user_ids: Vec<String>,
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
