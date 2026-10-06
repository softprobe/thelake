// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

use crate::api::{map_execute_result, AppState};
use crate::authn::TenantInfo;
use crate::runtime_engine::RuntimeEngine;
use crate::sql::telemetry::{
    details_logs, details_spans, field_spec, field_values as field_values_sql,
    query_window_from_time_range, search as search_sql, SEARCH_FIELDS,
};
use crate::sql::QueryWindow;
use crate::storage::schema::attribute_map::parse_projected_json_value;
use axum::extract::Extension;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

// Re-export compile types under the HTTP module path for OpenAPI/caller stability.
pub use crate::sql::telemetry::{
    compile_details_sql, compile_search_sql, CompiledDetailsSql, TelemetryDetailsTarget,
    TelemetryFilter, TelemetryFilterExpr, TelemetrySearchRequest, TelemetrySearchScope,
    TelemetrySort, TelemetrySortDirection, TelemetryTimeRange,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryDetailsRequest {
    pub version: u32,
    pub target: TelemetryDetailsTarget,
    #[serde(default)]
    pub time_range: Option<TelemetryTimeRange>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn search(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<TelemetrySearchRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let trusted = search_sql(&request).map_err(bad_request)?;
    let engine = resolve_engine(&state, tenant.as_ref().map(|e| &e.0)).await?;
    let result =
        map_execute_result(engine.execute_trusted(trusted).await).map_err(storage_error)?;
    let rows = rows_to_search_response(&request.scope, &result.columns, &result.rows);

    Ok(Json(json!({
        "version": 1,
        "scope": match request.scope {
            TelemetrySearchScope::Sessions => "sessions",
            TelemetrySearchScope::Traces => "traces",
        },
        "columns": selected_columns(&request.columns),
        "rows": rows,
        "nextCursor": Value::Null,
        "query": { "compiled": false }
    })))
}

pub async fn session_details(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    details_for_target(
        state,
        tenant.as_ref().map(|e| &e.0),
        TelemetryDetailsTarget {
            kind: "session".to_string(),
            id: session_id,
        },
        time_range_from_query(&params),
        1000,
    )
    .await
}

pub async fn trace_details(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(trace_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    details_for_target(
        state,
        tenant.as_ref().map(|e| &e.0),
        TelemetryDetailsTarget {
            kind: "trace".to_string(),
            id: trace_id,
        },
        time_range_from_query(&params),
        1000,
    )
    .await
}

pub async fn details_post(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<TelemetryDetailsRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if request.version != 1 {
        return Err(bad_request(
            "unsupported telemetry details version".to_string(),
        ));
    }
    details_for_target(
        state,
        tenant.as_ref().map(|e| &e.0),
        request.target,
        request.time_range,
        request.limit.unwrap_or(1000),
    )
    .await
}

pub async fn fields() -> Json<Value> {
    Json(json!({
        "version": 1,
        "fields": SEARCH_FIELDS.iter().map(|field| json!({
            "key": field.key,
            "type": field.value_type,
            "entity": field.entity,
            "filterable": field.filterable,
            "sortable": field.sortable,
            "projectable": field.projectable,
            "operators": field.ops,
        })).collect::<Vec<_>>()
    }))
}

pub async fn field_values(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(field): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let spec = field_spec(&field).ok_or_else(|| bad_request("unknown field".to_string()))?;
    if !spec.filterable {
        return Err(bad_request("field is not filterable".to_string()));
    }
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500)
        .clamp(1, 10_000);
    let window = parse_field_values_window(&params).map_err(bad_request)?;
    let trusted = field_values_sql(spec.sql, &window, limit).map_err(bad_request)?;
    let engine = resolve_engine(&state, tenant.as_ref().map(|e| &e.0)).await?;
    let result =
        map_execute_result(engine.execute_trusted(trusted).await).map_err(storage_error)?;
    let values = result
        .rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    Ok(Json(
        json!({ "version": 1, "field": field, "values": values }),
    ))
}

/// Require `from`/`to` query params for field_values (AC2).
fn parse_field_values_window(params: &HashMap<String, String>) -> Result<QueryWindow, String> {
    let from = params
        .get("from")
        .ok_or_else(|| "`from` is required".to_string())?;
    let to = params
        .get("to")
        .ok_or_else(|| "`to` is required".to_string())?;
    query_window_from_time_range(&TelemetryTimeRange {
        from: from.clone(),
        to: to.clone(),
    })
    .map_err(|e| {
        e.replace("timeRange.from", "from")
            .replace("timeRange.to", "to")
    })
}

async fn details_for_target(
    state: AppState,
    tenant: Option<&TenantInfo>,
    target: TelemetryDetailsTarget,
    time_range: Option<TelemetryTimeRange>,
    limit: usize,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let time_range = time_range.ok_or_else(|| bad_request("timeRange is required".to_string()))?;
    let engine = resolve_engine(&state, tenant).await?;
    let spans_sql = details_spans(&target, &time_range, limit).map_err(bad_request)?;
    let logs_sql = details_logs(&target, &time_range, limit).map_err(bad_request)?;
    let spans_result =
        map_execute_result(engine.execute_trusted(spans_sql).await).map_err(storage_error)?;
    let logs_result =
        map_execute_result(engine.execute_trusted(logs_sql).await).map_err(storage_error)?;
    let spans = rows_to_objects(&spans_result.columns, &spans_result.rows);
    let logs = rows_to_objects(&logs_result.columns, &logs_result.rows);
    let summary = json!({
        "spanCount": spans.len(),
        "logCount": logs.len(),
        "traceCount": distinct_count(&spans, "trace_id"),
        "errorCount": spans.iter().filter(|row| is_error_span(row)).count(),
        "services": distinct_strings(&spans, "app_id"),
    });

    Ok(Json(json!({
        "version": 1,
        "kind": target.kind,
        "id": target.id,
        "timeRange": time_range,
        "summary": summary,
        "spans": spans,
        "logs": logs,
    })))
}

async fn resolve_engine(
    state: &AppState,
    tenant: Option<&TenantInfo>,
) -> Result<Arc<RuntimeEngine>, (StatusCode, Json<Value>)> {
    match tenant {
        Some(info) => state.engine_for_tenant(info).await,
        None => state.engine_for_id("").await,
    }
    .map_err(storage_error)
}

fn time_range_from_query(params: &HashMap<String, String>) -> Option<TelemetryTimeRange> {
    Some(TelemetryTimeRange {
        from: params.get("from")?.clone(),
        to: params.get("to")?.clone(),
    })
}

fn rows_to_objects(columns: &[String], rows: &[Vec<Value>]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut object = Map::new();
            for (idx, column) in columns.iter().enumerate() {
                let raw = row.get(idx).cloned().unwrap_or(Value::Null);
                // Attribute-map projections use CAST(... AS JSON); DuckDB returns text —
                // parse it so clients keep object-valued attributes/resource_attributes.
                let value = match column.as_str() {
                    "attributes" | "resource_attributes" => parse_projected_json_value(raw),
                    _ => raw,
                };
                object.insert(column.clone(), value);
            }
            Value::Object(object)
        })
        .collect()
}

fn rows_to_search_response(
    scope: &TelemetrySearchScope,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Vec<Value> {
    rows_to_objects(columns, rows)
        .into_iter()
        .map(|row| {
            let id = row.get("id").cloned().unwrap_or(Value::Null);
            let services = row
                .get("services")
                .and_then(Value::as_str)
                .map(|s| {
                    s.split(',')
                        .filter(|part| !part.is_empty())
                        .map(|part| Value::String(part.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let summary = match scope {
                TelemetrySearchScope::Sessions => json!({
                    "sessionId": row.get("session_id").cloned().unwrap_or(Value::Null),
                    "traceCount": numeric_cell(row.get("trace_count")),
                    "spanCount": numeric_cell(row.get("span_count")),
                    "logCount": 0,
                                        "errorCount": numeric_cell(row.get("error_count")),
                    "durationMs": numeric_cell(row.get("duration_ms")),
                    "services": services,
                    "entryPath": row.get("entry_path").cloned().unwrap_or(Value::Null),
                    "lastError": row.get("last_error").cloned().unwrap_or(Value::Null),
                }),
                TelemetrySearchScope::Traces => json!({
                    "sessionId": row.get("session_id").cloned().unwrap_or(Value::Null),
                    "traceId": row.get("trace_id").cloned().unwrap_or(Value::Null),
                    "spanCount": numeric_cell(row.get("span_count")),
                    "errorCount": numeric_cell(row.get("error_count")),
                    "durationMs": numeric_cell(row.get("duration_ms")),
                    "services": services,
                    "name": row.get("name").cloned().unwrap_or(Value::Null),
                    "entryPath": row.get("entry_path").cloned().unwrap_or(Value::Null),
                    "lastError": row.get("last_error").cloned().unwrap_or(Value::Null),
                }),
            };
            json!({
                "id": id,
                "kind": match scope {
                    TelemetrySearchScope::Sessions => "session",
                    TelemetrySearchScope::Traces => "trace",
                },
                "timeRange": {
                    "from": row.get("start_time").cloned().unwrap_or(Value::Null),
                    "to": row.get("end_time").cloned().unwrap_or(Value::Null),
                },
                "summary": summary,
                "cells": row,
            })
        })
        .collect()
}

fn numeric_cell(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Number(_)) => value.cloned().unwrap(),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<f64>().map(Value::from))
            .unwrap_or(Value::Null),
        _ => json!(0),
    }
}

fn selected_columns(columns: &[String]) -> Vec<Value> {
    let keys = if columns.is_empty() {
        vec![
            "session_id".to_string(),
            "trace_count".to_string(),
            "span_count".to_string(),
        ]
    } else {
        columns.to_vec()
    };
    keys.into_iter()
        .map(|key| {
            json!({
                "key": key,
                "type": field_spec(&key).map(|field| field.value_type).unwrap_or("dynamic"),
                "label": key,
            })
        })
        .collect()
}

fn distinct_count(rows: &[Value], key: &str) -> usize {
    distinct_strings(rows, key).len()
}

fn distinct_strings(rows: &[Value], key: &str) -> Vec<String> {
    let mut values = rows
        .iter()
        .filter_map(|row| row.get(key).and_then(Value::as_str))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn is_error_span(row: &Value) -> bool {
    row.get("status_code").and_then(Value::as_str) == Some("ERROR")
        || row
            .get("http_response_status_code")
            .and_then(Value::as_i64)
            .is_some_and(|status| status >= 500)
}

fn bad_request(message: String) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": { "code": "bad_request", "message": message } })),
    )
}

fn storage_error(err: anyhow::Error) -> (StatusCode, Json<Value>) {
    warn!("telemetry query failed: {}", err);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": { "code": "storage_error", "message": err.to_string() } })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn field_values_without_window_rejected() {
        let empty = HashMap::new();
        let err = parse_field_values_window(&empty).unwrap_err();
        assert!(err.contains("`from` is required"));

        let mut only_from = HashMap::new();
        only_from.insert("from".into(), "2023-11-14T22:13:20Z".into());
        let err = parse_field_values_window(&only_from).unwrap_err();
        assert!(err.contains("`to` is required"));
    }
}
