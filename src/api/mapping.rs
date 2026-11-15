use crate::api::error::{bad_request, storage_error, ApiError};
use crate::api::{map_execute_result, AppState};
use crate::authn::WorkspaceAuth;
use crate::sql::telemetry::{
    details_logs, details_spans, field_spec, search as search_sql, TelemetryDetailsTarget,
    TelemetrySearchRequest, TelemetrySearchScope, TelemetryTimeRange,
};
use crate::storage::schema::attribute_map::{
    attribute_map_json_to_string_map, parse_projected_json_value,
};
use crate::workspace::WorkspaceContext;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub async fn resolve_workspace(
    state: &AppState,
    auth: Option<&WorkspaceAuth>,
) -> Result<Arc<WorkspaceContext>, ApiError> {
    match auth {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(storage_error)
}

pub fn column_value<'a>(columns: &[String], row: &'a [Value], name: &str) -> Option<&'a Value> {
    let index = columns.iter().position(|column| column == name)?;
    row.get(index)
}

pub fn required_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    optional_string(columns, row, name).filter(|value| !value.is_empty())
}

pub fn optional_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

pub fn optional_i64(columns: &[String], row: &[Value], name: &str) -> Option<i64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|v| v as i64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub fn optional_f64(columns: &[String], row: &[Value], name: &str) -> Option<f64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub fn required_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    optional_timestamp(columns, row, name)
}

pub fn optional_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    let value = column_value(columns, row, name)?;
    parse_timestamp_value(value)
}

pub fn parse_timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
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

pub fn parse_timestamp_text(text: &str) -> Option<DateTime<Utc>> {
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

pub fn map_string_map(value: Option<&Value>) -> HashMap<String, String> {
    value
        .map(attribute_map_json_to_string_map)
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let cleaned = crate::models::strip_nested_json_prefix(&v)
                .map(str::to_owned)
                .unwrap_or(v);
            (k, cleaned)
        })
        .collect()
}

pub fn map_events(value: Option<&Value>) -> Vec<Value> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityDetailsRequest {
    #[serde(default = "default_version_1")]
    pub version: u32,
    pub target: Option<TelemetryDetailsTarget>,
    pub trace_id: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub time_range: Option<TelemetryTimeRange>,
    #[serde(default)]
    pub limit: Option<usize>,
}

fn default_version_1() -> u32 {
    1
}

pub async fn execute_dynamic_search(
    state: &AppState,
    auth: Option<&WorkspaceAuth>,
    request: &TelemetrySearchRequest,
) -> Result<Value, ApiError> {
    let trusted = search_sql(request).map_err(bad_request)?;
    let ws = resolve_workspace(state, auth).await?;
    let result =
        map_execute_result(ws.query().execute_trusted(trusted).await).map_err(storage_error)?;
    let limit = request.limit.unwrap_or(100).clamp(1, 1000);
    let (visible_rows, next_cursor) =
        if request.scope == TelemetrySearchScope::Traces && result.rows.len() > limit {
            let page = &result.rows[..limit];
            let cursor = page
                .last()
                .and_then(|last| {
                    let end_time = optional_timestamp(&result.columns, last, "end_time")?;
                    let trace_id = required_string(&result.columns, last, "trace_id")?;
                    Some(Value::String(crate::sql::paging::encode_cursor(
                        end_time, &trace_id,
                    )))
                })
                .unwrap_or(Value::Null);
            (page, cursor)
        } else {
            (result.rows.as_slice(), Value::Null)
        };
    let rows = rows_to_search_response(&request.scope, &result.columns, visible_rows);

    Ok(json!({
        "version": 1,
        "scope": match request.scope {
            TelemetrySearchScope::Sessions => "sessions",
            TelemetrySearchScope::Traces => "traces",
        },
        "columns": selected_columns(&request.columns),
        "rows": rows,
        "nextCursor": next_cursor,
        "query": { "compiled": false }
    }))
}

pub async fn details_for_target(
    state: AppState,
    auth: Option<&WorkspaceAuth>,
    target: TelemetryDetailsTarget,
    time_range: Option<TelemetryTimeRange>,
    limit: usize,
) -> Result<Value, ApiError> {
    let time_range = time_range.ok_or_else(|| bad_request("timeRange is required"))?;
    let ws = resolve_workspace(&state, auth).await?;
    let spans_sql = details_spans(&target, &time_range, limit).map_err(bad_request)?;
    let logs_sql = details_logs(&target, &time_range, limit).map_err(bad_request)?;
    let spans_result =
        map_execute_result(ws.query().execute_trusted(spans_sql).await).map_err(storage_error)?;
    let logs_result =
        map_execute_result(ws.query().execute_trusted(logs_sql).await).map_err(storage_error)?;
    let spans = rows_to_objects(&spans_result.columns, &spans_result.rows);
    let logs = rows_to_objects(&logs_result.columns, &logs_result.rows);
    let summary = json!({
        "spanCount": spans.len(),
        "logCount": logs.len(),
        "traceCount": distinct_count(&spans, "trace_id"),
        "errorCount": spans.iter().filter(|row| is_error_span(row)).count(),
        "services": distinct_strings(&spans, "app_id"),
    });

    Ok(json!({
        "version": 1,
        "kind": target.kind,
        "id": target.id,
        "timeRange": time_range,
        "summary": summary,
        "spans": spans,
        "logs": logs,
    }))
}

pub fn rows_to_objects(columns: &[String], rows: &[Vec<Value>]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut object = Map::new();
            for (idx, column) in columns.iter().enumerate() {
                let raw = row.get(idx).cloned().unwrap_or(Value::Null);
                let value = match column.as_str() {
                    "session_id" => match raw.as_str() {
                        Some("") => Value::Null,
                        _ => raw,
                    },
                    "attributes" | "resource_attributes" => {
                        strip_projected_attribute_map_prefixes(parse_projected_json_value(raw))
                    }
                    "start_time" | "end_time" | "timestamp" | "end_timestamp"
                    | "observed_timestamp" => parse_timestamp_value(&raw)
                        .map(|dt| {
                            Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
                        })
                        .unwrap_or(raw),
                    _ => raw,
                };
                object.insert(column.clone(), value);
            }
            Value::Object(object)
        })
        .collect()
}

fn strip_projected_attribute_map_prefixes(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    let cleaned = match v {
                        Value::String(text) => {
                            match crate::models::strip_nested_json_prefix(&text) {
                                Some(stripped) => Value::String(stripped.to_owned()),
                                None => Value::String(text),
                            }
                        }
                        other => other,
                    };
                    (k, cleaned)
                })
                .collect(),
        ),
        other => other,
    }
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
                    "sessionId": optional_non_empty_cell(row.get("session_id")),
                    "traceId": row.get("trace_id").cloned().unwrap_or(Value::Null),
                    "spanCount": numeric_cell(row.get("span_count")),
                    "errorCount": numeric_cell(row.get("error_count")),
                    "durationMs": numeric_cell(row.get("duration_ms")),
                    "services": services,
                    "name": row.get("name").cloned().unwrap_or(Value::Null),
                    "entryPath": row.get("entry_path").cloned().unwrap_or(Value::Null),
                    "httpMethod": optional_non_empty_cell(row.get("http_request_method")),
                    "httpStatusCode": nullable_numeric_cell(row.get("http_response_status_code")),
                    "agentId": optional_non_empty_cell(row.get("agent_id")),
                    "agentName": optional_non_empty_cell(row.get("agent_name")),
                    "lastError": optional_non_empty_cell(row.get("last_error")),
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

fn nullable_numeric_cell(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Number(_)) => value.cloned().unwrap(),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<f64>().map(Value::from))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn optional_non_empty_cell(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Value::Null
            } else {
                Value::String(trimmed.to_string())
            }
        }
        _ => Value::Null,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_to_objects_strips_nested_json_prefixes_on_attribute_maps() {
        let columns = vec![
            "span_id".to_string(),
            "attributes".to_string(),
            "resource_attributes".to_string(),
        ];
        let rows = vec![vec![
            Value::String("sp_1".to_string()),
            Value::String(
                r#"{"gen_ai.response.finish_reasons":"sp.json:[\"stop\"]","plain":"{\"a\":1}"}"#
                    .to_string(),
            ),
            Value::String(
                r#"{"service.name":"travel-agent","process.command_args":"sp.json:[\"node\"]"}"#
                    .to_string(),
            ),
        ]];

        let objects = rows_to_objects(&columns, &rows);
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0]["attributes"]["gen_ai.response.finish_reasons"],
            "[\"stop\"]"
        );
        assert_eq!(objects[0]["attributes"]["plain"], "{\"a\":1}");
        assert_eq!(
            objects[0]["resource_attributes"]["process.command_args"],
            "[\"node\"]"
        );
        assert_eq!(
            objects[0]["resource_attributes"]["service.name"],
            "travel-agent"
        );
    }

    #[test]
    fn trace_search_response_formats_nanosecond_timestamps_and_null_empty_session() {
        let columns = vec![
            "id".to_string(),
            "kind".to_string(),
            "session_id".to_string(),
            "trace_id".to_string(),
            "start_time".to_string(),
            "end_time".to_string(),
            "span_count".to_string(),
            "error_count".to_string(),
            "duration_ms".to_string(),
            "services".to_string(),
            "name".to_string(),
            "http_request_method".to_string(),
            "entry_path".to_string(),
            "http_response_status_code".to_string(),
            "agent_id".to_string(),
            "agent_name".to_string(),
            "last_error".to_string(),
        ];
        let rows = vec![vec![
            Value::String("tr-1".to_string()),
            Value::String("trace".to_string()),
            Value::String(String::new()),
            Value::String("tr-1".to_string()),
            Value::String("Nanosecond:1777802400000000000".to_string()),
            Value::String("Nanosecond:1777802401500000000".to_string()),
            json!(2),
            json!(1),
            json!(1500),
            Value::String("web,api".to_string()),
            Value::String("POST /api/pay".to_string()),
            Value::String("POST".to_string()),
            Value::String("/api/pay".to_string()),
            json!(502),
            Value::String("ag-1".to_string()),
            Value::String("checkout-web".to_string()),
            Value::String("upstream timeout".to_string()),
        ]];
        let mapped = rows_to_search_response(&TelemetrySearchScope::Traces, &columns, &rows);
        assert_eq!(mapped.len(), 1);
        assert!(
            mapped[0]["summary"]["sessionId"].is_null(),
            "empty session_id must map to null: {}",
            mapped[0]
        );
        assert_eq!(mapped[0]["summary"]["agentName"], "checkout-web");
        assert_eq!(mapped[0]["summary"]["agentId"], "ag-1");
        assert_eq!(mapped[0]["summary"]["httpMethod"], "POST");
        assert_eq!(mapped[0]["summary"]["httpStatusCode"], 502);
        assert_eq!(mapped[0]["timeRange"]["from"], "2026-05-03T10:00:00Z");
        assert_eq!(mapped[0]["timeRange"]["to"], "2026-05-03T10:00:01.500Z");
    }
}
