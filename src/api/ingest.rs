use crate::api::AppState;
use crate::authn::WorkspaceAuth;
use crate::models::{Log as LogData, Span as SpanData};
use anyhow::Result;
use axum::extract::{Extension, State};
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub success: bool,
    pub ingested_count: usize,
    pub message: String,
}

/// Durable write failed after DuckLake's own conflict retries — ask exporters to retry.
pub(crate) fn ingest_write_failed(message: String) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(IngestResponse {
            success: false,
            ingested_count: 0,
            message,
        }),
    )
        .into_response()
}

/// Count a failed OTLP decode toward `thelake_ingest_errors_total` (customer tenants only).
pub(crate) fn record_ingest_decode_failure(
    tenant: Option<&WorkspaceAuth>,
    signal: &str,
    start: Instant,
) {
    let Some(t) = tenant else {
        return;
    };
    if crate::self_monitoring::instrument_customer_workspace(&t.workspace_id) {
        crate::self_monitoring::record_ingest(
            &t.workspace_id,
            signal,
            false,
            None,
            start.elapsed(),
        );
    }
}

/// Unified OTLP /v1/traces handler that switches on Content-Type
pub async fn ingest_traces(
    State(state): State<AppState>,
    tenant: Option<Extension<WorkspaceAuth>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let start = Instant::now();
    let body_size = body.len();
    let tenant_info = tenant.map(|t| t.0);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => {
                match process_traces_with_capture(state, request, body_size, tenant_info.clone())
                    .await
                {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} spans", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP traces: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(e) => {
                record_ingest_decode_failure(tenant_info.as_ref(), "traces", start);
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Protobuf decode failed").into_response()
            }
        }
    } else {
        match decode_otlp_traces_json(&body) {
            Ok(request) => {
                match process_traces_with_capture(state, request, body_size, tenant_info.clone())
                    .await
                {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} spans", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP traces: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(_) => {
                record_ingest_decode_failure(tenant_info.as_ref(), "traces", start);
                (StatusCode::BAD_REQUEST, "Invalid JSON").into_response()
            }
        }
    }
}

/// Unified OTLP /v1/logs handler that switches on Content-Type
pub async fn ingest_logs(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let start = Instant::now();
    let body_size = body.len();
    let workspace_auth = auth.map(|t| t.0);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => {
                match process_logs(state, request, body_size, workspace_auth.clone()).await {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} log records", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP logs: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(e) => {
                record_ingest_decode_failure(workspace_auth.as_ref(), "logs", start);
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Protobuf decode failed").into_response()
            }
        }
    } else {
        match decode_otlp_logs_json(&body) {
            Ok(request) => {
                match process_logs(state, request, body_size, workspace_auth.clone()).await {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} log records", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP logs: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(e) => {
                record_ingest_decode_failure(workspace_auth.as_ref(), "logs", start);
                error!("Failed to decode JSON: {}", e);
                (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response()
            }
        }
    }
}

/// Core OTLP processing logic (shared by HTTP and gRPC ingest).
pub async fn process_traces(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
    auth_tenant: Option<WorkspaceAuth>,
) -> Result<usize> {
    process_traces_with_capture(state, request, body_size, auth_tenant).await
}

async fn process_traces_with_capture(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
    auth_tenant: Option<WorkspaceAuth>,
) -> Result<usize> {
    let start = std::time::Instant::now();
    let tid_hint = auth_tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let result = process_traces_inner(state, request, body_size, auth_tenant).await;
    if crate::self_monitoring::instrument_customer_workspace(&tid_hint) {
        let (ok, app) = match &result {
            Ok((_, app)) => (true, app.clone()),
            Err(_) => (false, None),
        };
        crate::self_monitoring::record_ingest(
            &tid_hint,
            "traces",
            ok,
            app.as_deref(),
            start.elapsed(),
        );
        return result.map(|(n, _)| n);
    }
    result.map(|(n, _)| n)
}

async fn process_traces_inner(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
    auth_tenant: Option<WorkspaceAuth>,
) -> Result<(usize, Option<String>)> {
    let mut spans = Vec::new();
    let mut app: Option<String> = None;

    for resource_spans in request.resource_spans {
        let resource_attributes = SpanData::extract_resource_attributes(&resource_spans);
        if app.is_none() {
            app = resource_attributes.get("service.name").cloned();
        }

        for scope_spans in resource_spans.scope_spans {
            let instrumentation_scope = scope_spans
                .scope
                .as_ref()
                .map(crate::models::span::encode_instrumentation_scope);
            for span in scope_spans.spans {
                let links = crate::models::span::encode_links(&span.links);
                let mut span_data = SpanData::from_otlp(span, &resource_attributes)?;
                span_data.attributes.retain(|key, _| {
                    !key.starts_with(crate::models::span::RESERVED_ATTRIBUTE_PREFIX)
                });
                if let Some(scope) = &instrumentation_scope {
                    span_data.attributes.insert(
                        crate::models::span::INSTRUMENTATION_SCOPE_ATTRIBUTE.into(),
                        scope.clone(),
                    );
                }
                if links != "[]" {
                    span_data
                        .attributes
                        .insert(crate::models::span::LINKS_ATTRIBUTE.into(), links);
                }
                spans.push(span_data);
            }
        }
    }

    let tid = auth_tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let agent_id = auth_tenant.as_ref().and_then(|t| t.agent_id.clone());
    let agent_name = auth_tenant.as_ref().and_then(|t| t.agent_name.clone());

    for span in &mut spans {
        span.workspace_id = Some(tid.clone());
        span.agent_id = agent_id.clone();
        if let Some(ref stamped_agent_name) = agent_name {
            span.agent_name = Some(stamped_agent_name.clone());
        }
    }

    crate::capture_correlation::validate_capture_batch(&spans, &tid, agent_id.as_deref())?;
    let uses_capture_correlation =
        crate::capture_correlation::uses_capture_correlation(&spans, agent_id.as_deref());

    let span_count = spans.len();

    let ws = state.workspace_for_id(&tid).await?;
    let write_start = std::time::Instant::now();
    if uses_capture_correlation {
        let agent_id = agent_id.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Agent telemetry correlation requires a trusted Agent")
        })?;
        ws.ingest_capture_spans(spans, body_size, agent_id).await?;
    } else {
        ws.ingest().add_spans(spans, body_size).await?;
    }
    if crate::self_monitoring::instrument_customer_workspace(&tid) {
        crate::self_monitoring::record_write(&tid, "traces", app.as_deref(), write_start.elapsed());
    }

    info!(
        "Processed {} spans from OTLP request ({} bytes)",
        span_count, body_size
    );
    Ok((span_count, app))
}

async fn process_logs(
    state: AppState,
    request: ExportLogsServiceRequest,
    body_size: usize,
    tenant: Option<WorkspaceAuth>,
) -> Result<usize> {
    let start = std::time::Instant::now();
    let tid = tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let result = process_logs_inner(state, request, body_size, tenant).await;
    if crate::self_monitoring::instrument_customer_workspace(&tid) {
        let (ok, app) = match &result {
            Ok((_, app)) => (true, app.clone()),
            Err(_) => (false, None),
        };
        crate::self_monitoring::record_ingest(&tid, "logs", ok, app.as_deref(), start.elapsed());
        return result.map(|(n, _)| n);
    }
    result.map(|(n, _)| n)
}

async fn process_logs_inner(
    state: AppState,
    request: ExportLogsServiceRequest,
    body_size: usize,
    tenant: Option<WorkspaceAuth>,
) -> Result<(usize, Option<String>)> {
    let mut logs = Vec::new();
    let mut app: Option<String> = None;

    for resource_logs in request.resource_logs {
        let resource_attributes = LogData::extract_resource_attributes(&resource_logs);
        if app.is_none() {
            app = resource_attributes.get("service.name").cloned();
        }

        for scope_logs in resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|s| s.name.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            for log_record in scope_logs.log_records {
                let mut log_data = LogData::from_otlp(log_record, &resource_attributes)?;
                LogData::promote_scope_logger_name(&mut log_data.attributes, scope_name.as_deref());
                logs.push(log_data);
            }
        }
    }

    let log_count = logs.len();

    let workspace_id = tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let agent_id = tenant.as_ref().and_then(|t| t.agent_id.clone());
    let agent_name = tenant.as_ref().and_then(|t| t.agent_name.clone());
    for log in &mut logs {
        log.agent_id = agent_id.clone();
        log.agent_name = agent_name.clone();
    }
    let ws = state.workspace_for_id(&workspace_id).await?;
    let write_start = std::time::Instant::now();
    ws.ingest().add_logs(logs, body_size).await?;
    if crate::self_monitoring::instrument_customer_workspace(&workspace_id) {
        crate::self_monitoring::record_write(
            &workspace_id,
            "logs",
            app.as_deref(),
            write_start.elapsed(),
        );
    }

    info!(
        "Processed {} log records from OTLP request ({} bytes)",
        log_count, body_size
    );
    Ok((log_count, app))
}

/// `opentelemetry-proto` 0.7 uses `deserialize_from_value` on `KeyValue.value` and
/// `LogRecord.body`, which expects unwrapped OTLP JSON variants and a string `intValue`.
/// `ArrayValue.values` uses derived `Deserialize` on `AnyValue`, which expects a `"value"`
/// wrapper and an integer `intValue`.
#[derive(Clone, Copy)]
enum ProtoJsonAnyValueEncoding {
    CustomDeserializer,
    DerivedMessage,
}

fn decode_otlp_traces_json(body: &[u8]) -> serde_json::Result<ExportTraceServiceRequest> {
    let mut root: serde_json::Value = serde_json::from_slice(body)?;
    normalize_otlp_traces_json(&mut root);
    serde_json::from_value(root)
}

fn decode_otlp_logs_json(body: &[u8]) -> serde_json::Result<ExportLogsServiceRequest> {
    let mut root: serde_json::Value = serde_json::from_slice(body)?;
    normalize_otlp_logs_json(&mut root);
    serde_json::from_value(root)
}

fn normalize_otlp_traces_json(root: &mut serde_json::Value) {
    let Some(resource_spans) = root
        .as_object_mut()
        .and_then(|o| o.get_mut("resourceSpans"))
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };
    for rs in resource_spans {
        let Some(rs_obj) = rs.as_object_mut() else {
            continue;
        };
        if let Some(res_attrs) = rs_obj
            .get_mut("resource")
            .and_then(|r| r.as_object_mut())
            .and_then(|r| r.get_mut("attributes"))
        {
            normalize_otlp_key_values(res_attrs);
        }
        if let Some(scope_spans) = rs_obj.get_mut("scopeSpans").and_then(|v| v.as_array_mut()) {
            for ss in scope_spans {
                let Some(ss_obj) = ss.as_object_mut() else {
                    continue;
                };
                if let Some(scope_attrs) = ss_obj
                    .get_mut("scope")
                    .and_then(|s| s.as_object_mut())
                    .and_then(|s| s.get_mut("attributes"))
                {
                    normalize_otlp_key_values(scope_attrs);
                }
                if let Some(spans) = ss_obj.get_mut("spans").and_then(|v| v.as_array_mut()) {
                    for span in spans {
                        let Some(span_obj) = span.as_object_mut() else {
                            continue;
                        };
                        if let Some(attrs) = span_obj.get_mut("attributes") {
                            normalize_otlp_key_values(attrs);
                        }
                        if let Some(events) =
                            span_obj.get_mut("events").and_then(|v| v.as_array_mut())
                        {
                            for ev in events {
                                if let Some(attrs) =
                                    ev.as_object_mut().and_then(|o| o.get_mut("attributes"))
                                {
                                    normalize_otlp_key_values(attrs);
                                }
                            }
                        }
                        if let Some(links) =
                            span_obj.get_mut("links").and_then(|v| v.as_array_mut())
                        {
                            for link in links {
                                if let Some(attrs) =
                                    link.as_object_mut().and_then(|o| o.get_mut("attributes"))
                                {
                                    normalize_otlp_key_values(attrs);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn normalize_otlp_logs_json(root: &mut serde_json::Value) {
    let Some(resource_logs) = root
        .as_object_mut()
        .and_then(|o| o.get_mut("resourceLogs"))
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };
    for rl in resource_logs {
        let Some(rl_obj) = rl.as_object_mut() else {
            continue;
        };
        if let Some(res_attrs) = rl_obj
            .get_mut("resource")
            .and_then(|r| r.as_object_mut())
            .and_then(|r| r.get_mut("attributes"))
        {
            normalize_otlp_key_values(res_attrs);
        }
        if let Some(scope_logs) = rl_obj.get_mut("scopeLogs").and_then(|v| v.as_array_mut()) {
            for sl in scope_logs {
                let Some(sl_obj) = sl.as_object_mut() else {
                    continue;
                };
                if let Some(scope_attrs) = sl_obj
                    .get_mut("scope")
                    .and_then(|s| s.as_object_mut())
                    .and_then(|s| s.get_mut("attributes"))
                {
                    normalize_otlp_key_values(scope_attrs);
                }
                if let Some(records) = sl_obj.get_mut("logRecords").and_then(|v| v.as_array_mut()) {
                    for lr in records {
                        let Some(lr_obj) = lr.as_object_mut() else {
                            continue;
                        };
                        if let Some(body) = lr_obj.get_mut("body") {
                            normalize_otlp_any_value(
                                body,
                                ProtoJsonAnyValueEncoding::CustomDeserializer,
                            );
                        }
                        if let Some(attrs) = lr_obj.get_mut("attributes") {
                            normalize_otlp_key_values(attrs);
                        }
                    }
                }
            }
        }
    }
}

fn normalize_otlp_key_values(attrs: &mut serde_json::Value) {
    let Some(items) = attrs.as_array_mut() else {
        return;
    };
    for item in items {
        if let Some(val) = item.as_object_mut().and_then(|o| o.get_mut("value")) {
            normalize_otlp_any_value(val, ProtoJsonAnyValueEncoding::CustomDeserializer);
        }
    }
}

fn normalize_otlp_any_value(val: &mut serde_json::Value, encoding: ProtoJsonAnyValueEncoding) {
    match val {
        serde_json::Value::Null => {
            if matches!(encoding, ProtoJsonAnyValueEncoding::DerivedMessage) {
                *val = serde_json::json!({ "value": null });
            }
        }
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                if matches!(encoding, ProtoJsonAnyValueEncoding::DerivedMessage) {
                    *val = serde_json::json!({ "value": null });
                }
                return;
            }
            if matches!(encoding, ProtoJsonAnyValueEncoding::DerivedMessage)
                && map.len() == 1
                && map.contains_key("value")
            {
                if let Some(inner) = map.get_mut("value") {
                    if !inner.is_null() {
                        normalize_otlp_any_value_variant(
                            inner,
                            ProtoJsonAnyValueEncoding::DerivedMessage,
                        );
                    }
                }
                return;
            }
            normalize_otlp_any_value_variant(val, encoding);
            if matches!(encoding, ProtoJsonAnyValueEncoding::DerivedMessage) {
                let inner = std::mem::replace(val, serde_json::Value::Null);
                *val = serde_json::json!({ "value": inner });
            }
        }
        _ => {}
    }
}

fn normalize_otlp_any_value_variant(
    val: &mut serde_json::Value,
    encoding: ProtoJsonAnyValueEncoding,
) {
    let Some(map) = val.as_object_mut() else {
        return;
    };
    if let Some(int_val) = map.get_mut("intValue") {
        match encoding {
            ProtoJsonAnyValueEncoding::CustomDeserializer => {
                if let Some(n) = int_val.as_i64() {
                    *int_val = serde_json::Value::String(n.to_string());
                } else if let Some(u) = int_val.as_u64() {
                    *int_val = serde_json::Value::String(u.to_string());
                }
            }
            ProtoJsonAnyValueEncoding::DerivedMessage => {
                if let Some(s) = int_val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        *int_val = serde_json::Value::Number(n.into());
                    }
                }
            }
        }
    }
    if let Some(items) = map
        .get_mut("arrayValue")
        .and_then(|a| a.as_object_mut())
        .and_then(|a| a.get_mut("values"))
        .and_then(|v| v.as_array_mut())
    {
        for item in items {
            normalize_otlp_any_value(item, ProtoJsonAnyValueEncoding::DerivedMessage);
        }
    }
    if let Some(kv_values) = map
        .get_mut("kvlistValue")
        .and_then(|kv| kv.as_object_mut())
        .and_then(|kv| kv.get_mut("values"))
    {
        normalize_otlp_key_values(kv_values);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::any_value::key_values_to_map;

    #[test]
    fn decode_otlp_traces_json_preserves_nested_arrays_kvlists_and_int_values() {
        let payload = serde_json::json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "svc"}},
                        {"key": "args", "value": {"arrayValue": {"values": [{"stringValue": "a"}, {}]}}}
                    ]
                },
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "0102030405060708090a0b0c0d0e0f10",
                        "spanId": "0102030405060708",
                        "name": "chat",
                        "startTimeUnixNano": "1000",
                        "endTimeUnixNano": "2000",
                        "attributes": [
                            {"key": "gen_ai.usage.input_tokens", "value": {"intValue": 42}},
                            {
                                "key": "gen_ai.response.finish_reasons",
                                "value": {"arrayValue": {"values": [{"stringValue": "tool_calls"}]}}
                            },
                            {
                                "key": "counts",
                                "value": {"arrayValue": {"values": [{"intValue": "7"}, {"intValue": 8}]}}
                            }
                        ]
                    }]
                }]
            }]
        });

        let decoded = decode_otlp_traces_json(payload.to_string().as_bytes()).expect("decode json");
        let rs = &decoded.resource_spans[0];
        let res_map = key_values_to_map(&rs.resource.as_ref().unwrap().attributes);
        assert_eq!(
            res_map.get("args").map(String::as_str),
            Some("sp.json:[\"a\",null]")
        );

        let span = &rs.scope_spans[0].spans[0];
        let attr_map = key_values_to_map(&span.attributes);
        assert_eq!(
            attr_map
                .get("gen_ai.usage.input_tokens")
                .map(String::as_str),
            Some("42")
        );
        assert_eq!(
            attr_map
                .get("gen_ai.response.finish_reasons")
                .map(String::as_str),
            Some("sp.json:[\"tool_calls\"]")
        );
        assert_eq!(
            attr_map.get("counts").map(String::as_str),
            Some("sp.json:[7,8]")
        );
    }

    #[test]
    fn decode_otlp_logs_json_preserves_structured_body_and_attributes() {
        let payload = serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "1000",
                        "observedTimeUnixNano": "1000",
                        "body": {
                            "arrayValue": {
                                "values": [{"stringValue": "line1"}, {"intValue": "9"}]
                            }
                        },
                        "attributes": [
                            {"key": "tags", "value": {"arrayValue": {"values": [{"stringValue": "llm"}]}}}
                        ]
                    }]
                }]
            }]
        });

        let decoded = decode_otlp_logs_json(payload.to_string().as_bytes()).expect("decode logs");
        let lr = &decoded.resource_logs[0].scope_logs[0].log_records[0];
        let body_json = crate::models::any_value::any_value_to_json(lr.body.as_ref().unwrap());
        assert_eq!(body_json, Some(serde_json::json!(["line1", 9])));
        let attr_map = key_values_to_map(&lr.attributes);
        assert_eq!(
            attr_map.get("tags").map(String::as_str),
            Some("sp.json:[\"llm\"]")
        );
    }
}
