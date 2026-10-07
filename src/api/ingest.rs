use crate::api::AppState;
use crate::authn::TenantInfo;
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
    tenant: Option<&TenantInfo>,
    signal: &str,
    start: Instant,
) {
    let Some(t) = tenant else {
        return;
    };
    if crate::self_monitoring::instrument_customer_tenant(&t.workspace_id) {
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
    tenant: Option<Extension<TenantInfo>>,
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
                match process_traces(state, request, body_size, tenant_info.clone()).await {
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
        match serde_json::from_slice::<ExportTraceServiceRequest>(&body) {
            Ok(request) => {
                match process_traces(state, request, body_size, tenant_info.clone()).await {
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
    tenant: Option<Extension<TenantInfo>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let start = Instant::now();
    let body_size = body.len();
    let runtime_engine = tenant.map(|t| t.0);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => {
                match process_logs(state, request, body_size, runtime_engine.clone()).await {
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
                record_ingest_decode_failure(runtime_engine.as_ref(), "logs", start);
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Protobuf decode failed").into_response()
            }
        }
    } else {
        match serde_json::from_slice::<ExportLogsServiceRequest>(&body) {
            Ok(request) => {
                match process_logs(state, request, body_size, runtime_engine.clone()).await {
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
                record_ingest_decode_failure(runtime_engine.as_ref(), "logs", start);
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
    auth_tenant: Option<TenantInfo>,
) -> Result<usize> {
    let start = std::time::Instant::now();
    let tid_hint = auth_tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let result = process_traces_inner(state, request, body_size, auth_tenant).await;
    if crate::self_monitoring::instrument_customer_tenant(&tid_hint) {
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
    auth_tenant: Option<TenantInfo>,
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
        span.agent_name = agent_name.clone();
    }

    let span_count = spans.len();

    let engine = state.engine_for_id(&tid).await?;
    let write_start = std::time::Instant::now();
    engine.add_spans(spans, body_size).await?;
    if crate::self_monitoring::instrument_customer_tenant(&tid) {
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
    tenant: Option<TenantInfo>,
) -> Result<usize> {
    let start = std::time::Instant::now();
    let tid = tenant
        .as_ref()
        .map(|t| t.workspace_id.clone())
        .unwrap_or_default();
    let result = process_logs_inner(state, request, body_size, tenant).await;
    if crate::self_monitoring::instrument_customer_tenant(&tid) {
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
    tenant: Option<TenantInfo>,
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
    let engine = state.engine_for_id(&workspace_id).await?;
    let write_start = std::time::Instant::now();
    engine.add_logs(logs, body_size).await?;
    if crate::self_monitoring::instrument_customer_tenant(&workspace_id) {
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
