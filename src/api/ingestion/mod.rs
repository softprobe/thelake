pub mod logs;
pub mod metrics;
pub mod traces;

use crate::authn::TenantInfo;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::Instant;

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
    if crate::self_monitoring::instrument_customer_tenant(&t.tenant_id) {
        crate::self_monitoring::record_ingest(
            &t.tenant_id,
            signal,
            false,
            None,
            start.elapsed(),
        );
    }
}
