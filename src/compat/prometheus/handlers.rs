//! Prometheus HTTP API route handlers.

use super::encode::{
    encode_eval_result, label_values_data, labels_data, metadata_data, series_data,
    success_response_limited,
};
use super::params::{
    pairs_from_form, pairs_from_query, parse_discovery_params, parse_metadata_params,
    parse_query_params, DiscoveryParams,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::compat::backends::metrics::{MetricsDiscoveryRequest, MetricsQueryBackend};
use crate::compat::backends::DuckLakeMetricsBackend;
use crate::compat::envelopes::error_response;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::promql::{eval_instant, eval_range, parse_promql};
use crate::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};
use crate::compat::ttl_lru::TtlLruCache;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const PROTO: ProtocolScope = ProtocolScope::Prometheus;

/// Short-lived PromQL `query_range` result cache for Grafana refresh storms
/// (findings §3 / short TTL). Keys include start/end/step so live dashboards
/// with a moving `end` rarely reuse a stale window; TTL stays short so fixed
/// ranges do not serve multi-minute stale JSON after late ingest.
/// Capacity must exceed the dashboard cell count (~1472) plus live refreshes —
/// never wipe the whole map on overflow. Byte budget caps RSS (responses may
/// approach the 16 MiB capability limit).
const RANGE_CACHE_TTL: Duration = Duration::from_secs(60);
const RANGE_CACHE_MAX: usize = 8192;
/// ~64 MiB serialized JSON budget across all cached range answers.
const RANGE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

fn range_result_cache() -> &'static Mutex<TtlLruCache<String, Value>> {
    static CACHE: OnceLock<Mutex<TtlLruCache<String, Value>>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(TtlLruCache::with_byte_budget(
            RANGE_CACHE_TTL,
            RANGE_CACHE_MAX,
            Some(RANGE_CACHE_MAX_BYTES),
        ))
    })
}

fn range_cache_get(key: &String) -> Option<Value> {
    let mut guard = range_result_cache().lock().ok()?;
    guard.get(key, Instant::now())
}

fn range_cache_put(key: String, data: Value) {
    let Ok(mut guard) = range_result_cache().lock() else {
        return;
    };
    let bytes = serde_json::to_vec(&data).map(|v| v.len()).unwrap_or(0);
    guard.put_sized(key, data, bytes, Instant::now());
}

fn tenant_ctx(tenant: TenantInfo) -> Result<TenantContext, CompatError> {
    TenantContext::from_authenticated(tenant, PROTO, None, QueryLimits::default())
}

async fn backend_for(
    state: &AppState,
    ctx: &TenantContext,
) -> Result<DuckLakeMetricsBackend, CompatError> {
    let engine = state.engine_for_tenant(&ctx.tenant).await.map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("tenant engine unavailable: {e}"),
        )
    })?;
    Ok(DuckLakeMetricsBackend::new(Arc::clone(&engine.query)))
}

fn collect_pairs(
    method: &Method,
    uri_query: Option<&str>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Vec<(String, String)>, CompatError> {
    let mut pairs = Vec::new();
    if let Some(q) = uri_query {
        pairs.extend(pairs_from_query(q));
    }
    if *method == Method::POST {
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if ct.starts_with("application/x-www-form-urlencoded") {
            let text = std::str::from_utf8(body).map_err(|_| {
                CompatError::new(
                    CompatErrorCode::BadRequest,
                    "POST form body is not valid UTF-8",
                )
            })?;
            pairs.extend(pairs_from_form(text));
        }
    }
    Ok(pairs)
}

fn map_err(err: CompatError) -> Response {
    error_response(PROTO, err)
}

fn respond_data(ctx: &TenantContext, data: Value) -> Response {
    match success_response_limited(data, ctx.limits.max_response_bytes) {
        Ok(resp) => resp,
        Err(e) => map_err(e),
    }
}

/// Shared auth → pairs → backend setup for discovery/query handlers.
async fn prepare(
    state: &AppState,
    tenant: TenantInfo,
    method: &Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<(TenantContext, Vec<(String, String)>, DuckLakeMetricsBackend), CompatError> {
    let ctx = tenant_ctx(tenant)?;
    let pairs = collect_pairs(method, uri.query(), headers, body)?;
    let backend = backend_for(state, &ctx).await?;
    Ok((ctx, pairs, backend))
}

fn discovery_request(params: DiscoveryParams) -> MetricsDiscoveryRequest {
    MetricsDiscoveryRequest {
        start_ms: params.start_ms,
        end_ms: params.end_ms,
        matchers: params.matchers,
    }
}

async fn labels_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend.label_names(&ctx, &discovery_request(params)).await {
        Ok(names) => respond_data(&ctx, labels_data(&names)),
        Err(e) => map_err(e),
    }
}

async fn label_values_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(name): Path<String>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend
        .label_values(&ctx, &name, &discovery_request(params))
        .await
    {
        Ok(values) => respond_data(&ctx, label_values_data(&values)),
        Err(e) => map_err(e),
    }
}

async fn series_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend.series(&ctx, &discovery_request(params)).await {
        Ok(series) => respond_data(&ctx, series_data(&series)),
        Err(e) => map_err(e),
    }
}

async fn metadata_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let ctx = match tenant_ctx(tenant) {
        Ok(c) => c,
        Err(e) => return map_err(e),
    };
    let pairs: Vec<(String, String)> = query.into_iter().collect();
    let params = match parse_metadata_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(b) => b,
        Err(e) => return map_err(e),
    };
    match backend
        .metadata(
            &ctx,
            params.metric.as_deref(),
            params.limit,
            params.start_ms,
            params.end_ms,
        )
        .await
    {
        Ok(items) => respond_data(&ctx, metadata_data(&items)),
        Err(e) => map_err(e),
    }
}

async fn query_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_query_params(&pairs, &ctx.limits, false) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let expr = match parse_promql(&params.query) {
        Ok(e) => e,
        Err(e) => return map_err(e),
    };
    let eval_ms = params
        .time_ms
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    match eval_instant(&backend, &ctx, &expr, eval_ms).await {
        Ok(result) => respond_data(&ctx, encode_eval_result(result)),
        Err(e) => map_err(e),
    }
}

async fn query_range_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Cache lookup before acquiring a tenant DuckDB engine — Greptime-style
    // range-result cache must not pay ingest/query pool contention on hits.
    let ctx = match tenant_ctx(tenant) {
        Ok(c) => c,
        Err(e) => return map_err(e),
    };
    let pairs = match collect_pairs(&method, uri.query(), &headers, &body) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let params = match parse_query_params(&pairs, &ctx.limits, true) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let start_ms = params.start_ms.unwrap();
    let end_ms = params.end_ms.unwrap();
    let step_ms = params.step_ms.unwrap();
    let cache_key = format!(
        "{}|{}|{}|{}|{}",
        ctx.tenant.tenant_id, params.query, start_ms, end_ms, step_ms
    );
    if let Some(data) = range_cache_get(&cache_key) {
        return respond_data(&ctx, data);
    }
    let backend = match backend_for(&state, &ctx).await {
        Ok(b) => b,
        Err(e) => return map_err(e),
    };
    let expr = match parse_promql(&params.query) {
        Ok(e) => e,
        Err(e) => return map_err(e),
    };
    match eval_range(&backend, &ctx, &expr, start_ms, end_ms, step_ms).await {
        Ok(result) => {
            let data = encode_eval_result(result);
            range_cache_put(cache_key, data.clone());
            respond_data(&ctx, data)
        }
        Err(e) => map_err(e),
    }
}

async fn rules_handler() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"success","data":{"groups":[]}}"#,
    )
        .into_response()
}

async fn query_exemplars_handler() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"success","data":[]}"#,
    )
        .into_response()
}

async fn buildinfo_handler() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"success","data":{"version":"2.54.1","revision":"softprobe","branch":"main","buildUser":"softprobe","buildDate":"2026-09-04","goVersion":"go1.22.5"}}"#,
    )
        .into_response()
}

pub fn prometheus_routes() -> axum::Router<AppState> {
    use axum::routing::get;
    axum::Router::new()
        .route("/api/v1/query", get(query_handler).post(query_handler))
        .route(
            "/api/v1/query_range",
            get(query_range_handler).post(query_range_handler),
        )
        .route("/api/v1/labels", get(labels_handler).post(labels_handler))
        .route(
            "/api/v1/label/{name}/values",
            get(label_values_handler).post(label_values_handler),
        )
        .route("/api/v1/series", get(series_handler).post(series_handler))
        .route("/api/v1/metadata", get(metadata_handler))
        .route("/api/v1/rules", get(rules_handler))
        .route(
            "/api/v1/query_exemplars",
            get(query_exemplars_handler).post(query_exemplars_handler),
        )
        .route("/api/v1/status/buildinfo", get(buildinfo_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn range_cache_constants_cover_dashboard_cell_count() {
        // SLO measures ~1472 expr×range cells; capacity must leave headroom for
        // live Grafana refreshes without a nuclear clear.
        const {
            assert!(RANGE_CACHE_MAX >= 8192);
            assert!(RANGE_CACHE_TTL.as_secs() >= 30);
            assert!(RANGE_CACHE_TTL.as_secs() <= 120);
            assert!(RANGE_CACHE_MAX_BYTES >= 16 * 1024 * 1024);
        }
    }

    #[test]
    fn range_cache_put_get_round_trip() {
        let mut cache = TtlLruCache::with_byte_budget(RANGE_CACHE_TTL, 16, Some(1024 * 1024));
        let now = Instant::now();
        let key = "tenant|sum(x)|1|2|3".to_string();
        let data = json!({"status": "success"});
        let bytes = serde_json::to_vec(&data).unwrap().len();
        cache.put_sized(key.clone(), data, bytes, now);
        assert_eq!(
            cache.get(&key, now).and_then(|v| v.get("status").cloned()),
            Some(json!("success"))
        );
    }
}
