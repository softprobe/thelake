//! Shared HTTP helpers for Loki/Tempo compatibility contract tests.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::AppState;
use softprobe_runtime::runtime_api::runtime_control_routes;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

pub async fn build_tenant_router_with_state() -> (Router, AppState, TempDir) {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let (router, state) = softprobe_runtime::api::create_router(
        std::sync::Arc::new(config),
        post(ingest_traces),
        None,
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn(inject_local_sqlite_tenant));
    (router, state, temp)
}

pub async fn get_json(router: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    get_json_as(router, path, None).await
}

pub async fn get_json_as(
    router: &Router,
    path: &str,
    tenant_id: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    request_json(router, "GET", path, Body::empty(), tenant_id, None, None).await
}

/// GET with `Authorization: Bearer …` (Grafana datasource style).
pub async fn get_json_bearer(
    router: &Router,
    path: &str,
    bearer: &str,
) -> (StatusCode, serde_json::Value) {
    request_json(router, "GET", path, Body::empty(), None, Some(bearer), None).await
}

async fn request_json(
    router: &Router,
    method: &str,
    path: &str,
    body: Body,
    tenant_id: Option<&str>,
    bearer: Option<&str>,
    content_type: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    if let Some(tid) = tenant_id {
        builder = builder.header("x-test-tenant-id", tid);
    }
    if let Some(token) = bearer {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    let resp = router
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Encode `application/x-www-form-urlencoded` query components (`+` for space).
pub fn encode_query_pairs(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Encode owned key/value pairs the same way as [`encode_query_pairs`].
pub fn encode_query_owned(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
