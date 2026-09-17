//! Ingest → session_stats_delta → sessions/search merge-on-read.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, AppState};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn trace_batch(
    session_id: &str,
    spans: Vec<(Vec<u8>, Vec<u8>, i64, Option<&str>, Vec<KeyValue>)>,
) -> ExportTraceServiceRequest {
    let otlp_spans = spans
        .into_iter()
        .map(|(trace_id, span_id, start_ns, status, mut attrs)| {
            attrs.push(string_kv("sp.session.id", session_id));
            Span {
                trace_id,
                span_id,
                parent_span_id: vec![],
                name: "gen".to_string(),
                kind: span::SpanKind::Internal as i32,
                start_time_unix_nano: start_ns as u64,
                end_time_unix_nano: (start_ns + 1_000_000_000) as u64,
                attributes: attrs,
                status: status.map(|code| Status {
                    code: if code == "ERROR" { 2 } else { 1 },
                    message: String::new(),
                }),
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "session-stats-delta-test")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.test".to_string(),
                    ..Default::default()
                }),
                spans: otlp_spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn response_json(resp: axum::response::Response) -> Value {
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

async fn test_router() -> (Router, AppState, TempDir) {
    let temp = TempDir::new().expect("tempdir");
    let (router, state) = create_router(
        Arc::new(file_backed_test_config(&temp)),
        post(ingest_traces),
        None,
    )
    .await
    .expect("router");
    (router, state, temp)
}

async fn post_traces(router: &Router, body: ExportTraceServiceRequest) {
    let mut buf = Vec::new();
    body.encode(&mut buf).expect("encode");
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(buf))
                .unwrap(),
        )
        .await
        .expect("traces");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn multi_batch_ingest_merges_on_sessions_search() {
    let (router, state, _temp) = test_router().await;
    let session_id = "sess-delta-merge-1";
    // 2024-07-19T12:00:00Z
    let t0 = 1_721_392_800_000_000_000i64;

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![(
                vec![0x01; 16],
                vec![0xa1; 8],
                t0,
                Some("ERROR"),
                vec![
                    string_kv("total_tokens", "10"),
                    string_kv("total_cost", "0.1"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![
                (
                    vec![0x01; 16],
                    vec![0xa2; 8],
                    t0 + 60_000_000_000,
                    None,
                    vec![
                        string_kv("total_tokens", "20"),
                        string_kv("total_cost", "0.2"),
                        string_kv("sp.observation.type", "generation"),
                    ],
                ),
                (
                    vec![0x02; 16],
                    vec![0xa3; 8],
                    t0 + 120_000_000_000,
                    Some("ERROR"),
                    vec![string_kv("sp.observation.type", "tool")],
                ),
            ],
        ),
    )
    .await;

    let engine = state.engine_for_id("").await.expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");

    let delta_sql = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "sql": format!(
                    "SELECT COUNT(*)::BIGINT, SUM(observation_count)::BIGINT, SUM(error_count)::BIGINT, SUM(total_tokens)::BIGINT \
                     FROM session_stats_delta WHERE session_id = '{session_id}'"
                )
            })
            .to_string(),
        ))
        .unwrap();
    let delta_resp = router.clone().oneshot(delta_sql).await.expect("delta sql");
    let delta_status = delta_resp.status();
    let delta = response_json(delta_resp).await;
    assert_eq!(
        delta_status,
        StatusCode::OK,
        "delta sql failed: {delta}"
    );
    assert_eq!(delta["rows"][0][0], 2, "two delta rows (one per batch): {delta}");
    assert_eq!(delta["rows"][0][1], 3, "obs sum: {delta}");
    assert_eq!(delta["rows"][0][2], 2, "error sum: {delta}");
    assert_eq!(delta["rows"][0][3], 30, "token sum: {delta}");

    let search = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "roots_only": false,
                "limit": 20
            })
            .to_string(),
        ))
        .unwrap();
    let search_resp = router.oneshot(search).await.expect("sessions search");
    assert_eq!(search_resp.status(), StatusCode::OK);
    let body = response_json(search_resp).await;
    let items = body["items"].as_array().expect("items");
    let row = items
        .iter()
        .find(|r| r["session_id"] == session_id)
        .unwrap_or_else(|| panic!("session missing: {body}"));
    assert_eq!(row["observation_count"], 3);
    assert_eq!(row["error_count"], 2);
    assert_eq!(row["total_tokens"], 30);
    assert!((row["total_cost"].as_f64().unwrap() - 0.3).abs() < 1e-9);
}
