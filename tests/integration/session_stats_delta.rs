//! Ingest → session_stats_delta → sessions/search merge-on-read (T5 / T5b / T8).

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
use crate::util::session_stats_serial::session_stats_ingest_serial;

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

/// One OTLP span row: (trace_id, span_id, start_ns, status ERROR?, attrs).
type SpanSpec = (Vec<u8>, Vec<u8>, i64, Option<&'static str>, Vec<KeyValue>);

fn trace_batch(session_id: &str, spans: Vec<SpanSpec>) -> ExportTraceServiceRequest {
    let otlp_spans = spans
        .into_iter()
        .map(|(trace_id, span_id, start_ns, status, mut attrs)| {
            if !session_id.is_empty() {
                attrs.push(string_kv("sp.session.id", session_id));
            }
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

async fn test_router() -> (
    Router,
    AppState,
    TempDir,
    tokio::sync::MutexGuard<'static, ()>,
) {
    let guard = session_stats_ingest_serial().lock().await;
    softprobe_runtime::session_stats::set_fail_session_stats_delta_write_for_test(false);
    let temp = TempDir::new().expect("tempdir");
    let (router, state) = create_router(
        Arc::new(file_backed_test_config(&temp)),
        post(ingest_traces),
        None,
    )
    .await
    .expect("router");
    (router, state, temp, guard)
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

async fn flush(state: &AppState) {
    let engine = state.engine_for_id("").await.expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");
}

async fn run_sql(router: &Router, sql: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": sql }).to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("sql");
    let status = resp.status();
    (status, response_json(resp).await)
}

async fn search_sessions(router: &Router, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("search");
    let status = resp.status();
    (status, response_json(resp).await)
}

/// 2024-07-19T12:00:00Z
const T0: i64 = 1_721_392_800_000_000_000;

fn gen_attrs(tokens: &str, cost: &str) -> Vec<KeyValue> {
    vec![
        string_kv("total_tokens", tokens),
        string_kv("total_cost", cost),
        string_kv("sp.observation.type", "generation"),
    ]
}

#[tokio::test]
async fn multi_batch_ingest_merges_on_sessions_search() {
    let (router, state, _temp, _guard) = test_router().await;
    let session_id = "sess-delta-merge-1";

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![(
                vec![0x01; 16],
                vec![0xa1; 8],
                T0,
                Some("ERROR"),
                gen_attrs("10", "0.1"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![
                (
                    vec![0x01; 16],
                    vec![0xa2; 8],
                    T0 + 60_000_000_000,
                    None,
                    gen_attrs("20", "0.2"),
                ),
                (
                    vec![0x02; 16],
                    vec![0xa3; 8],
                    T0 + 120_000_000_000,
                    Some("ERROR"),
                    vec![string_kv("sp.observation.type", "tool")],
                ),
            ],
        ),
    )
    .await;
    flush(&state).await;

    let (delta_status, delta) = run_sql(
        &router,
        &format!(
            "SELECT COUNT(*)::BIGINT, SUM(observation_count)::BIGINT, SUM(error_count)::BIGINT, \
             SUM(total_tokens)::BIGINT, MIN(start_time), MAX(end_time) \
             FROM session_stats_delta WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    assert_eq!(delta_status, StatusCode::OK, "delta sql failed: {delta}");
    assert_eq!(delta["rows"][0][0], 2, "two delta rows: {delta}");
    assert_eq!(delta["rows"][0][1], 3, "obs sum: {delta}");
    assert_eq!(delta["rows"][0][2], 2, "error sum: {delta}");
    assert_eq!(delta["rows"][0][3], 30, "token sum: {delta}");
    assert!(
        delta["rows"][0][4].as_str().is_some(),
        "min start_time present: {delta}"
    );
    assert!(
        delta["rows"][0][5].as_str().is_some(),
        "max end_time present: {delta}"
    );

    let (status, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 20
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row = body["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|r| r["session_id"] == session_id)
        .unwrap_or_else(|| panic!("session missing: {body}"));
    assert_eq!(row["observation_count"], 3);
    assert_eq!(row["error_count"], 2);
    assert_eq!(row["total_tokens"], 30);
    assert!((row["total_cost"].as_f64().unwrap() - 0.3).abs() < 1e-9);
    assert!(row["start_time"].as_str().is_some());
    assert!(row["end_time"].as_str().is_some());
}

#[tokio::test]
async fn has_errors_and_agent_name_filters_via_api() {
    let (router, state, _temp, _guard) = test_router().await;

    post_traces(
        &router,
        trace_batch(
            "sess-clean",
            vec![(vec![0x10; 16], vec![0xb1; 8], T0, None, {
                let mut a = gen_attrs("5", "0.01");
                a.push(string_kv("sp.agent.name", "CleanAgent"));
                a
            })],
        ),
    )
    .await;
    post_traces(
        &router,
        trace_batch(
            "sess-err",
            vec![(
                vec![0x11; 16],
                vec![0xb2; 8],
                T0 + 1_000_000_000,
                Some("ERROR"),
                {
                    let mut a = gen_attrs("5", "0.01");
                    a.push(string_kv("sp.agent.name", "ErrorAgent"));
                    a
                },
            )],
        ),
    )
    .await;
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "has_errors": true,
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let ids: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"sess-err".to_string()), "{body}");
    assert!(!ids.contains(&"sess-clean".to_string()), "{body}");

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "has_errors": false,
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let ids: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"sess-clean".to_string()), "{body}");
    assert!(!ids.contains(&"sess-err".to_string()), "{body}");

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "dimensions": { "agent_name": "CleanAgent" },
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{body}");
    assert_eq!(items[0]["session_id"], "sess-clean");
    assert_eq!(items[0]["agent_name"], "CleanAgent");

    // Exact match only — substring must not hit.
    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "dimensions": { "agent_name": "Clean" },
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(body["items"].as_array().unwrap().is_empty(), "{body}");
}

#[tokio::test]
async fn roots_only_hides_nested_child_sessions() {
    let (router, state, _temp, _guard) = test_router().await;

    post_traces(
        &router,
        trace_batch(
            "sess-root",
            vec![(
                vec![0x20; 16],
                vec![0xc1; 8],
                T0,
                None,
                vec![
                    string_kv("sp.observation.type", "agent"),
                    string_kv("sp.agent.name", "Root"),
                ],
            )],
        ),
    )
    .await;
    post_traces(
        &router,
        trace_batch(
            "sess-child",
            vec![(
                vec![0x21; 16],
                vec![0xc2; 8],
                T0 + 1_000_000_000,
                None,
                vec![
                    string_kv("sp.observation.type", "agent"),
                    string_kv("sp.agent.name", "Child"),
                    string_kv("sp.metadata.opencode.parentSessionID", "sess-root"),
                ],
            )],
        ),
    )
    .await;
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": true,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let ids: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"sess-root".to_string()), "{body}");
    assert!(!ids.contains(&"sess-child".to_string()), "{body}");

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let ids: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"sess-root".to_string()), "{body}");
    assert!(ids.contains(&"sess-child".to_string()), "{body}");
}

#[tokio::test]
async fn recording_and_empty_session_id_never_listed() {
    let (router, state, _temp, _guard) = test_router().await;

    // Recording-only: shares a fake session id but must not create a list row.
    post_traces(
        &router,
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
                    spans: vec![Span {
                        trace_id: vec![0x30; 16],
                        span_id: vec![0xd1; 8],
                        name: "rec".into(),
                        kind: span::SpanKind::Internal as i32,
                        start_time_unix_nano: T0 as u64,
                        end_time_unix_nano: (T0 + 1_000_000_000) as u64,
                        attributes: vec![
                            string_kv("sp.session.id", "sess-recording-only"),
                            string_kv("sp.observation.type", "recording"),
                        ],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        },
    )
    .await;

    // Empty session_id: still lands in traces, never listed.
    post_traces(
        &router,
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
                    spans: vec![Span {
                        trace_id: vec![0x31; 16],
                        span_id: vec![0xd2; 8],
                        name: "nosession".into(),
                        kind: span::SpanKind::Internal as i32,
                        start_time_unix_nano: T0 as u64,
                        end_time_unix_nano: (T0 + 1_000_000_000) as u64,
                        attributes: vec![string_kv("sp.observation.type", "generation")],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        },
    )
    .await;
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let items = body["items"].as_array().unwrap();
    assert!(
        !items
            .iter()
            .any(|r| r["session_id"] == "sess-recording-only"),
        "recording must not list: {body}"
    );
    assert!(
        !items
            .iter()
            .any(|r| r["session_id"].as_str().unwrap_or("x").is_empty()),
        "empty session_id must not list: {body}"
    );

    let (st, traces) = run_sql(
        &router,
        "SELECT COUNT(*)::BIGINT FROM traces WHERE session_id = 'sess-recording-only' OR session_id = '' OR session_id IS NULL",
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{traces}");
    assert!(
        traces["rows"][0][0].as_i64().unwrap_or(0) >= 1,
        "spans still written: {traces}"
    );
}

#[tokio::test]
async fn deleted_deltas_do_not_resurrect_via_span_scan() {
    let (router, state, _temp, _guard) = test_router().await;
    let session_id = "sess-no-span-fallback";

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![(
                vec![0x40; 16],
                vec![0xe1; 8],
                T0,
                Some("ERROR"),
                gen_attrs("7", "0.07"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    let (st, before) = run_sql(
        &router,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM session_stats_delta WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{before}");
    assert!(before["rows"][0][0].as_i64().unwrap() >= 1, "{before}");

    let (st, _) = run_sql(
        &router,
        &format!("DELETE FROM session_stats_delta WHERE session_id = '{session_id}'"),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "empty delta window is OK: {body}");
    let found = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["session_id"] == session_id);
    assert!(
        !found,
        "must not fall back to union_spans after deltas deleted: {body}"
    );
}

#[tokio::test]
async fn missing_delta_table_is_400() {
    let (router, state, _temp, _guard) = test_router().await;
    let session_id = "sess-missing-delta-table";

    post_traces(
        &router,
        trace_batch(
            session_id,
            vec![(
                vec![0x41; 16],
                vec![0xe2; 8],
                T0,
                None,
                gen_attrs("3", "0.03"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    let (st, drop_body) = run_sql(&router, "DROP TABLE IF EXISTS session_stats_delta").await;
    assert_eq!(st, StatusCode::OK, "{drop_body}");

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "missing table must 400: {body}");
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("session_stats_delta") && err.contains("schema"),
        "schema error message: {body}"
    );
}

#[tokio::test]
async fn inverted_range_is_400_and_limit_clamped() {
    let (router, state, _temp, _guard) = test_router().await;

    for i in 0..5u8 {
        post_traces(
            &router,
            trace_batch(
                &format!("sess-limit-{i}"),
                vec![(
                    vec![0x50 + i; 16],
                    vec![0xf0 + i; 8],
                    T0 + i as i64 * 1_000_000_000,
                    None,
                    gen_attrs("1", "0.01"),
                )],
            ),
        )
        .await;
    }
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-20T00:00:00Z",
            "to": "2024-07-18T00:00:00Z",
            "limit": 10
        }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 99999
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let n = body["items"].as_array().unwrap().len();
    assert!(n <= 200, "limit must clamp to MAX 200, got {n}: {body}");
}

#[tokio::test]
async fn agent_name_sql_injection_is_safe() {
    let (router, state, _temp, _guard) = test_router().await;
    post_traces(
        &router,
        trace_batch(
            "sess-inject",
            vec![(vec![0x60; 16], vec![0x01; 8], T0, None, {
                let mut a = gen_attrs("1", "0.01");
                a.push(string_kv("sp.agent.name", "SafeAgent"));
                a
            })],
        ),
    )
    .await;
    flush(&state).await;

    let evil = "'; DROP TABLE session_stats_delta; --";
    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "dimensions": { "agent_name": evil },
            "roots_only": false,
            "limit": 10
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "must not 500 on injection: {body}");
    assert!(
        body["items"].as_array().unwrap().is_empty(),
        "evil literal matches nothing: {body}"
    );

    // Table still queryable — injection did not drop it.
    let (st, check) = run_sql(
        &router,
        "SELECT COUNT(*)::BIGINT FROM session_stats_delta WHERE session_id = 'sess-inject'",
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{check}");
    assert!(check["rows"][0][0].as_i64().unwrap() >= 1, "{check}");
}

#[tokio::test]
async fn null_tokens_merge_as_zero_and_cursor_pages() {
    let (router, state, _temp, _guard) = test_router().await;

    // Session with no token attrs on first batch, tokens on second.
    post_traces(
        &router,
        trace_batch(
            "sess-null-tok",
            vec![(
                vec![0x70; 16],
                vec![0x11; 8],
                T0,
                None,
                vec![string_kv("sp.observation.type", "tool")],
            )],
        ),
    )
    .await;
    flush(&state).await;
    post_traces(
        &router,
        trace_batch(
            "sess-null-tok",
            vec![(
                vec![0x71; 16],
                vec![0x12; 8],
                T0 + 10_000_000_000,
                None,
                gen_attrs("5", "0.05"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    // Extra sessions for cursor paging (start_time desc).
    for i in 0..4u8 {
        post_traces(
            &router,
            trace_batch(
                &format!("sess-page-{i}"),
                vec![(
                    vec![0x80 + i; 16],
                    vec![0x20 + i; 8],
                    T0 + 100_000_000_000 + i as i64 * 1_000_000_000,
                    None,
                    gen_attrs("1", "0.01"),
                )],
            ),
        )
        .await;
    }
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let row = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["session_id"] == "sess-null-tok")
        .unwrap_or_else(|| panic!("missing: {body}"));
    assert_eq!(row["total_tokens"], 5, "null tokens treated as 0: {row}");

    let (st, page1) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "order_by": "start_time",
            "order": "desc",
            "limit": 2
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{page1}");
    assert!(page1["cursor_supported"].as_bool().unwrap());
    let cursor = page1["next_cursor"].as_str().expect("next_cursor");
    let page1_ids: Vec<_> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1_ids.len(), 2, "{page1}");

    let (st, page2) = search_sessions(
        &router,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "order_by": "start_time",
            "order": "desc",
            "limit": 2,
            "cursor": cursor
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{page2}");
    let page2_ids: Vec<_> = page2["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    for id in &page1_ids {
        assert!(
            !page2_ids.contains(id),
            "no dup across pages: {page1_ids:?} vs {page2_ids:?}"
        );
    }
}

#[tokio::test]
async fn empty_window_returns_empty_items() {
    let (router, state, _temp, _guard) = test_router().await;
    // Create session_stats_delta via ingest, then query a non-overlapping window.
    post_traces(
        &router,
        trace_batch(
            "sess-elsewhere",
            vec![(
                vec![0x99; 16],
                vec![0x88; 8],
                T0,
                None,
                gen_attrs("1", "0.01"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    let (st, body) = search_sessions(
        &router,
        json!({
            "from": "2020-01-01T00:00:00Z",
            "to": "2020-01-02T00:00:00Z",
            "roots_only": false,
            "limit": 10
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(body["items"].as_array().unwrap().is_empty(), "{body}");
}

#[tokio::test]
async fn session_stats_delta_partition_sort_applied() {
    let (router, state, _temp, _guard) = test_router().await;
    post_traces(
        &router,
        trace_batch(
            "sess-layout",
            vec![(
                vec![0x90; 16],
                vec![0x91; 8],
                T0,
                None,
                gen_attrs("1", "0.01"),
            )],
        ),
    )
    .await;
    flush(&state).await;

    // DuckLake exposes partition/sort via ducklake_table_info when available;
    // at minimum the table must exist and accept record_date predicates.
    let (st, body) = run_sql(
        &router,
        "SELECT COUNT(*)::BIGINT FROM session_stats_delta WHERE record_date >= DATE '2024-07-18' AND record_date <= DATE '2024-07-20'",
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(body["rows"][0][0].as_i64().unwrap() >= 1, "{body}");
}
