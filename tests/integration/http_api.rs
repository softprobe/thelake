//! Integration tests for the OTLP HTTP surface (in-process router — same handlers as
//! production without `main` middleware layers). Workspaces are lazy via WorkspaceManager.

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::AppState;
use std::sync::Arc;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::{double_kv, int_kv, string_kv};

async fn build_router() -> (Router, tempfile::TempDir) {
    let (router, _state, temp) = build_router_and_state().await;
    (router, temp)
}

async fn build_router_and_state() -> (Router, AppState, tempfile::TempDir) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let config = Arc::new(file_backed_test_config(&temp));
    let (router, state) = softprobe_runtime::api::create_router(config, None)
        .await
        .expect("router");
    (router, state, temp)
}

async fn response_json(resp: Response<Body>) -> Value {
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

fn llm_generation_request(
    session_id: &str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> ExportTraceServiceRequest {
    let generation = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: vec![],
        name: "chat.completions".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: 1_721_349_720_000_000_000,
        end_time_unix_nano: 1_721_349_721_500_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("sp.observation.type", "generation"),
            string_kv("sp.user.id", "user-llm-1"),
            string_kv("gen_ai.provider.name", "openai"),
            string_kv("gen_ai.request.model", "gpt-4o"),
            string_kv("gen_ai.operation.name", "chat"),
            int_kv("gen_ai.usage.input_tokens", 12),
            int_kv("gen_ai.usage.output_tokens", 34),
            int_kv("gen_ai.usage.total_tokens", 46),
            double_kv("sp.cost.total", 0.0123),
        ],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    };

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "llm-gateway")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.llm".to_string(),
                    ..Default::default()
                }),
                spans: vec![generation],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn telemetry_trace_request(session_id: &str, trace_id: [u8; 16]) -> ExportTraceServiceRequest {
    let root = Span {
        trace_id: trace_id.to_vec(),
        span_id: vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "POST /api/checkout".to_string(),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: 1_777_802_600_000_000_000,
        end_time_unix_nano: 1_777_802_601_500_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("http.request.method", "POST"),
            string_kv("http.request.path", "/api/checkout"),
            int_kv("http.response.status_code", 503),
        ],
        status: Some(Status {
            code: 2,
            message: "payment provider timeout".to_string(),
        }),
        ..Default::default()
    };

    let resource_spans = ResourceSpans {
        resource: Some(Resource {
            attributes: vec![string_kv("service.name", "checkout-api")],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "softprobe.e2e".to_string(),
                ..Default::default()
            }),
            spans: vec![root],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

fn telemetry_logs_request(session_id: &str, trace_id: [u8; 16]) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("sp.session.id", session_id),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_777_802_601_000_000_000,
                    severity_number: 17,
                    severity_text: "ERROR".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "payment provider timeout".to_string(),
                        )),
                    }),
                    attributes: vec![string_kv("sp.session.id", session_id)],
                    trace_id: trace_id.to_vec(),
                    span_id: vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn health_returns_ok_envelope() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert_eq!(v["status"], "ok");
    assert_eq!(v["specVersion"], "http-control-api@v1");
}

#[tokio::test]
async fn landing_page_is_served_at_root_without_shadowing_health() {
    let (router, _t) = build_router().await;

    let landing = router
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .expect("landing");
    assert_eq!(landing.status(), StatusCode::OK);
    let ctype = landing
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        ctype.starts_with("text/html"),
        "expected text/html, got {ctype}"
    );
    let body = landing
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let html = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(html.contains("thelake"));
    assert!(html.contains("make run"));
    assert!(html.contains("200x cheaper"));
    assert!(html.contains("long-term storage"));
    assert!(html.contains("https://www.softprobe.ai/"));
    assert!(html.contains("/styles.css"));
    assert!(html.contains("/script.js"));
    assert!(html.contains("/assets/hero.svg"));
    assert!(html.contains(r#"property="og:image""#));
    assert!(html.contains("https://thelake.softprobe.ai/assets/og-image.png"));
    assert!(html.contains(r#"name="twitter:card""#));
    assert!(html.contains(r#"application/ld+json"#));
    assert!(html.contains("SoftwareApplication"));

    let css = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/styles.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("css");
    assert_eq!(css.status(), StatusCode::OK);
    assert!(css
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/css"));

    let js = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/script.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("js");
    assert_eq!(js.status(), StatusCode::OK);
    assert!(js
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("application/javascript"));

    let hero = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/assets/hero.svg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("hero");
    assert_eq!(hero.status(), StatusCode::OK);
    assert_eq!(
        hero.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("image/svg+xml")
    );
    let hero_body = hero
        .into_body()
        .collect()
        .await
        .expect("hero body")
        .to_bytes();
    let hero_svg = String::from_utf8(hero_body.to_vec()).expect("hero utf8");
    assert!(hero_svg.contains("open SQL on your lake"));

    let og = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/assets/og-image.png")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("og-image");
    assert_eq!(og.status(), StatusCode::OK);
    assert_eq!(
        og.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("image/png")
    );

    for (uri, mime_prefix, must_contain) in [
        (
            "/robots.txt",
            "text/plain",
            "Sitemap: https://thelake.softprobe.ai/sitemap.xml",
        ),
        (
            "/sitemap.xml",
            "application/xml",
            "https://thelake.softprobe.ai/",
        ),
        ("/llms.txt", "text/plain", "# thelake"),
    ] {
        let resp = router
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap_or_else(|_| panic!("{uri}"));
        assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        let ctype = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            ctype.starts_with(mime_prefix),
            "{uri}: expected {mime_prefix}, got {ctype}"
        );
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|_| panic!("{uri} body"))
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap_or_else(|_| panic!("{uri} utf8"));
        assert!(
            text.contains(must_contain),
            "{uri} missing {must_contain:?}"
        );
    }

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/assets/does-not-exist.svg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("missing asset");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let health = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("health");
    assert_eq!(health.status(), StatusCode::OK);
    let v = response_json(health).await;
    assert_eq!(v["status"], "ok");
}

#[tokio::test]
async fn ready_returns_ready() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .uri("/ready")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert_eq!(v["status"], "ready");
}

#[tokio::test]
async fn openapi_and_swagger_endpoints_are_served() {
    let (router, _t) = build_router().await;

    let req = Request::builder()
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );

    let req = Request::builder()
        .uri("/swagger")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let html = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(html.contains("SwaggerUIBundle"));
    assert!(html.contains("/openapi.json"));
    assert!(html.contains("thelake API"));
}

#[tokio::test]
async fn traces_json_empty_batch_succeeds() {
    let (router, _t) = build_router().await;
    let body = json!({ "resourceSpans": [] }).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert!(v["success"].as_bool().unwrap());
    assert_eq!(v["ingested_count"], 0);
}

#[tokio::test]
async fn traces_json_invalid_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("not-json"))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn traces_protobuf_empty_roundtrip() {
    let (router, _t) = build_router().await;
    let mut buf = Vec::new();
    ExportTraceServiceRequest::default()
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn traces_protobuf_garbage_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(vec![0xffu8, 0xfe]))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn logs_json_empty_batch() {
    let (router, _t) = build_router().await;
    let body = json!({ "resourceLogs": [] }).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn logs_json_invalid_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn query_sql_empty_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": "   " }).to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn telemetry_search_select_literal() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "version": 1,
                "scope": "traces",
                "timeRange": {
                    "from": "2026-05-03T10:00:00Z",
                    "to": "2026-05-03T11:00:00Z"
                },
                "limit": 1
            })
            .to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert!(v["rows"].is_array());
}

#[tokio::test]
async fn logs_protobuf_empty() {
    let (router, _t) = build_router().await;
    let mut buf = Vec::new();
    ExportLogsServiceRequest::default()
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn telemetry_search_sessions_returns_summary_rows() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-search-e2e";
    let trace_id = [
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];

    let trace_body = serde_json::to_string(&telemetry_trace_request(session_id, trace_id)).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(trace_body))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("trace ingest");
    assert_eq!(resp.status(), StatusCode::OK);

    let ws = state.workspace_for_id("").await.expect("workspace context");
    ws.ingest().force_flush_spans().await.expect("flush spans");

    let body = json!({
        "version": 1,
        "scope": "sessions",
        "timeRange": {
            "from": "2026-05-03T10:00:00Z",
            "to": "2026-05-03T11:00:00Z"
        },
        "filter": {
            "and": [
                { "field": "service.name", "op": "eq", "value": "checkout-api" },
                { "field": "http_request_path", "op": "prefix", "value": "/api/checkout" }
            ]
        },
        "columns": ["session_id", "trace_count", "error_count"],
        "sort": [{ "field": "timestamp", "direction": "desc" }],
        "limit": 25
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("search");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;

    assert_eq!(v["version"], 1);
    assert_eq!(v["scope"], "sessions");
    assert_eq!(v["rows"][0]["id"], session_id);
    assert_eq!(v["rows"][0]["summary"]["traceCount"], 1);
    assert_eq!(v["rows"][0]["summary"]["spanCount"], 1);
    assert_eq!(v["rows"][0]["summary"]["errorCount"], 1);
}

#[tokio::test]
async fn timestamp_ns_span_queries_work_through_http_paths() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-timestamp-ns-regression";
    let trace_id = [0x42; 16];
    let span_hex = hex::encode([0x43; 8]);

    let body =
        serde_json::to_string(&llm_generation_request(session_id, trace_id, [0x43; 8])).unwrap();
    let ingest = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = router.clone().oneshot(ingest).await.expect("ingest");
    assert_eq!(response.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_spans()
        .await
        .expect("flush spans");

    let search = Request::builder()
        .method("POST")
        .uri("/v1/spans/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "session_id": session_id,
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(search).await.unwrap().status(),
        StatusCode::OK
    );

    for uri in [
        format!("/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"),
        format!(
            "/v1/traces/{}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z",
            hex::encode(trace_id)
        ),
    ] {
        let response = router
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Session detail requires a reduced session_summary row; none exists yet for
    // this session (no reduce has run), so lookup is a 404, not an error.
    let session_resp = router
        .clone()
        .oneshot(
            Request::get(format!("/v1/sessions/{session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(session_resp.status(), StatusCode::NOT_FOUND);

    let telemetry = Request::builder()
        .method("POST")
        .uri("/v1/traces/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "version": 1,
                "scope": "traces",
                "timeRange": {
                    "from": "2024-07-18T00:00:00Z",
                    "to": "2024-07-20T00:00:00Z"
                },
                "filter": { "field": "trace_id", "op": "eq", "value": hex::encode(trace_id) },
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        router.oneshot(telemetry).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn telemetry_session_details_returns_spans_and_logs() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-details-e2e";
    let trace_bytes = [
        0xba, 0xad, 0xf0, 0x0d, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];
    let trace_hex = hex::encode(trace_bytes);

    for (uri, body) in [
        (
            "/v1/traces",
            serde_json::to_string(&telemetry_trace_request(session_id, trace_bytes)).unwrap(),
        ),
        (
            "/v1/logs",
            serde_json::to_string(&telemetry_logs_request(session_id, trace_bytes)).unwrap(),
        ),
    ] {
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("ingest");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let ws = state.workspace_for_id("").await.expect("workspace context");
    ws.ingest().force_flush_spans().await.expect("flush spans");
    ws.ingest().force_flush_logs().await.expect("flush logs");

    let req = Request::builder()
        .uri(format!(
            "/v1/sessions/{}?from=2026-05-03T10:00:00Z&to=2026-05-03T11:00:00Z",
            session_id
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("details");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;

    assert_eq!(v["version"], 1);
    assert_eq!(v["kind"], "session");
    assert_eq!(v["id"], session_id);
    assert_eq!(v["summary"]["spanCount"], 1);
    assert_eq!(v["summary"]["logCount"], 1);
    assert_eq!(v["spans"][0]["trace_id"], trace_hex);
    assert_eq!(v["logs"][0]["body"], "payment provider timeout");
}

#[tokio::test]
async fn llm_query_endpoints_return_spans_traces_sessions_and_scores() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-llm-query-e2e";
    let trace_bytes = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47,
        0x36,
    ];
    let span_bytes = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
    let trace_hex = hex::encode(trace_bytes);
    let span_hex = hex::encode(span_bytes);

    let ingest = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(&llm_generation_request(session_id, trace_bytes, span_bytes))
                .unwrap(),
        ))
        .unwrap();
    let ingest_resp = router.clone().oneshot(ingest).await.expect("ingest");
    assert_eq!(ingest_resp.status(), StatusCode::OK);

    let ws = state.workspace_for_id("").await.expect("workspace context");
    ws.ingest().force_flush_spans().await.expect("flush spans");

    let score_body = json!({
        "score_id": "score-llm-query-1",
        "timestamp": "2024-07-19T00:02:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "correctness",
        "data_type": "numeric",
        "numeric_value": 0.91,
        "source": "evaluator",
        "metadata": { "suite": "integration" }
    });
    let score_req = Request::builder()
        .method("POST")
        .uri("/v1/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let score_resp = router.clone().oneshot(score_req).await.expect("score");
    assert_eq!(score_resp.status(), StatusCode::CREATED);

    let search_req = Request::builder()
        .method("POST")
        .uri("/v1/spans/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "span_types": ["generation"],
                "model_name": "gpt-4o",
                "user_id": "user-llm-1",
                "session_id": session_id,
                "limit": 50
            })
            .to_string(),
        ))
        .unwrap();
    let search_resp = router.clone().oneshot(search_req).await.expect("search");
    assert_eq!(search_resp.status(), StatusCode::OK);
    let search = response_json(search_resp).await;
    assert_eq!(search["items"].as_array().unwrap().len(), 1);
    assert_eq!(search["items"][0]["span_id"], span_hex);
    assert_eq!(search["items"][0]["span_type"], "generation");
    assert_eq!(search["items"][0]["model_name"], "gpt-4o");
    assert_eq!(search["items"][0]["model_provider"], "openai");
    assert_eq!(search["items"][0]["user_id"], "user-llm-1");
    assert_eq!(search["items"][0]["input_tokens"], 12);
    assert_eq!(search["items"][0]["output_tokens"], 34);
    assert_eq!(search["items"][0]["total_tokens"], 46);
    assert!(
        (search["items"][0]["total_cost"].as_f64().unwrap_or(0.0) - 0.0123).abs() < 1e-9,
        "total_cost={}",
        search["items"][0]["total_cost"]
    );
    assert!(search["items"][0].get("attributes").is_none());

    // Attribute-map key negative filter: wrong user_id must not match.
    let miss_req = Request::builder()
        .method("POST")
        .uri("/v1/spans/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "span_types": ["generation"],
                "user_id": "no-such-user",
                "session_id": session_id,
                "limit": 50
            })
            .to_string(),
        ))
        .unwrap();
    let miss_resp = router.clone().oneshot(miss_req).await.expect("miss search");
    assert_eq!(miss_resp.status(), StatusCode::OK);
    let miss = response_json(miss_resp).await;
    assert_eq!(miss["items"].as_array().unwrap().len(), 0);

    let obs_req = Request::builder()
        .uri(format!(
            "/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let obs_resp = router.clone().oneshot(obs_req).await.expect("observation");
    if obs_resp.status() != StatusCode::OK {
        let body = response_json(obs_resp).await;
        panic!("DEBUG span failed: {body}");
    }
    let obs = response_json(obs_resp).await;
    assert_eq!(obs["span_id"], span_hex);
    assert_eq!(obs["attributes"]["sp.observation.type"], "generation");
    assert_eq!(obs["scores"].as_array().unwrap().len(), 1);
    assert_eq!(obs["scores"][0]["score_id"], "score-llm-query-1");

    let trace_req = Request::builder()
        .uri(format!(
            "/v1/traces/{trace_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let trace_resp = router.clone().oneshot(trace_req).await.expect("trace");
    assert_eq!(trace_resp.status(), StatusCode::OK);
    let trace = response_json(trace_resp).await;
    assert_eq!(trace["trace"]["trace_id"], trace_hex);
    assert_eq!(trace["trace"]["span_count"], 1);
    assert_eq!(trace["spans"].as_array().unwrap().len(), 1);
    assert_eq!(trace["scores"].as_array().unwrap().len(), 1);

    let session_req = Request::builder()
        .uri(format!("/v1/sessions/{session_id}"))
        .body(Body::empty())
        .unwrap();
    let session_resp = router.clone().oneshot(session_req).await.expect("session");
    // No reduce has run yet → summary row missing → 404 (not a registry outage).
    assert_eq!(session_resp.status(), StatusCode::NOT_FOUND);

    let missing = Request::builder()
        .uri("/v1/spans/does-not-exist?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z")
        .body(Body::empty())
        .unwrap();
    let missing_resp = router.oneshot(missing).await.expect("missing");
    assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn logs_promote_scope_name_to_logger_name_attribute() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-logger-name-promote";

    let payload = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("sp.session.id", session_id),
                ],
                ..Default::default()
            }),
            scope_logs: vec![
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "agent.transform.success".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_777_802_601_000_000_000,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "promoted-from-scope".to_string(),
                            )),
                        }),
                        attributes: vec![string_kv("sp.session.id", session_id)],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                },
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope.should.not.win".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_777_802_602_000_000_000,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "explicit-attribute-wins".to_string(),
                            )),
                        }),
                        attributes: vec![
                            string_kv("sp.session.id", session_id),
                            string_kv("logger_name", "explicit.logger"),
                        ],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                },
            ],
            schema_url: String::new(),
        }],
    };

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&payload).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);

    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_logs()
        .await
        .expect("flush logs");

    let req = Request::builder()
        .method("POST")
        .uri("/v1/sessions/details")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "version": 1,
                "target": { "kind": "session", "id": session_id },
                "timeRange": {
                    "from": "2026-05-03T10:00:00Z",
                    "to": "2026-05-03T11:00:00Z"
                },
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.expect("query");
    let status = resp.status();
    let v = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let rows = v["logs"].as_array().expect("logs");
    assert_eq!(rows.len(), 2, "{v}");
    assert_eq!(rows[0]["body"], "promoted-from-scope");
    assert_eq!(
        rows[0]["attributes"]["logger_name"],
        "agent.transform.success"
    );
    assert_eq!(rows[1]["body"], "explicit-attribute-wins");
    assert_eq!(rows[1]["attributes"]["logger_name"], "explicit.logger");
}

/// Session search pagination with sub-millisecond cursors is covered by
/// `session_summary_list` (integration-e2e), which runs dirty→reduce before search.

#[tokio::test]
async fn spans_without_events_are_readable() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-empty-events";
    let span_hex = hex::encode([0x91u8; 8]);

    // llm_generation_request leaves `events` empty -> `[]` once stored.
    let mut buf = Vec::new();
    llm_generation_request(session_id, [0x90; 16], [0x91; 8])
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_spans()
        .await
        .expect("flush spans");

    // The detail endpoint projects events; on 1.5.2 this is where it died.
    let req = Request::builder()
        .uri(format!(
            "/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("observation detail");
    let status = resp.status();
    let obs = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "reading a span whose events are an empty array must not fail: {obs}"
    );
    assert_eq!(obs["span_id"], span_hex);
    assert!(
        obs["events"].as_array().is_none_or(|e| e.is_empty()),
        "expected no events, got: {}",
        obs["events"]
    );

    // And again through search, which projects a different column set.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spans/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "session_id": session_id,
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.expect("search");
    assert_eq!(resp.status(), StatusCode::OK);
    let found = response_json(resp).await;
    assert_eq!(
        found["items"].as_array().expect("items").len(),
        1,
        "{found}"
    );
}

/// Pins DuckLake data-inlining behavior across a maintenance pass -- the
/// 2026-08-03 production outage shape. Collector-sized batches can be inlined
/// into the catalog (`data_inlining_row_limit`, default 500),
/// and reads of inlined rows go through the ducklake extension's inlined-data
/// reader; in production (postgres catalog) that reader crashed with
/// "INTERNAL Error: Attempted to access index 0 within vector of size 0" and
/// DuckDB invalidated the whole database. Until this test existed no CI path
/// ever read inlined data back, let alone after maintenance ran over it.
///
/// Attribute bags use MAP columns, so PostgreSQL-backed DuckLake
/// can inline traces/logs under the catalog limit. This test raises
/// `data_inlining_row_limit=10_000` to force the inlined reader on collector-sized
/// batches (production default is 500). Scores (MAP
/// metadata) remain the primary inlined path this test walks across maintenance.
#[tokio::test]
async fn inlined_data_stays_readable_across_maintenance() {
    fn parquet_count(dir: &std::path::Path) -> usize {
        let mut n = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    n += parquet_count(&path);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    n += 1;
                }
            }
        }
        n
    }

    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    // Explicit, not inherited from Config::default(): the inlined path is the
    // point, and this test must keep covering it even if the default flips
    // to 0 (the post-outage production setting).
    config.ducklake.data_inlining_row_limit = Some(10_000);
    // file_backed_test_config turns maintenance OFF for quiet tests. Without
    // these two the maintenance step below is a no-op loop that reports
    // Skipped for every table, and the "across a maintenance pass" claim in
    // this test's name would be theatre.
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    let config = Arc::new(config);
    let (router, state) = softprobe_runtime::api::create_router(config.clone(), None)
        .await
        .expect("router");

    let session_id = "sess-inline-maintenance";
    let trace_hex = hex::encode([0x51u8; 16]);
    let span_hex = hex::encode([0x61u8; 8]);
    let data_dir = temp.path().join("ducklake").join("data");

    // 1. One collector-sized span batch. MAP bags may inline under limit=10_000;
    //    either Parquet or inlined catalog rows must stay readable below.
    let mut buf = Vec::new();
    llm_generation_request(session_id, [0x51; 16], [0x61; 8])
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_spans()
        .await
        .expect("flush spans");
    let schema_dir = data_dir.join(&config.ducklake.metadata_schema);
    let _traces_parquet = parquet_count(&schema_dir.join("traces"));
    // MAP + limit=10_000 may inline (0 parquet) or write Parquet; both are OK.

    // 2. One score -> the scores table has MAP metadata and should inline under
    //    limit=10_000 (not written as Parquet).
    let score_body = json!({
        "score_id": "score-inline-1",
        "timestamp": "2024-07-19T00:02:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "correctness",
        "data_type": "numeric",
        "numeric_value": 0.91,
        "source": "evaluator",
        "metadata": { "suite": "inline" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("score");
    assert_eq!(resp.status(), StatusCode::CREATED);
    // `parquet_count` returns 0 for a missing directory, so "scores has no
    // Parquet" alone would also pass if the layout assumption
    // (<data_path>/<metadata_schema>/<table>/) ever broke. Anchor on the
    // traces directory existing to tell the two apart.
    let scores_dir = schema_dir.join("scores");
    assert!(
        schema_dir.join("traces").is_dir(),
        "expected <data_path>/{}/<table>/ layout; found: {:?}",
        config.ducklake.metadata_schema,
        std::fs::read_dir(&data_dir).map(|e| e.flatten().map(|x| x.path()).collect::<Vec<_>>())
    );
    assert_eq!(
        parquet_count(&scores_dir),
        0,
        "score batch wrote Parquet -- inlining is not active, this test no \
         longer covers the inlined read path"
    );

    // 3. Inlined read #1: observation detail joins scores through the query
    //    engine's ducklake attachment.
    let req = Request::builder()
        .uri(format!(
            "/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("observation");
    assert_eq!(resp.status(), StatusCode::OK);
    let obs = response_json(resp).await;
    assert_eq!(obs["scores"].as_array().expect("scores").len(), 1, "{obs}");
    assert_eq!(obs["scores"][0]["score_id"], "score-inline-1");

    // 4. A maintenance pass over the same catalog (production runs this
    //    hourly; the outage query came 23 minutes after one).
    //    Merge and cleanup outcomes are persisted by the SQL script. A SQL
    //    failure returns Err from run_once and fails this integration pass.
    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("maintenance executor");
    maintenance.run_once().await.expect("maintenance run");

    // 5. Inlined read #2, after maintenance -- the production crash site.
    let req = Request::builder()
        .uri(format!(
            "/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("observation after maintenance");
    let status = resp.status();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("obs body");
    assert_eq!(
        status,
        StatusCode::OK,
        "observation after maintenance: {}",
        String::from_utf8_lossy(&body_bytes)
    );
    let obs: serde_json::Value = serde_json::from_slice(&body_bytes).expect("obs json");
    assert_eq!(
        obs["scores"].as_array().expect("scores").len(),
        1,
        "inlined score disappeared after maintenance: {obs}"
    );

    // 6. Inlined write + read after maintenance.
    let score_body = json!({
        "score_id": "score-inline-2",
        "timestamp": "2024-07-19T00:03:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "helpfulness",
        "data_type": "numeric",
        "numeric_value": 0.8,
        "source": "evaluator",
        "metadata": { "suite": "inline" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("score after maintenance");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .uri(format!(
            "/v1/spans/{span_hex}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("final observation");
    assert_eq!(resp.status(), StatusCode::OK);
    let obs = response_json(resp).await;
    assert_eq!(
        obs["scores"].as_array().expect("scores").len(),
        2,
        "post-maintenance inlined write not visible: {obs}"
    );
}

#[tokio::test]
async fn metrics_ingest_route_is_not_found() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn prometheus_query_routes_are_not_found() {
    let (router, _t) = build_router().await;
    for uri in [
        "/api/v1/query?query=up",
        "/api/v1/query_range?query=up&start=1&end=2&step=15",
    ] {
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "uri={uri}");
    }
}

#[tokio::test]
async fn otel_genai_hierarchy_serves_canonical_types_leaf_tokens_clean_json_and_optional_bounds() {
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValue};

    let (router, state, _t) = build_router_and_state().await;
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let start_ns = now_ns - 60_000_000_000;
    let trace_bytes = [0xab; 16];
    let agent_bytes = [0x11; 8];
    let gen_bytes = [0x22; 8];
    let tool_bytes = [0x33; 8];
    let body_bytes = [0x44; 8];
    let trace_hex = hex::encode(trace_bytes);
    let gen_hex = hex::encode(gen_bytes);
    let session_id = "ses_pure_otel_http";

    let finish_reasons = KeyValue {
        key: "gen_ai.response.finish_reasons".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(any_value::Value::StringValue("stop".to_string())),
                }],
            })),
        }),
    };

    let payload = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "travel-agent"),
                    KeyValue {
                        key: "process.command_args".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::ArrayValue(ArrayValue {
                                values: vec![AnyValue {
                                    value: Some(any_value::Value::StringValue("node".to_string())),
                                }],
                            })),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "otel.genai".to_string(),
                    ..Default::default()
                }),
                spans: vec![
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: agent_bytes.to_vec(),
                        parent_span_id: vec![],
                        name: "invoke_agent concierge".to_string(),
                        kind: span::SpanKind::Internal as i32,
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 4_000_000_000,
                        attributes: vec![
                            string_kv("gen_ai.operation.name", "invoke_agent"),
                            string_kv("gen_ai.agent.name", "concierge"),
                            string_kv("gen_ai.conversation.id", session_id),
                            string_kv("user.id", "usr_otel_42"),
                            int_kv("gen_ai.usage.input_tokens", 25854),
                            int_kv("gen_ai.usage.output_tokens", 2447),
                            int_kv("gen_ai.usage.total_tokens", 29000),
                        ],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        ..Default::default()
                    },
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: gen_bytes.to_vec(),
                        parent_span_id: agent_bytes.to_vec(),
                        name: "chat gemini-2.5-flash".to_string(),
                        kind: span::SpanKind::Client as i32,
                        start_time_unix_nano: start_ns + 500_000_000,
                        end_time_unix_nano: start_ns + 3_000_000_000,
                        attributes: vec![
                            string_kv("gen_ai.operation.name", "chat"),
                            string_kv("gen_ai.conversation.id", session_id),
                            string_kv("user.id", "usr_otel_42"),
                            string_kv("gen_ai.request.model", "gemini-2.5-flash"),
                            string_kv("gen_ai.provider.name", "gcp.vertex.agent"),
                            int_kv("gen_ai.usage.input_tokens", 25854),
                            int_kv("gen_ai.usage.output_tokens", 2447),
                            int_kv("gen_ai.usage.total_tokens", 29000),
                            finish_reasons,
                        ],
                        status: Some(Status {
                            code: 1,
                            message: "cached context used".to_string(),
                        }),
                        ..Default::default()
                    },
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: tool_bytes.to_vec(),
                        parent_span_id: agent_bytes.to_vec(),
                        name: "execute_tool search_flights".to_string(),
                        kind: span::SpanKind::Internal as i32,
                        start_time_unix_nano: start_ns + 3_100_000_000,
                        end_time_unix_nano: start_ns + 3_800_000_000,
                        attributes: vec![
                            string_kv("gen_ai.operation.name", "execute_tool"),
                            string_kv("gen_ai.conversation.id", session_id),
                            string_kv("gen_ai.tool.name", "search_flights"),
                        ],
                        ..Default::default()
                    },
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: body_bytes.to_vec(),
                        parent_span_id: tool_bytes.to_vec(),
                        name: "softprobe.http.body.capture".to_string(),
                        kind: span::SpanKind::Unspecified as i32,
                        start_time_unix_nano: start_ns + 3_200_000_000,
                        end_time_unix_nano: start_ns + 3_250_000_000,
                        attributes: vec![string_kv("gen_ai.conversation.id", session_id)],
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let mut encoded = Vec::new();
    payload.encode(&mut encoded).expect("encode");
    let ingest_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(encoded))
                .unwrap(),
        )
        .await
        .expect("ingest");
    assert_eq!(ingest_resp.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_spans()
        .await
        .expect("flush spans");

    let from_iso = (chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339();
    let to_iso = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let search_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/spans/search")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "from": from_iso,
                        "to": to_iso,
                        "session_id": session_id,
                        "user_id": "usr_otel_42"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("search spans");
    let search_status = search_resp.status();
    let search_json = response_json(search_resp).await;
    assert_eq!(search_status, StatusCode::OK, "{search_json}");
    let items = search_json["items"].as_array().expect("items");
    assert_eq!(
        items.len(),
        2,
        "agent + generation carry usr_otel_42: {search_json}"
    );
    let gen_summary = items
        .iter()
        .find(|item| item["span_id"] == gen_hex)
        .expect("gen summary");
    assert_eq!(gen_summary["span_type"], "generation");
    assert_eq!(gen_summary["service_name"], "travel-agent");
    assert_eq!(gen_summary["span_kind"], "CLIENT");
    assert_eq!(gen_summary["status_message"], "cached context used");
    assert!(
        gen_summary.get("app_id").is_none(),
        "SpanSummary must not expose internal app_id: {gen_summary}"
    );

    let span_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/spans/{gen_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get span without from/to");
    assert_eq!(span_resp.status(), StatusCode::OK);
    let span_json = response_json(span_resp).await;
    assert_eq!(span_json["service_name"], "travel-agent");
    assert_eq!(span_json["span_kind"], "CLIENT");
    assert_eq!(span_json["status_message"], "cached context used");
    assert_eq!(
        span_json["app_id"], "travel-agent",
        "SpanDetail preserves backward-compatible app_id"
    );
    assert_eq!(
        span_json["attributes"]["gen_ai.response.finish_reasons"],
        "[\"stop\"]"
    );
    assert_eq!(
        span_json["resource_attributes"]["service.name"],
        "travel-agent"
    );
    assert_eq!(
        span_json["resource_attributes"]["process.command_args"],
        "[\"node\"]"
    );

    let trace_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/traces/{trace_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get trace without from/to");
    assert_eq!(trace_resp.status(), StatusCode::OK);
    let trace_json = response_json(trace_resp).await;
    assert_eq!(trace_json["trace"]["trace_id"], trace_hex);
    assert_eq!(trace_json["trace"]["span_count"], 3);
    assert_eq!(trace_json["trace"]["input_tokens"], 25854);
    assert_eq!(trace_json["trace"]["output_tokens"], 2447);
    assert_eq!(trace_json["trace"]["total_tokens"], 29000);

    let log_body = json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "travel-agent"}},
                    {
                        "key": "process.command_args",
                        "value": {"arrayValue": {"values": [{"stringValue": "node"}]}}
                    }
                ]
            },
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": (start_ns + 1_000_000_000).to_string(),
                    "observedTimeUnixNano": (start_ns + 1_000_000_000).to_string(),
                    "traceId": trace_hex,
                    "spanId": gen_hex,
                    "body": {},
                    "attributes": [
                        {"key": "sp.session.id", "value": {"stringValue": session_id}},
                        {"key": "empty_meta", "value": {}}
                    ]
                }]
            }]
        }]
    });
    let log_ingest_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(log_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest correlated log");
    assert_eq!(log_ingest_resp.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_logs()
        .await
        .expect("flush logs");

    let from_z = (chrono::Utc::now() - chrono::Duration::minutes(10))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to_z = (chrono::Utc::now() + chrono::Duration::minutes(5))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let bounded_session_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/sessions/{session_id}?from={from_z}&to={to_z}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get bounded session details");
    assert_eq!(bounded_session_resp.status(), StatusCode::OK);
    let bounded_session_json = response_json(bounded_session_resp).await;
    let bounded_gen = bounded_session_json["spans"]
        .as_array()
        .expect("bounded session spans")
        .iter()
        .find(|s| s["span_id"] == gen_hex)
        .expect("bounded gen span");
    assert_eq!(
        bounded_gen["attributes"]["gen_ai.response.finish_reasons"],
        "[\"stop\"]"
    );
    let bounded_log = &bounded_session_json["logs"]
        .as_array()
        .expect("bounded session logs")[0];
    assert_eq!(
        bounded_log["resource_attributes"]["process.command_args"],
        "[\"node\"]"
    );
    assert_eq!(bounded_log["attributes"]["empty_meta"], "");

    let trace_details_resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces/details")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "traceId": trace_hex,
                        "timeRange": { "from": from_z, "to": to_z }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("post trace details");
    assert_eq!(trace_details_resp.status(), StatusCode::OK);
    let trace_details_json = response_json(trace_details_resp).await;
    let details_gen = trace_details_json["spans"]
        .as_array()
        .expect("trace details spans")
        .iter()
        .find(|s| s["span_id"] == gen_hex)
        .expect("trace details gen span");
    assert_eq!(
        details_gen["attributes"]["gen_ai.response.finish_reasons"],
        "[\"stop\"]"
    );
    let details_log = &trace_details_json["logs"]
        .as_array()
        .expect("trace details logs")[0];
    assert_eq!(
        details_log["resource_attributes"]["process.command_args"],
        "[\"node\"]"
    );
}

#[tokio::test]
async fn otlp_json_ingest_preserves_array_kvlist_and_int_attributes() {
    let (router, state, _temp) = build_router_and_state().await;
    let trace_hex = "99887766554433221100ffeeddccbbaa";
    let span_hex = "aabbccddeeff0011";
    let start_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap() as u64;

    let body = json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "json-agent"}},
                    {
                        "key": "process.command_args",
                        "value": {"arrayValue": {"values": [{"stringValue": "bun"}, {"stringValue": "run"}]}}
                    }
                ]
            },
            "scopeSpans": [{
                "scope": {"name": "otel.json.test"},
                "spans": [{
                    "traceId": trace_hex,
                    "spanId": span_hex,
                    "name": "chat gpt-4o",
                    "kind": 3,
                    "startTimeUnixNano": start_ns.to_string(),
                    "endTimeUnixNano": (start_ns + 1_000_000_000).to_string(),
                    "attributes": [
                        {"key": "gen_ai.operation.name", "value": {"stringValue": "chat"}},
                        {"key": "gen_ai.conversation.id", "value": {"stringValue": "ses_json_1"}},
                        {"key": "gen_ai.usage.input_tokens", "value": {"intValue": 120}},
                        {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "30"}},
                        {
                            "key": "gen_ai.response.finish_reasons",
                            "value": {
                                "arrayValue": {
                                    "values": [{"stringValue": "tool_calls"}]
                                }
                            }
                        },
                        {
                            "key": "llm.token_windows",
                            "value": {
                                "arrayValue": {
                                    "values": [{"intValue": "100"}, {"intValue": 200}]
                                }
                            }
                        },
                        {
                            "key": "gen_ai.input.messages",
                            "value": {
                                "arrayValue": {
                                    "values": [{
                                        "kvlistValue": {
                                            "values": [
                                                {"key": "role", "value": {"stringValue": "user"}},
                                                {
                                                    "key": "parts",
                                                    "value": {
                                                        "arrayValue": {
                                                            "values": [{"stringValue": "book flight"}]
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    }]
                                }
                            }
                        }
                    ]
                }]
            }]
        }]
    });

    let ingest_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest json");
    assert_eq!(ingest_resp.status(), StatusCode::OK);
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
        .force_flush_spans()
        .await
        .expect("flush spans");

    let span_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/spans/{span_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get span");
    assert_eq!(span_resp.status(), StatusCode::OK);
    let span_json = response_json(span_resp).await;
    assert_eq!(span_json["input_tokens"], 120);
    assert_eq!(span_json["output_tokens"], 30);
    assert_eq!(span_json["total_tokens"], Value::Null);
    assert_eq!(
        span_json["attributes"]["gen_ai.response.finish_reasons"],
        "[\"tool_calls\"]"
    );
    assert_eq!(span_json["attributes"]["llm.token_windows"], "[100,200]");
    let parsed_msgs: Value = serde_json::from_str(
        span_json["attributes"]["gen_ai.input.messages"]
            .as_str()
            .expect("messages string"),
    )
    .expect("valid json messages");
    assert_eq!(
        parsed_msgs,
        json!([{"role": "user", "parts": ["book flight"]}])
    );
    assert_eq!(
        span_json["resource_attributes"]["process.command_args"],
        "[\"bun\",\"run\"]"
    );

    let trace_resp = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/traces/{trace_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get trace");
    assert_eq!(trace_resp.status(), StatusCode::OK);
    let trace_json = response_json(trace_resp).await;
    assert_eq!(trace_json["trace"]["input_tokens"], 120);
    assert_eq!(trace_json["trace"]["output_tokens"], 30);
    assert_eq!(trace_json["trace"]["total_tokens"], 150);
}

#[tokio::test]
async fn invalid_vendor_token_strings_do_not_reject_promoted_otlp_ingest_batch() {
    use crate::util::promotion_file_backed::{
        apply_promotion_yaml, ingest_otlp_protobuf, setup_file_backed_promotion_env,
    };

    let env = setup_file_backed_promotion_env().await;
    let hot_attrs_yaml = include_str!("../../docs/promotion/traces-query-hot-attrs.yaml");
    apply_promotion_yaml(&env.router, hot_attrs_yaml).await;

    let trace_bytes = [0x51u8; 16];
    let trimmed_bytes = [0x61u8; 8];
    let float_bytes = [0x62u8; 8];
    let unknown_bytes = [0x63u8; 8];
    let trimmed_hex = hex::encode(trimmed_bytes);
    let float_hex = hex::encode(float_bytes);
    let unknown_hex = hex::encode(unknown_bytes);
    let start_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap() as u64;

    let payload = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "vercel-agent")],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "ai".to_string(),
                    ..Default::default()
                }),
                spans: vec![
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: trimmed_bytes.to_vec(),
                        name: "ai.streamText.doStream".to_string(),
                        kind: span::SpanKind::Client as i32,
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 500_000_000,
                        attributes: vec![
                            string_kv("ai.operationId", "ai.streamText.doStream"),
                            string_kv("ai.telemetry.metadata.sessionId", "ses_promo_tokens"),
                            string_kv("ai.usage.promptTokens", " 120 "),
                            string_kv("ai.usage.completionTokens", " 30 "),
                            string_kv("ai.usage.totalTokens", " 150 "),
                        ],
                        ..Default::default()
                    },
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: float_bytes.to_vec(),
                        name: "ai.streamText.doStream".to_string(),
                        kind: span::SpanKind::Client as i32,
                        start_time_unix_nano: start_ns + 600_000_000,
                        end_time_unix_nano: start_ns + 900_000_000,
                        attributes: vec![
                            string_kv("ai.operationId", "ai.streamText.doStream"),
                            string_kv("ai.telemetry.metadata.sessionId", "ses_promo_tokens"),
                            string_kv("ai.usage.promptTokens", "120.5"),
                        ],
                        ..Default::default()
                    },
                    Span {
                        trace_id: trace_bytes.to_vec(),
                        span_id: unknown_bytes.to_vec(),
                        name: "ai.streamText.doStream".to_string(),
                        kind: span::SpanKind::Client as i32,
                        start_time_unix_nano: start_ns + 1_000_000_000,
                        end_time_unix_nano: start_ns + 1_200_000_000,
                        attributes: vec![
                            string_kv("ai.operationId", "ai.streamText.doStream"),
                            string_kv("ai.telemetry.metadata.sessionId", "ses_promo_tokens"),
                            string_kv("ai.usage.promptTokens", "unknown"),
                        ],
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let mut encoded = Vec::new();
    payload.encode(&mut encoded).expect("encode");
    ingest_otlp_protobuf(env.router.clone(), encoded).await;

    let trimmed_resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/spans/{trimmed_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get trimmed span");
    assert_eq!(trimmed_resp.status(), StatusCode::OK);
    let trimmed_json = response_json(trimmed_resp).await;
    assert_eq!(trimmed_json["input_tokens"], 120);
    assert_eq!(trimmed_json["output_tokens"], 30);
    assert_eq!(trimmed_json["total_tokens"], 150);
    assert_eq!(
        trimmed_json["attributes"]["gen_ai.usage.input_tokens"],
        "120"
    );

    let float_resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/spans/{float_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get float span");
    assert_eq!(float_resp.status(), StatusCode::OK);
    let float_json = response_json(float_resp).await;
    assert!(
        float_json["attributes"]
            .get("gen_ai.usage.input_tokens")
            .is_none(),
        "float vendor string must not populate int64 promotion source key: {float_json}"
    );
    assert_eq!(float_json["attributes"]["ai.usage.promptTokens"], "120.5");

    let unknown_resp = env
        .router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/spans/{unknown_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get unknown span");
    assert_eq!(unknown_resp.status(), StatusCode::OK);
    let unknown_json = response_json(unknown_resp).await;
    assert!(unknown_json["attributes"]
        .get("gen_ai.usage.input_tokens")
        .is_none());
    assert_eq!(unknown_json["input_tokens"], Value::Null);
    assert_eq!(
        unknown_json["attributes"]["ai.usage.promptTokens"],
        "unknown"
    );
}
