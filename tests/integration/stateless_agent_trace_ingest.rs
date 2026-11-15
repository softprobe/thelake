use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::Router;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{Duration as ChronoDuration, Utc};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use sha2::Sha256;
use softprobe_runtime::api::auth::runtime_auth_middleware;
use softprobe_runtime::api::{create_router, AppState, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::softprobe_assertion::ASSERTION_HEADER;
use softprobe_runtime::storage::ducklake::open_attached_from_config;
use softprobe_runtime::workspace::ScopeProvisioningRequest;
use softprobe_runtime::workspace_scope::WorkspaceScopeMode;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;

const STORED_ROWS_SQL: &str = include_str!("../fixtures/stateless_ingest_stored_rows.sql");
const DIRTY_COUNT_SQL: &str = include_str!("../fixtures/stateless_ingest_dirty_count.sql");
const SUMMARY_COUNT_SQL: &str = include_str!("../fixtures/stateless_ingest_summary_count.sql");
const TEST_ASSERTION_SECRET: &str = "assert-agent-columns-secret";

fn mint_assertion(payload: Value, secret: &str) -> String {
    let hdr = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let body = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing = format!("{hdr}.{body}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing}.{sig}")
}

fn agent_token(secret: &str, workspace_id: &str, agent_id: &str, agent_name: &str) -> String {
    let now = Utc::now().timestamp();
    mint_assertion(
        json!({
            "iss": "softprobe-edge",
            "aud": "sp-backend",
            "sub": "agent-key",
            "workspace_id": workspace_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "capabilities": ["ingest"],
            "exp": now + 600
        }),
        secret,
    )
}

fn user_token(secret: &str, workspace_id: &str) -> String {
    let now = Utc::now().timestamp();
    mint_assertion(
        json!({
            "iss": "softprobe-edge",
            "aud": "sp-backend",
            "sub": "workspace-user",
            "workspace_id": workspace_id,
            "roles": ["member"],
            "exp": now + 600
        }),
        secret,
    )
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn int_kv(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn make_request_with_resource(
    resource_attrs: Vec<KeyValue>,
    spans: Vec<Span>,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.stateless.test".to_string(),
                    ..Default::default()
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn make_request(service_name: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    make_request_with_resource(vec![string_kv("service.name", service_name)], spans)
}

async fn setup_router(
    secret: &str,
    flush_interval_seconds: u64,
    mode: WorkspaceScopeMode,
) -> (Router, AppState, TempDir) {
    std::env::set_var("SOFTPROBE_ASSERTION_HMAC_SECRET", secret);
    let temp = TempDir::new().expect("tempdir");
    let mut cfg: Config = file_backed_test_config(&temp);
    cfg.ingest.flush_interval_seconds = flush_interval_seconds;
    cfg.ducklake.workspace_scope_mode = mode;
    let control = ControlPlaneRuntime {
        resolver: Resolver::new("http://127.0.0.1:9/", Duration::from_secs(60)),
    };
    let (router, state) = create_router(Arc::new(cfg), Some(control))
        .await
        .expect("router");
    let router = router.layer(from_fn_with_state(state.clone(), runtime_auth_middleware));
    (router, state, temp)
}

async fn provision_workspace(state: &AppState, temp: &TempDir, workspace_id: &str) -> String {
    let ducklake = state.workspaces.config().ducklake.clone();
    let suffix = workspace_id.replace('-', "_");
    let metadata_schema = if ducklake.workspace_scope_mode == WorkspaceScopeMode::Isolated {
        format!("st_ws_{}", &suffix[..16.min(suffix.len())])
    } else {
        ducklake.metadata_schema.clone()
    };
    let data_path = if ducklake.workspace_scope_mode == WorkspaceScopeMode::Isolated {
        temp.path()
            .join(format!("ws_{suffix}"))
            .to_string_lossy()
            .into_owned()
    } else {
        ducklake.data_path.clone()
    };
    state
        .workspaces
        .provision_scope(ScopeProvisioningRequest {
            scope_id: workspace_id.to_string(),
            metadata_schema: metadata_schema.clone(),
            data_path,
        })
        .await
        .expect("provision workspace");
    metadata_schema
}

async fn post_otlp(router: &Router, token: &str, req: ExportTraceServiceRequest) -> StatusCode {
    let mut buf = Vec::new();
    req.encode(&mut buf).expect("encode otlp");
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(ASSERTION_HEADER, token)
                .body(Body::from(buf))
                .unwrap(),
        )
        .await
        .expect("post traces")
        .status()
}

async fn response_json(resp: axum::response::Response) -> Value {
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }))
}

async fn flush_workspace(state: &AppState, workspace_id: &str) {
    let ws = state.workspace_for_id(workspace_id).await.expect("ws");
    ws.ingest().force_flush_spans().await.expect("flush spans");
}

async fn reduce_workspace(state: &AppState, workspace_id: &str) -> usize {
    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("maintenance");
    let max_sessions = state
        .workspaces
        .config()
        .session_summary
        .max_sessions_per_reduce;
    maintenance
        .reduce_session_summary_for_key(workspace_id, max_sessions)
        .await
        .expect("reduce")
}

fn quote_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

async fn pg_count(state: &AppState, schema: &str, template: &str) -> i64 {
    let dsn = &state.workspaces.config().ducklake.metadata_path;
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("pg connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let sql = template.replace("{{schema}}", &quote_ident(schema));
    client.query_one(&sql, &[]).await.expect("pg count").get(0)
}

async fn stored_rows_for_trace(
    state: &AppState,
    workspace_id: &str,
    trace_hex: &str,
) -> Vec<(String, String, Option<String>, Option<String>)> {
    let hints = state
        .workspaces
        .scope_storage_hints(workspace_id)
        .await
        .expect("scope storage hints");
    let mut dl = state.workspaces.config().ducklake.clone();
    dl.metadata_schema = hints.metadata_schema;
    dl.data_path = hints.data_path;
    let ws = workspace_id.to_string();
    let tid = trace_hex.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = open_attached_from_config(&dl, Some(0));
        let mut stmt = conn.prepare(STORED_ROWS_SQL).expect("prepare stored rows");
        stmt.query_map([ws.as_str(), tid.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .expect("query stored rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect stored rows")
    })
    .await
    .expect("join stored_rows_for_trace")
}

#[tokio::test]
async fn sibling_spans_without_session_id_are_never_correlated_inside_thelake() {
    let secret = TEST_ASSERTION_SECRET;
    let (router, state, temp) = setup_router(secret, 0, WorkspaceScopeMode::Isolated).await;
    let ws_id = Uuid::new_v4().to_string();
    let _schema = provision_workspace(&state, &temp, &ws_id).await;
    let ingest_jwt = agent_token(secret, &ws_id, "agent-ota", "Travel OTA");
    let read_jwt = user_token(secret, &ws_id);

    let trace_bytes = [0x11u8; 16];
    let browser_span = [0x21u8; 8];
    let obi_same_batch = [0x22u8; 8];
    let obi_next_batch = [0x23u8; 8];
    let trace_hex = hex::encode(trace_bytes);
    let browser_hex = hex::encode(browser_span);
    let obi_same_hex = hex::encode(obi_same_batch);
    let obi_next_hex = hex::encode(obi_next_batch);

    let now = Utc::now();
    let now_ns = now.timestamp_nanos_opt().unwrap() as u64;

    let batch_1 = make_request(
        "demo-ota-web",
        vec![
            Span {
                trace_id: trace_bytes.to_vec(),
                span_id: browser_span.to_vec(),
                name: "GET /api/flights".to_string(),
                kind: span::SpanKind::Client as i32,
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 50_000_000,
                attributes: vec![string_kv("session.id", "sess-stateless-browser")],
                ..Default::default()
            },
            Span {
                trace_id: trace_bytes.to_vec(),
                span_id: obi_same_batch.to_vec(),
                parent_span_id: browser_span.to_vec(),
                name: "GET /api/flights".to_string(),
                kind: span::SpanKind::Server as i32,
                start_time_unix_nano: now_ns + 5_000_000,
                end_time_unix_nano: now_ns + 40_000_000,
                attributes: vec![
                    string_kv("http.request.method", "GET"),
                    string_kv("url.path", "/api/flights"),
                    string_kv(
                        "http.request.header.baggage",
                        "sp.session.id=sess-stateless-browser",
                    ),
                ],
                ..Default::default()
            },
        ],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, batch_1).await,
        StatusCode::OK
    );

    let batch_2 = make_request(
        "demo-ota-api",
        vec![Span {
            trace_id: trace_bytes.to_vec(),
            span_id: obi_next_batch.to_vec(),
            parent_span_id: obi_same_batch.to_vec(),
            name: "SELECT flights".to_string(),
            kind: span::SpanKind::Client as i32,
            start_time_unix_nano: now_ns + 10_000_000,
            end_time_unix_nano: now_ns + 25_000_000,
            status: Some(Status {
                code: 2,
                message: "db pool exhausted".to_string(),
            }),
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, batch_2).await,
        StatusCode::OK
    );
    flush_workspace(&state, &ws_id).await;

    let stored = stored_rows_for_trace(&state, &ws_id, &trace_hex).await;
    assert_eq!(stored.len(), 3);
    let by_span: std::collections::HashMap<_, _> = stored.into_iter().map(|r| (r.0, r.1)).collect();
    assert_eq!(
        by_span.get(&browser_hex).map(String::as_str),
        Some("sess-stateless-browser")
    );
    assert_eq!(
        by_span.get(&obi_same_hex).map(String::as_str),
        Some(""),
        "thelake must not parse HTTP baggage headers or correlate sibling spans in-process"
    );
    assert_eq!(
        by_span.get(&obi_next_hex).map(String::as_str),
        Some(""),
        "thelake must not cache trace_id -> session_id across batches"
    );

    let from = (now - ChronoDuration::minutes(5)).to_rfc3339();
    let to = (now + ChronoDuration::minutes(5)).to_rfc3339();
    let details_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions/details")
                .header(header::CONTENT_TYPE, "application/json")
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::from(
                    json!({
                        "version": 1,
                        "sessionId": "sess-stateless-browser",
                        "timeRange": { "from": from, "to": to },
                        "limit": 50
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("session details");
    assert_eq!(details_resp.status(), StatusCode::OK);
    let details_body = response_json(details_resp).await;
    let detail_spans = details_body["spans"].as_array().expect("spans array");
    assert_eq!(
        detail_spans.len(),
        1,
        "session details must scan session_id directly with zero cross-span trace_id join: {details_body}"
    );
    assert_eq!(detail_spans[0]["span_id"], json!(browser_hex));

    let v2_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v2/traces/{trace_hex}"))
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo v2 trace");
    assert_eq!(v2_resp.status(), StatusCode::OK);
    let v2_body = response_json(v2_resp).await;
    let mut session_attrs_by_span = std::collections::HashMap::new();
    for batch in v2_body["trace"]["resourceSpans"].as_array().unwrap() {
        for scope in batch["scopeSpans"].as_array().unwrap() {
            for sp_json in scope["spans"].as_array().unwrap() {
                let sid_b64 = sp_json["spanId"].as_str().unwrap();
                let sid_hex = hex::encode(
                    base64::engine::general_purpose::STANDARD
                        .decode(sid_b64)
                        .unwrap(),
                );
                let sp_session = sp_json["attributes"].as_array().and_then(|attrs| {
                    attrs.iter().find_map(|kv| {
                        (kv["key"] == "sp.session.id")
                            .then(|| kv["value"]["stringValue"].as_str().map(str::to_owned))
                            .flatten()
                    })
                });
                session_attrs_by_span.insert(sid_hex, sp_session);
            }
        }
    }
    assert_eq!(
        session_attrs_by_span.get(&browser_hex),
        Some(&Some("sess-stateless-browser".to_string()))
    );
    assert_eq!(
        session_attrs_by_span.get(&obi_same_hex),
        Some(&None),
        "Tempo v2 trace lookup must project each span's stored attributes without sibling backfill"
    );
    assert_eq!(session_attrs_by_span.get(&obi_next_hex), Some(&None));

    let partial_search = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces/search")
                .header(header::CONTENT_TYPE, "application/json")
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::from(
                    json!({
                        "version": 1,
                        "scope": "traces",
                        "timeRange": { "from": from, "to": to },
                        "filter": {
                            "and": [
                                { "field": "session_id", "op": "eq", "value": "sess-stateless-browser" },
                                { "field": "status_code", "op": "eq", "value": "ERROR" }
                            ]
                        },
                        "limit": 10
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("partially correlated trace search");
    assert_eq!(partial_search.status(), StatusCode::OK);
    let partial_body = response_json(partial_search).await;
    let partial_rows = partial_body["rows"].as_array().expect("rows");
    assert_eq!(partial_rows.len(), 1, "{partial_body}");
    assert_eq!(partial_rows[0]["summary"]["spanCount"], json!(3));
    assert_eq!(partial_rows[0]["summary"]["errorCount"], json!(1));
}

#[tokio::test]
async fn unassigned_spans_persist_immediately_and_remain_searchable_via_traces_and_tempo() {
    let secret = TEST_ASSERTION_SECRET;
    let (router, state, temp) = setup_router(secret, 0, WorkspaceScopeMode::Isolated).await;
    let ws_id = Uuid::new_v4().to_string();
    let schema = provision_workspace(&state, &temp, &ws_id).await;
    let ingest_jwt = agent_token(secret, &ws_id, "agent-ota", "Travel OTA");
    let read_jwt = user_token(secret, &ws_id);

    let trace_bytes = [0xa1u8; 16];
    let server_bytes = [0xb1u8; 8];
    let body_bytes = [0xb2u8; 8];
    let trace_hex = hex::encode(trace_bytes);
    let server_hex = hex::encode(server_bytes);
    let now = Utc::now();
    let now_ns = now.timestamp_nanos_opt().unwrap() as u64;

    let req = make_request(
        "demo-ota-api",
        vec![
            Span {
                trace_id: trace_bytes.to_vec(),
                span_id: server_bytes.to_vec(),
                parent_span_id: vec![0x99u8; 8],
                name: "POST /api/checkout".to_string(),
                kind: span::SpanKind::Server as i32,
                start_time_unix_nano: now_ns,
                end_time_unix_nano: now_ns + 120_000_000,
                attributes: vec![
                    string_kv("http.request.method", "POST"),
                    string_kv("url.path", "/api/checkout"),
                    int_kv("http.response.status_code", 500),
                ],
                status: Some(Status {
                    code: 2,
                    message: "gateway timeout".to_string(),
                }),
                ..Default::default()
            },
            Span {
                trace_id: trace_bytes.to_vec(),
                span_id: body_bytes.to_vec(),
                parent_span_id: server_bytes.to_vec(),
                name: "softprobe.http.body.capture".to_string(),
                kind: span::SpanKind::Internal as i32,
                start_time_unix_nano: now_ns + 10_000_000,
                end_time_unix_nano: now_ns + 110_000_000,
                attributes: vec![
                    string_kv("http.request.body", r#"{"cart":"flt_1"}"#),
                    string_kv("http.response.body", r#"{"error":"upstream_timeout"}"#),
                ],
                ..Default::default()
            },
        ],
    );

    assert_eq!(post_otlp(&router, &ingest_jwt, req).await, StatusCode::OK);
    flush_workspace(&state, &ws_id).await;

    let stored = stored_rows_for_trace(&state, &ws_id, &trace_hex).await;
    assert_eq!(stored.len(), 2);
    for (_, stored_sid, stored_attr_sid, _) in &stored {
        assert_eq!(stored_sid, "");
        assert_eq!(stored_attr_sid, &None);
    }
    let server_row = stored
        .iter()
        .find(|r| r.0 == server_hex)
        .expect("server row");
    assert_eq!(server_row.3.as_deref(), Some("/api/checkout"));

    assert_eq!(pg_count(&state, &schema, DIRTY_COUNT_SQL).await, 0);
    assert_eq!(reduce_workspace(&state, &ws_id).await, 0);
    assert_eq!(pg_count(&state, &schema, SUMMARY_COUNT_SQL).await, 0);

    let trace_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/traces/{trace_hex}"))
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get unassigned trace");
    assert_eq!(trace_resp.status(), StatusCode::OK);
    let trace_body = response_json(trace_resp).await;
    assert_eq!(trace_body["sessionId"], Value::Null);

    let from = (now - ChronoDuration::minutes(5)).to_rfc3339();
    let to = (now + ChronoDuration::minutes(5)).to_rfc3339();
    let search_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces/search")
                .header(header::CONTENT_TYPE, "application/json")
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::from(
                    json!({
                        "version": 1,
                        "scope": "traces",
                        "timeRange": { "from": from, "to": to },
                        "filter": {
                            "and": [
                                { "field": "session_id", "op": "eq", "value": "" },
                                { "field": "agent_id", "op": "eq", "value": "agent-ota" }
                            ]
                        },
                        "limit": 10
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("search unassigned traces");
    assert_eq!(search_resp.status(), StatusCode::OK);
    let search_body = response_json(search_resp).await;
    let rows = search_body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(trace_hex));
    assert_eq!(rows[0]["summary"]["traceId"], json!(trace_hex));
    assert_eq!(rows[0]["summary"]["sessionId"], Value::Null);
    assert_eq!(rows[0]["summary"]["spanCount"], json!(1));
    assert_eq!(rows[0]["summary"]["errorCount"], json!(1));
    assert_eq!(rows[0]["summary"]["agentId"], json!("agent-ota"));
    assert_eq!(rows[0]["summary"]["agentName"], json!("Travel OTA"));

    let tempo_q = "%7B%20.sp.session.id%20!~%20%22.%2B%22%20%7D";
    let tempo_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/search?q={tempo_q}&limit=10"))
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo unassigned search");
    assert_eq!(tempo_resp.status(), StatusCode::OK);
    let tempo_body = response_json(tempo_resp).await;
    let traces = tempo_body["traces"].as_array().expect("tempo traces");
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0]["traceID"], json!(trace_hex));
    assert!(traces[0]["serviceStats"].is_object());
    assert!(traces[0]["spanSet"].is_object());

    for unscoped_status_q in [
        "%7B%20.status%20%3D%20error%20%7D",
        "%7B%20.status_code%20%3D%202%20%7D",
    ] {
        let status_resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/search?q={unscoped_status_q}&limit=10"))
                    .header(ASSERTION_HEADER, &read_jwt)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("tempo unscoped status search");
        assert_eq!(status_resp.status(), StatusCode::OK);
        let status_body = response_json(status_resp).await;
        let matched = status_body["traces"].as_array().expect("tempo traces");
        assert_eq!(matched.len(), 1, "query {unscoped_status_q}: {status_body}");
        assert_eq!(matched[0]["traceID"], json!(trace_hex));
    }

    for sort_field in [
        "service.name",
        "http_request_path",
        "status_code",
        "duration_ms",
        "span_count",
        "error_count",
    ] {
        let sort_resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces/search")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(ASSERTION_HEADER, &read_jwt)
                    .body(Body::from(
                        json!({
                            "version": 1,
                            "scope": "traces",
                            "timeRange": { "from": from, "to": to },
                            "sort": [{ "field": sort_field, "direction": "desc" }],
                            "limit": 10
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("trace search sort");
        assert_eq!(
            sort_resp.status(),
            StatusCode::OK,
            "sort by {sort_field} failed"
        );
    }

    let session_scope_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces/search")
                .header(header::CONTENT_TYPE, "application/json")
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::from(
                    json!({
                        "version": 1,
                        "scope": "sessions",
                        "timeRange": { "from": from, "to": to },
                        "limit": 10
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("sessions scope search");
    assert_eq!(session_scope_resp.status(), StatusCode::OK);
    let session_scope_body = response_json(session_scope_resp).await;
    assert_eq!(
        session_scope_body["rows"]
            .as_array()
            .expect("session rows")
            .len(),
        0,
        "unassigned traces must not appear as synthetic empty sessions in scope=sessions"
    );

    for bad_payload in [
        json!({
            "version": 1,
            "scope": "traces",
            "timeRange": { "from": from, "to": to },
            "sort": [{ "field": "span_id", "direction": "desc" }],
            "limit": 10
        }),
        json!({
            "version": 1,
            "scope": "traces",
            "timeRange": { "from": from, "to": to },
            "sort": [{ "field": "trace_count", "direction": "desc" }],
            "limit": 10
        }),
    ] {
        let bad_resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces/search")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(ASSERTION_HEADER, &read_jwt)
                    .body(Body::from(bad_payload.to_string()))
                    .unwrap(),
            )
            .await
            .expect("invalid trace search payload");
        assert_eq!(
            bad_resp.status(),
            StatusCode::BAD_REQUEST,
            "payload {bad_payload}"
        );
    }

    let details_post_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces/details")
                .header(header::CONTENT_TYPE, "application/json")
                .header(ASSERTION_HEADER, &read_jwt)
                .body(Body::from(
                    json!({
                        "traceId": trace_hex,
                        "timeRange": { "from": from, "to": to },
                        "limit": 50
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("trace details post");
    assert_eq!(details_post_resp.status(), StatusCode::OK);
    let details_post_body = response_json(details_post_resp).await;
    for span_obj in details_post_body["spans"].as_array().expect("detail spans") {
        assert_eq!(span_obj["session_id"], Value::Null, "{span_obj}");
    }
}

#[tokio::test]
async fn random_sessionless_trace_flood_creates_zero_summary_rows_and_per_agent_dirty_cap_isolates_noise(
) {
    let secret = TEST_ASSERTION_SECRET;
    let (router, state, temp) = setup_router(secret, 60, WorkspaceScopeMode::Isolated).await;
    let ws_id = Uuid::new_v4().to_string();
    let schema = provision_workspace(&state, &temp, &ws_id).await;
    let noisy_jwt = agent_token(secret, &ws_id, "agent-noisy", "Noisy OBI");
    let quiet_jwt = agent_token(secret, &ws_id, "agent-quiet", "Quiet App");

    let base_ns = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    for batch_idx in 0..10u64 {
        let mut spans = Vec::with_capacity(200);
        for i in 0..200u64 {
            let seq = batch_idx * 200 + i + 1;
            let mut tid = [0u8; 16];
            tid[..8].copy_from_slice(&seq.to_be_bytes());
            tid[8..].copy_from_slice(&(seq ^ 0x55aa_55aa_55aa_55aa).to_be_bytes());
            let mut sid = [0u8; 8];
            sid.copy_from_slice(&seq.to_be_bytes());
            spans.push(Span {
                trace_id: tid.to_vec(),
                span_id: sid.to_vec(),
                name: "GET /healthz".to_string(),
                kind: span::SpanKind::Server as i32,
                start_time_unix_nano: base_ns + seq * 1_000,
                end_time_unix_nano: base_ns + seq * 1_000 + 500,
                ..Default::default()
            });
        }
        assert_eq!(
            post_otlp(&router, &noisy_jwt, make_request("health-probe", spans)).await,
            StatusCode::OK
        );
    }
    flush_workspace(&state, &ws_id).await;
    assert_eq!(pg_count(&state, &schema, DIRTY_COUNT_SQL).await, 0);
    assert_eq!(reduce_workspace(&state, &ws_id).await, 0);
    assert_eq!(pg_count(&state, &schema, SUMMARY_COUNT_SQL).await, 0);

    let mut noisy_session_spans = Vec::with_capacity(1_200);
    for i in 0..1_200u64 {
        let seq = 10_000 + i;
        let mut tid = [0u8; 16];
        tid[..8].copy_from_slice(&seq.to_be_bytes());
        let mut sid = [0u8; 8];
        sid.copy_from_slice(&seq.to_be_bytes());
        noisy_session_spans.push(Span {
            trace_id: tid.to_vec(),
            span_id: sid.to_vec(),
            name: "POST /noisy".to_string(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: base_ns + seq * 1_000,
            end_time_unix_nano: base_ns + seq * 1_000 + 500,
            attributes: vec![string_kv("session.id", &format!("noisy-sess-{i}"))],
            ..Default::default()
        });
    }
    assert_eq!(
        post_otlp(
            &router,
            &noisy_jwt,
            make_request("noisy-app", noisy_session_spans)
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        post_otlp(
            &router,
            &quiet_jwt,
            make_request(
                "quiet-app",
                vec![Span {
                    trace_id: vec![0x77; 16],
                    span_id: vec![0x88; 8],
                    name: "GET /quiet".to_string(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: base_ns + 50_000_000,
                    end_time_unix_nano: base_ns + 60_000_000,
                    attributes: vec![string_kv("session.id", "quiet-good-session")],
                    ..Default::default()
                }]
            )
        )
        .await,
        StatusCode::OK
    );
    flush_workspace(&state, &ws_id).await;

    let dirty_rows = pg_count(&state, &schema, DIRTY_COUNT_SQL).await;
    assert_eq!(
        dirty_rows, 1_025,
        "noisy Agent must cap at 1024 dirty sessions/min while quiet Agent's session still lands"
    );
}

#[tokio::test]
async fn recording_spans_validate_ascii_session_id_and_work_under_agent_and_workspace_auth() {
    let secret = TEST_ASSERTION_SECRET;
    let (router, state, temp) = setup_router(secret, 0, WorkspaceScopeMode::Isolated).await;
    let ws_id = Uuid::new_v4().to_string();
    let schema = provision_workspace(&state, &temp, &ws_id).await;
    let ingest_jwt = agent_token(secret, &ws_id, "agent-web", "Web Recorder");
    let workspace_jwt = user_token(secret, &ws_id);
    let now_ns = Utc::now().timestamp_nanos_opt().unwrap() as u64;

    let missing_sid = make_request(
        "web-app",
        vec![Span {
            trace_id: vec![0x31; 16],
            span_id: vec![0x41; 8],
            name: "softprobe.recording".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![string_kv("sp.observation.type", "recording")],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, missing_sid).await,
        StatusCode::BAD_REQUEST
    );

    let resource_only_sid = make_request_with_resource(
        vec![
            string_kv("service.name", "web-app"),
            string_kv("session.id", "resource-only-session"),
        ],
        vec![Span {
            trace_id: vec![0x32; 16],
            span_id: vec![0x42; 8],
            name: "softprobe.recording".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![string_kv("sp.observation.type", "recording")],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &workspace_jwt, resource_only_sid).await,
        StatusCode::BAD_REQUEST
    );

    let bad_ascii_sid = make_request(
        "web-app",
        vec![Span {
            trace_id: vec![0x33; 16],
            span_id: vec![0x43; 8],
            name: "softprobe.recording".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![
                string_kv("sp.observation.type", "recording"),
                string_kv("sp.session.id", "bad session\n"),
            ],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, bad_ascii_sid).await,
        StatusCode::BAD_REQUEST
    );

    let valid_agent_rec = make_request(
        "web-app",
        vec![Span {
            trace_id: vec![0x34; 16],
            span_id: vec![0x44; 8],
            name: "softprobe.recording".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![
                string_kv("sp.observation.type", "recording"),
                string_kv("sp.session.id", "sess-rec-agent"),
            ],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, valid_agent_rec).await,
        StatusCode::OK
    );

    let valid_workspace_rec = make_request(
        "web-app",
        vec![Span {
            trace_id: vec![0x35; 16],
            span_id: vec![0x45; 8],
            name: "softprobe.recording".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![
                string_kv("sp.observation.type", "recording"),
                string_kv("sp.session.id", "sess-rec-workspace"),
            ],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &workspace_jwt, valid_workspace_rec).await,
        StatusCode::OK,
        "workspace-authenticated recording spans must succeed without requiring an Agent assertion"
    );

    let invalid_trace_sid_bytes = [0x36u8; 16];
    let invalid_trace_hex = hex::encode(invalid_trace_sid_bytes);
    let invalid_trace_sid = make_request(
        "web-app",
        vec![Span {
            trace_id: invalid_trace_sid_bytes.to_vec(),
            span_id: vec![0x46; 8],
            name: "GET /bad-sid".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![string_kv("session.id", "has spaces and\tcontrol")],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, invalid_trace_sid).await,
        StatusCode::OK
    );

    let fallback_sid_bytes = [0x37u8; 16];
    let fallback_sid_hex = hex::encode(fallback_sid_bytes);
    let fallback_sid_req = make_request(
        "web-app",
        vec![Span {
            trace_id: fallback_sid_bytes.to_vec(),
            span_id: vec![0x47; 8],
            name: "GET /fallback-sid".to_string(),
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 1_000,
            attributes: vec![
                string_kv("sp.session.id", ""),
                string_kv("session.id", "sess-valid-fallback"),
            ],
            ..Default::default()
        }],
    );
    assert_eq!(
        post_otlp(&router, &ingest_jwt, fallback_sid_req).await,
        StatusCode::OK
    );
    flush_workspace(&state, &ws_id).await;

    assert_eq!(
        pg_count(&state, &schema, DIRTY_COUNT_SQL).await,
        1,
        "only the valid fallback session span may enqueue a dirty hint; recording and invalid non-ASCII session IDs must enqueue zero"
    );
    let stored = stored_rows_for_trace(&state, &ws_id, &invalid_trace_hex).await;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].1, "");

    let invalid_get = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/traces/{invalid_trace_hex}"))
                .header(ASSERTION_HEADER, &workspace_jwt)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get invalid-sid trace");
    assert_eq!(invalid_get.status(), StatusCode::OK);
    let invalid_body = response_json(invalid_get).await;
    assert_eq!(invalid_body["sessionId"], Value::Null, "{invalid_body}");

    let fallback_stored = stored_rows_for_trace(&state, &ws_id, &fallback_sid_hex).await;
    assert_eq!(fallback_stored.len(), 1);
    assert_eq!(fallback_stored[0].1, "sess-valid-fallback");
}
