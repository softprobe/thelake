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
use softprobe_runtime::workspace::ScopeProvisioningRequest;
use softprobe_runtime::workspace_scope::WorkspaceScopeMode;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;

const TEST_ASSERTION_SECRET: &str = "tempo-perf-cancel-assertion-secret";

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

async fn setup_router(
    secret: &str,
    mode: WorkspaceScopeMode,
    max_connections: usize,
) -> (Router, AppState, TempDir) {
    std::env::set_var("SOFTPROBE_ASSERTION_HMAC_SECRET", secret);
    let temp = TempDir::new().expect("tempdir");
    let mut cfg: Config = file_backed_test_config(&temp);
    cfg.ingest.flush_interval_seconds = 3600;
    cfg.ducklake.workspace_scope_mode = mode;
    cfg.query.max_connections = max_connections;
    cfg.query.sql_gate = true;
    let control = ControlPlaneRuntime {
        resolver: Resolver::new("http://127.0.0.1:9/", Duration::from_secs(60)),
    };
    let (router, state) = create_router(Arc::new(cfg), Some(control))
        .await
        .expect("router");
    let router = router.layer(from_fn_with_state(state.clone(), runtime_auth_middleware));
    (router, state, temp)
}

async fn provision_workspace(state: &AppState, temp: &TempDir, workspace_id: &str) {
    let ducklake = state.workspaces.config().ducklake.clone();
    let suffix = workspace_id.replace('-', "_");
    let metadata_schema = if ducklake.workspace_scope_mode == WorkspaceScopeMode::Isolated {
        format!("tp_ws_{}", &suffix[..16.min(suffix.len())])
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
            metadata_schema,
            data_path,
        })
        .await
        .expect("provision workspace");
}

async fn ingest_otlp(
    router: &Router,
    ingest_token: &str,
    request: ExportTraceServiceRequest,
) -> StatusCode {
    let bytes = request.encode_to_vec();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(ASSERTION_HEADER, ingest_token)
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .expect("otlp ingest");
    response.status()
}

async fn get_json(router: &Router, token: &str, uri: &str) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header(ASSERTION_HEADER, token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get request");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
    (status, body)
}

async fn assert_tempo_summary_fidelity_and_windowed_tags(mode: WorkspaceScopeMode) {
    let (router, state, temp) = setup_router(TEST_ASSERTION_SECRET, mode, 2).await;
    let workspace_id = Uuid::new_v4().to_string();
    provision_workspace(&state, &temp, &workspace_id).await;

    let ingest_tok = agent_token(
        TEST_ASSERTION_SECRET,
        &workspace_id,
        "agent-checkout",
        "checkout-agent",
    );
    let read_tok = user_token(TEST_ASSERTION_SECRET, &workspace_id);

    let now = Utc::now();
    let legacy_start = (now - ChronoDuration::days(3))
        .timestamp_nanos_opt()
        .unwrap() as u64;
    let recent_base = (now - ChronoDuration::minutes(10))
        .timestamp_nanos_opt()
        .unwrap() as u64;

    let legacy_trace_id: Vec<u8> = (0..16).map(|b| 0x10 + b).collect();
    let error_trace_id: Vec<u8> = (0..16).map(|b| 0x20 + b).collect();
    let ok_trace_id: Vec<u8> = (0..16).map(|b| 0x30 + b).collect();
    let recording_trace_id: Vec<u8> = (0..16).map(|b| 0x40 + b).collect();

    let error_trace_hex = hex::encode(&error_trace_id);
    let ok_trace_hex = hex::encode(&ok_trace_id);
    let recording_trace_hex = hex::encode(&recording_trace_id);

    let legacy_req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "legacy-svc"),
                    string_kv("legacy.resource.attr", "old-res"),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "legacy.scope".into(),
                    version: "0.1.0".into(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: legacy_trace_id,
                    span_id: vec![1, 1, 1, 1, 1, 1, 1, 1],
                    name: "GET /legacy".into(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: legacy_start,
                    end_time_unix_nano: legacy_start + 50_000_000,
                    attributes: vec![string_kv("legacy.only.attr", "v1")],
                    events: vec![span::Event {
                        time_unix_nano: legacy_start + 10_000_000,
                        name: "legacy.ev".into(),
                        attributes: vec![string_kv("legacy.event.attr", "ev1")],
                        dropped_attributes_count: 0,
                    }],
                    status: Some(Status {
                        code: 2,
                        message: "legacy-fail".into(),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    assert_eq!(
        ingest_otlp(&router, &ingest_tok, legacy_req).await,
        StatusCode::OK
    );

    let large_body = format!("PAYLOAD_MARKER_48KB:{}", "x".repeat(48 * 1024));
    let recent_req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("deployment.environment", "staging"),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "obi.http".into(),
                    version: "1.0.0".into(),
                    ..Default::default()
                }),
                spans: vec![
                    Span {
                        trace_id: error_trace_id,
                        span_id: vec![2, 2, 2, 2, 2, 2, 2, 2],
                        parent_span_id: vec![9, 9, 9, 9, 9, 9, 9, 9],
                        name: "POST /checkout".into(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: recent_base,
                        end_time_unix_nano: recent_base + 250_000_000,
                        attributes: vec![
                            string_kv("http.request.method", "POST"),
                            string_kv("http.response.status_code", "502"),
                            string_kv("http.request.body.content", &large_body),
                            string_kv("sp.session.id", "sess-checkout-1"),
                        ],
                        events: vec![span::Event {
                            time_unix_nano: recent_base + 100_000_000,
                            name: "exception".into(),
                            attributes: vec![string_kv("exception.type", "GatewayTimeout")],
                            dropped_attributes_count: 0,
                        }],
                        links: vec![span::Link {
                            trace_id: ok_trace_id.clone(),
                            span_id: vec![3, 3, 3, 3, 3, 3, 3, 3],
                            attributes: vec![string_kv("link.kind", "upstream-retry")],
                            ..Default::default()
                        }],
                        status: Some(Status {
                            code: 2,
                            message: "upstream 502".into(),
                        }),
                        ..Default::default()
                    },
                    Span {
                        trace_id: ok_trace_id,
                        span_id: vec![3, 3, 3, 3, 3, 3, 3, 3],
                        name: "GET /catalog".into(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: recent_base + 1_000_000_000,
                        end_time_unix_nano: recent_base + 1_120_000_000,
                        attributes: vec![
                            string_kv("http.request.method", "GET"),
                            string_kv("http.response.status_code", "200"),
                            string_kv("status", "error"),
                        ],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        ..Default::default()
                    },
                    Span {
                        trace_id: recording_trace_id,
                        span_id: vec![4, 4, 4, 4, 4, 4, 4, 4],
                        name: "rrweb.chunk".into(),
                        kind: span::SpanKind::Internal as i32,
                        start_time_unix_nano: recent_base + 2_000_000_000,
                        end_time_unix_nano: recent_base + 2_300_000_000,
                        attributes: vec![
                            string_kv("sp.observation.type", "recording"),
                            string_kv("sp.session.id", "sess-checkout-1"),
                        ],
                        status: Some(Status {
                            code: 2,
                            message: String::new(),
                        }),
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    assert_eq!(
        ingest_otlp(&router, &ingest_tok, recent_req).await,
        StatusCode::OK
    );
    let ws = state.workspace_for_id(&workspace_id).await.expect("ws");
    ws.ingest().force_flush_spans().await.expect("flush spans");

    let recent_start_sec = (now - ChronoDuration::hours(1)).timestamp();
    let recent_end_sec = (now + ChronoDuration::minutes(5)).timestamp();

    let (status, err_search) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20status%20%3D%20error%20%7D&minDuration=100ms&start={recent_start_sec}&end={recent_end_sec}&limit=20"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "err_search={err_search}");
    let traces = err_search["traces"].as_array().expect("traces");
    assert_eq!(traces.len(), 1, "expected only error_trace: {err_search}");
    assert_eq!(traces[0]["traceID"], error_trace_hex);
    assert_eq!(
        traces[0]["serviceStats"]["checkout-api"]["errorCount"],
        json!(1)
    );
    let summary_attrs = traces[0]["spanSet"]["spans"][0]["attributes"]
        .as_array()
        .expect("summary attributes");
    assert!(
        summary_attrs.iter().any(|a| {
            a["key"] == "sp.orphan_parent_span_id"
                && a["value"]["stringValue"] == "0909090909090909"
        }),
        "expected orphan parent marker in summary: {summary_attrs:?}"
    );
    assert!(
        !summary_attrs
            .iter()
            .any(|a| a["key"] == "http.request.body.content"),
        "48KB body must not bloat summary spanSet: {summary_attrs:?}"
    );

    let (status, ok_search) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20status%20%3D%20ok%20%7D&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ok_search["traces"].as_array().unwrap().len(), 1);
    assert_eq!(ok_search["traces"][0]["traceID"], ok_trace_hex);

    let (status, scoped_status) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20.status%20%3D%20%22error%22%20%26%26%20.http.request.method%20%3D%20%22GET%22%20%7D&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(scoped_status["traces"][0]["traceID"], ok_trace_hex);

    let (status, body_search) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20.http.request.body.content%20%3D~%20%22PAYLOAD_MARKER_48KB%22%20%7D&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body_search["traces"].as_array().unwrap().len(), 1);
    assert_eq!(body_search["traces"][0]["traceID"], error_trace_hex);

    let (status, full_v2) = get_json(
        &router,
        &read_tok,
        &format!("/api/v2/traces/{error_trace_hex}?start={recent_start_sec}&end={recent_end_sec}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "full_v2={full_v2}");
    let scope_span = &full_v2["trace"]["resourceSpans"][0]["scopeSpans"][0];
    assert_eq!(scope_span["scope"]["name"], "obi.http");
    assert_eq!(scope_span["scope"]["version"], "1.0.0");
    let full_span = &scope_span["spans"][0];
    assert_eq!(full_span["events"][0]["name"], "exception");
    assert_eq!(full_span["links"][0]["attributes"][0]["key"], "link.kind");
    let full_body_attr = full_span["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["key"] == "http.request.body.content")
        .expect("full 48KB body preserved on v2 trace lookup");
    assert_eq!(
        full_body_attr["value"]["stringValue"]
            .as_str()
            .unwrap()
            .len(),
        large_body.len()
    );

    let (status, inst_res) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20instrumentation.name%20%3D%20%22obi.http%22%20%26%26%20instrumentation.version%20%3D%20%221.0.0%22%20%7D&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(inst_res["traces"].as_array().unwrap().len(), 2);

    let (status, tag_search_res) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?tags=service.name%3Dcheckout-api%20http.request.method%3DPOST%20deployment.environment%3Dstaging&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tag_search_res["traces"].as_array().unwrap().len(), 1);
    assert_eq!(tag_search_res["traces"][0]["traceID"], error_trace_hex);

    let (status, rec_res) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20.sp.observation.type%20%3D%20%22recording%22%20%7D&start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rec_res["traces"][0]["traceID"], recording_trace_hex);

    let (status, recent_tags) = get_json(
        &router,
        &read_tok,
        &format!("/api/search/tags?start={recent_start_sec}&end={recent_end_sec}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recent_tags={recent_tags}");
    let recent_tag_list = recent_tags["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert!(recent_tag_list.contains(&"http.request.method"));
    assert!(recent_tag_list.contains(&"deployment.environment"));
    assert!(recent_tag_list.contains(&"service.name"));
    assert!(
        !recent_tag_list.contains(&"legacy.only.attr"),
        "bounded /api/search/tags must exclude 3-day-old span tags: {recent_tag_list:?}"
    );
    assert!(
        !recent_tag_list.contains(&"legacy.event.attr"),
        "bounded /api/search/tags must exclude 3-day-old event tags: {recent_tag_list:?}"
    );
    assert!(
        !recent_tag_list.contains(&"legacy.resource.attr"),
        "bounded /api/search/tags must exclude 3-day-old resource tags: {recent_tag_list:?}"
    );

    let (status, all_tags) = get_json(&router, &read_tok, "/api/search/tags").await;
    assert_eq!(status, StatusCode::OK);
    let all_tag_list = all_tags["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert!(all_tag_list.contains(&"legacy.only.attr"));
    assert!(all_tag_list.contains(&"legacy.event.attr"));

    let (status, empty_window_tags) = get_json(
        &router,
        &read_tok,
        &format!("/api/search/tags?start={recent_start_sec}&end={recent_start_sec}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty_window_tags["tagNames"], json!([]));

    let (status, recent_legacy_vals) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search/tag/legacy.only.attr/values?start={recent_start_sec}&end={recent_end_sec}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recent_legacy_vals["tagValues"], json!([]));

    let (status, all_legacy_vals) = get_json(
        &router,
        &read_tok,
        "/api/search/tag/legacy.only.attr/values",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(all_legacy_vals["tagValues"], json!(["v1"]));

    let (status, _) = get_json(
        &router,
        &read_tok,
        &format!("/api/search/tags?start={recent_end_sec}&end={recent_start_sec}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, unknown_body) = get_json(&router, &read_tok, "/api/search/tags?unknown=1").await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(unknown_body["softprobe_code"], "unsupported_feature");
}

#[tokio::test]
async fn tempo_summary_fidelity_and_windowed_tags_isolated() {
    assert_tempo_summary_fidelity_and_windowed_tags(WorkspaceScopeMode::Isolated).await;
}

#[tokio::test]
async fn tempo_summary_fidelity_and_windowed_tags_shared() {
    assert_tempo_summary_fidelity_and_windowed_tags(WorkspaceScopeMode::Shared).await;
}

async fn assert_tempo_deadline_and_http_burst_cancellation(mode: WorkspaceScopeMode) {
    use softprobe_runtime::authn::WorkspaceAuth;
    use softprobe_runtime::compat::backends::ducklake_traces::DuckLakeTraceBackend;
    use softprobe_runtime::compat::backends::traces::{TraceQueryBackend, TraceSearchRequest};
    use softprobe_runtime::compat::errors::CompatErrorCode;
    use softprobe_runtime::compat::tempo::traceql::parse_traceql;
    use softprobe_runtime::compat::workspace::{
        CompatWorkspaceContext, ProtocolScope, QueryLimits,
    };
    use softprobe_runtime::query::self_heal_snapshot;
    use std::collections::BTreeMap;
    use std::time::Instant;

    let (router, state, temp) = setup_router(TEST_ASSERTION_SECRET, mode, 1).await;
    let workspace_id = Uuid::new_v4().to_string();
    provision_workspace(&state, &temp, &workspace_id).await;

    let ingest_tok = agent_token(
        TEST_ASSERTION_SECRET,
        &workspace_id,
        "agent-burst",
        "burst-agent",
    );
    let read_tok = user_token(TEST_ASSERTION_SECRET, &workspace_id);

    let now = Utc::now();
    let base_ns = (now - ChronoDuration::minutes(5))
        .timestamp_nanos_opt()
        .unwrap() as u64;
    let heavy_payload = "abcdefghij0123456789".repeat(3200);

    let mut spans = Vec::with_capacity(451);
    for idx in 0u64..450 {
        let mut tid = [0u8; 16];
        tid[0..8].copy_from_slice(&(idx + 1).to_be_bytes());
        tid[8..16].copy_from_slice(&(idx + 1000).to_be_bytes());
        let mut sid = [0u8; 8];
        sid.copy_from_slice(&(idx + 1).to_be_bytes());
        spans.push(Span {
            trace_id: tid.to_vec(),
            span_id: sid.to_vec(),
            name: "POST /bulk".into(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: base_ns + idx * 1_000_000,
            end_time_unix_nano: base_ns + idx * 1_000_000 + 20_000_000,
            attributes: vec![string_kv("http.request.body.content", &heavy_payload)],
            status: Some(Status {
                code: 1,
                message: String::new(),
            }),
            ..Default::default()
        });
    }

    let target_trace_id: Vec<u8> = (0..16).map(|b| 0x90 + b).collect();
    let target_trace_hex = hex::encode(&target_trace_id);
    spans.push(Span {
        trace_id: target_trace_id,
        span_id: vec![9, 8, 7, 6, 5, 4, 3, 2],
        name: "POST /orders".into(),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: base_ns + 600_000_000,
        end_time_unix_nano: base_ns + 850_000_000,
        attributes: vec![string_kv("http.response.status_code", "500")],
        status: Some(Status {
            code: 2,
            message: "internal error".into(),
        }),
        ..Default::default()
    });

    for chunk in spans.chunks(20) {
        let batch = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "orders-api")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: chunk.to_vec(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        assert_eq!(
            ingest_otlp(&router, &ingest_tok, batch).await,
            StatusCode::OK
        );
    }
    let ws = state.workspace_for_id(&workspace_id).await.expect("ws");
    ws.ingest().force_flush_spans().await.expect("flush spans");

    let start_sec = (now - ChronoDuration::hours(1)).timestamp();
    let end_sec = (now + ChronoDuration::minutes(5)).timestamp();
    let start_ns = start_sec * 1_000_000_000;
    let end_ns = end_sec * 1_000_000_000;

    let heal_before = self_heal_snapshot();

    let backend = DuckLakeTraceBackend::new(ws.query().clone());
    let auth = WorkspaceAuth {
        workspace_id: workspace_id.clone(),
        bucket_name: "test-bucket".into(),
        dataset_id: "test-dataset".into(),
        agent_id: None,
        agent_name: None,
    };
    let mut expired_ctx = CompatWorkspaceContext::from_authenticated(
        auth,
        ProtocolScope::Tempo,
        None,
        QueryLimits::default(),
    )
    .expect("ctx");
    expired_ctx.deadline = Instant::now();
    let deadline_err = backend
        .search(
            &expired_ctx,
            TraceSearchRequest {
                tags: BTreeMap::new(),
                selector: Some(parse_traceql("{ status = error }").unwrap()),
                min_duration_ns: Some(100_000_000),
                max_duration_ns: None,
                start_ns: Some(start_ns),
                end_ns: Some(end_ns),
                limit: 20,
            },
        )
        .await
        .expect_err("zero remaining deadline must fail immediately");
    assert_eq!(deadline_err.code, CompatErrorCode::LimitExceeded);

    for burst_idx in 0..5 {
        let r = router.clone();
        let tok = read_tok.clone();
        let uri = format!(
            "/api/search?q=%7B%20.http.request.body.content%20%3D~%20%22.*abcdefghij.*0123456789.*miss_{burst_idx}%22%20%7D&start={start_sec}&end={end_sec}&limit=20"
        );
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .header(ASSERTION_HEADER, tok)
            .body(Body::empty())
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(5), r.oneshot(req)).await;
    }

    let sixth_started = Instant::now();
    let (status, sixth_body) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?q=%7B%20status%20%3D%20error%20%7D&minDuration=100ms&start={start_sec}&end={end_sec}&limit=20"
        ),
    )
    .await;
    let sixth_elapsed = sixth_started.elapsed();
    assert_eq!(status, StatusCode::OK, "sixth_body={sixth_body}");
    assert_eq!(sixth_body["traces"].as_array().unwrap().len(), 1);
    assert_eq!(sixth_body["traces"][0]["traceID"], target_trace_hex);
    assert!(
        sixth_elapsed < Duration::from_secs(2),
        "surviving 6th request after 5 aborted HTTP requests took too long in {mode:?}: {sixth_elapsed:?}"
    );

    for iter in 0..10 {
        let r = router.clone();
        let tok = read_tok.clone();
        let abort_uri = format!(
            "/api/search?q=%7B%20.http.request.body.content%20%3D~%20%22.*abcdefghij.*0123456789.*loop_{iter}%22%20%7D&start={start_sec}&end={end_sec}&limit=20"
        );
        let abort_req = Request::builder()
            .method("GET")
            .uri(abort_uri)
            .header(ASSERTION_HEADER, tok)
            .body(Body::empty())
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(2), r.oneshot(abort_req)).await;

        let (ok_status, ok_body) = get_json(
            &router,
            &read_tok,
            &format!(
                "/api/search?q=%7B%20status%20%3D%20error%20%7D&minDuration=100ms&start={start_sec}&end={end_sec}&limit=20"
            ),
        )
        .await;
        assert_eq!(
            ok_status,
            StatusCode::OK,
            "consecutive query {iter} after abort must not inherit a stale interrupt in {mode:?}: {ok_body}"
        );
        assert_eq!(ok_body["traces"].as_array().unwrap().len(), 1);
    }

    let (sum_status, sum_body) = get_json(
        &router,
        &read_tok,
        &format!("/api/search?start={start_sec}&end={end_sec}&limit=5"),
    )
    .await;
    assert_eq!(sum_status, StatusCode::OK, "sum_body={sum_body}");
    let sum_traces = sum_body["traces"].as_array().unwrap();
    assert!(!sum_traces.is_empty());
    let has_heavy_in_summary = sum_traces.iter().any(|trace| {
        trace["spanSet"]["spans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|sp| {
                sp["attributes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|attr| attr["key"] == "http.request.body.content")
            })
    });
    assert!(
        !has_heavy_in_summary,
        "summary /api/search must omit oversized http.request.body.content: {sum_body}"
    );

    let (heavy_tag_status, heavy_tag_body) = get_json(
        &router,
        &read_tok,
        &format!(
            "/api/search?tags=http.request.body.content%3Dabcdefghij&start={start_sec}&end={end_sec}&limit=5"
        ),
    )
    .await;
    assert_eq!(heavy_tag_status, StatusCode::OK, "heavy_tag_body={heavy_tag_body}");
    assert!(
        !heavy_tag_body["traces"].as_array().unwrap().is_empty(),
        "filtered /api/search?tags=... must preserve referenced >4KB keys so matching traces are not dropped"
    );

    let mut bulk_trace_bytes = [0u8; 16];
    bulk_trace_bytes[0..8].copy_from_slice(&1u64.to_be_bytes());
    bulk_trace_bytes[8..16].copy_from_slice(&1000u64.to_be_bytes());
    let bulk_trace_hex = hex::encode(bulk_trace_bytes);
    let (full_status, full_body) = get_json(
        &router,
        &read_tok,
        &format!("/api/v2/traces/{bulk_trace_hex}"),
    )
    .await;
    assert_eq!(full_status, StatusCode::OK);
    let full_has_heavy = full_body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|attr| {
            attr["key"] == "http.request.body.content"
                && attr["value"]["stringValue"]
                    .as_str()
                    .is_some_and(|v| v.len() == heavy_payload.len())
        });
    assert!(
        full_has_heavy,
        "full trace lookup must preserve the full 64 KB attribute body"
    );

    let heal_after = self_heal_snapshot();
    assert_eq!(
        heal_after.rebuilds, heal_before.rebuilds,
        "aborted HTTP burst must not poison or rebuild DuckDB slots in {mode:?}"
    );
    assert_eq!(heal_after.consecutive_failures, 0);
}

#[tokio::test]
async fn tempo_query_deadline_and_http_burst_cancellation_isolated() {
    assert_tempo_deadline_and_http_burst_cancellation(WorkspaceScopeMode::Isolated).await;
}

#[tokio::test]
async fn tempo_query_deadline_and_http_burst_cancellation_shared() {
    assert_tempo_deadline_and_http_burst_cancellation(WorkspaceScopeMode::Shared).await;
}
