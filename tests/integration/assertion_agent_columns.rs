//! Softprobe assertion JWT stamps `agent_id` / `agent_name` onto traces and logs.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use sha2::Sha256;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, AppState, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::runtime_api::{runtime_auth_middleware, runtime_control_routes};
use softprobe_runtime::runtime_engine::ScopeProvisioningRequest;
use softprobe_runtime::softprobe_assertion::ASSERTION_HEADER;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;

fn mint_assertion(payload: serde_json::Value, secret: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing = format!("{header}.{payload_b64}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing}.{sig}")
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn agent_trace_request(session_id: &str, start_unix_nano: u64) -> ExportTraceServiceRequest {
    let span = Span {
        trace_id: vec![0x11; 16],
        span_id: vec![0x22; 8],
        parent_span_id: vec![],
        name: "agent-run".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: start_unix_nano,
        end_time_unix_nano: start_unix_nano + 1_000_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("sp.observation.type", "agent"),
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
                attributes: vec![string_kv("service.name", "assert-agent-test")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.test".to_string(),
                    ..Default::default()
                }),
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn agent_logs_request(session_id: &str, time_unix_nano: u64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "assert-agent-test")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "softprobe.test".to_string(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("agent log".to_string())),
                    }),
                    attributes: vec![string_kv("sp.session.id", session_id)],
                    ..Default::default()
                }],
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

async fn assertion_router(secret: &str) -> (Router, AppState, TempDir) {
    std::env::set_var("SOFTPROBE_ASSERTION_HMAC_SECRET", secret);
    let temp = TempDir::new().expect("tempdir");
    let control = ControlPlaneRuntime {
        resolver: Resolver::new("http://127.0.0.1:9/", Duration::from_secs(60)),
    };
    let (router, state) = create_router(
        Arc::new(file_backed_test_config(&temp)),
        post(ingest_traces),
        Some(control),
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn_with_state(state.clone(), runtime_auth_middleware));
    (router, state, temp)
}

#[tokio::test]
async fn assertion_jwt_stamps_agent_columns_on_traces_and_logs() {
    let secret = "assert-agent-columns-secret";
    let (router, state, _temp) = assertion_router(secret).await;
    let tenant_key = "ws-assert-agent-cols";
    // Production requires an explicit `POST /v1/tenants` admin provisioning step
    // before a tenant can resolve a DuckLake scope; register it directly here.
    let ducklake = state.engines.config().ducklake.clone();
    state
        .engines
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_key.to_string(),
            metadata_schema: ducklake.metadata_schema,
            data_path: ducklake.data_path,
        })
        .await
        .expect("provision assertion tenant scope");
    let agent_id = "support-refund-agent";
    let agent_name = "Support Refund Agent";
    let session_id = "sess-assert-agent-1";
    let now = chrono::Utc::now().timestamp();
    let token = mint_assertion(
        json!({
            "iss": "softprobe-edge",
            "aud": "sp-backend",
            "sub": "agent-key",
            "tenant_key": tenant_key,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "exp": now + 600
        }),
        secret,
    );

    let now_unix_nano = (now * 1_000_000_000) as u64;
    let mut trace_buf = Vec::new();
    agent_trace_request(session_id, now_unix_nano)
        .encode(&mut trace_buf)
        .expect("encode traces");
    let trace_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(ASSERTION_HEADER, &token)
                .body(Body::from(trace_buf))
                .unwrap(),
        )
        .await
        .expect("traces");
    assert_eq!(trace_resp.status(), StatusCode::OK);

    let mut log_buf = Vec::new();
    agent_logs_request(session_id, now_unix_nano)
        .encode(&mut log_buf)
        .expect("encode logs");
    let log_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(ASSERTION_HEADER, &token)
                .body(Body::from(log_buf))
                .unwrap(),
        )
        .await
        .expect("logs");
    assert_eq!(log_resp.status(), StatusCode::OK);

    let engine = state.engine_for_id(tenant_key).await.expect("engine");
    engine.force_flush_spans().await.expect("flush spans");
    engine.force_flush_logs().await.expect("flush logs");

    // sessions/search is backed exclusively by the reduced `session_summary`
    // table; drive the same claim-dirty → reduce pipeline the leased job runs.
    let maintenance = state
        .engines
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    let summary_scope = maintenance
        .resolve_scope(tenant_key)
        .await
        .expect("maintenance scope");
    let session_cfg = &state.engines.config().session_summary;
    maintenance
        .reduce_session_summary(
            &summary_scope,
            session_cfg.max_sessions_per_reduce,
            session_cfg.max_reduce_span_seconds,
        )
        .await
        .expect("reduce_session_summary");

    let details_request = Request::builder()
        .method("POST")
        .uri("/v1/telemetry/details")
        .header(header::CONTENT_TYPE, "application/json")
        .header(ASSERTION_HEADER, &token)
        .body(Body::from(
            json!({
                "version": 1,
                "target": { "kind": "session", "id": session_id },
                "timeRange": {
                    "from": "1970-01-01T00:00:00Z",
                    "to": "2100-01-01T00:00:00Z"
                },
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    let details_resp = router
        .clone()
        .oneshot(details_request)
        .await
        .expect("telemetry details");
    let details_status = details_resp.status();
    let details = response_json(details_resp).await;
    assert_eq!(details_status, StatusCode::OK, "{details}");
    assert_eq!(details["spans"][0]["agent_id"], agent_id);
    assert_eq!(details["spans"][0]["agent_name"], agent_name);
    assert_eq!(details["logs"][0]["agent_id"], agent_id);
    assert_eq!(details["logs"][0]["agent_name"], agent_name);

    let search = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/search")
        .header(header::CONTENT_TYPE, "application/json")
        .header(ASSERTION_HEADER, &token)
        .body(Body::from(
            json!({
                "from": (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339(),
                "to": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                "agent_name": agent_name,
                "limit": 20
            })
            .to_string(),
        ))
        .unwrap();
    let search_resp = router.oneshot(search).await.expect("sessions search");
    assert_eq!(search_resp.status(), StatusCode::OK);
    let body = response_json(search_resp).await;
    let items = body["items"].as_array().expect("items");
    assert!(
        items.iter().any(|row| {
            row["session_id"] == session_id && row["agent_name"].as_str() == Some(agent_name)
        }),
        "sessions/search must return stamped agent_name: {body}"
    );
}
