//! Verify the canonical mocker-v1 promotion profile and Rolling span ingest.
//!
//! Covers HTTP bodies from `http.request` / `http.response` events (not sp_target_* attrs)
//! and typed `record_category` filters after promotion apply.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::json;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::promotion::{
    merge_telemetry_columns_manifests, parse_promotion_manifest, telemetry_columns_manifest_to_yaml,
    PromotionManifest,
};
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;
use crate::util::mocker_manifests::mocker_v1_manifest_path;
use crate::util::otlp::{int_kv, string_kv};
use crate::util::sp_llm_manifests::sp_llm_manifest_path;
use crate::util::tenant::inject_local_sqlite_tenant as inject_tenant;

fn load_mocker_v1_manifest() -> String {
    std::fs::read_to_string(mocker_v1_manifest_path()).expect("mocker-v1 manifest readable")
}

fn load_llm_v1_manifest_optional() -> Option<String> {
    let path = if let Ok(path) = std::env::var("SP_LLM_MANIFEST") {
        PathBuf::from(path)
    } else {
        sp_llm_manifest_path("llm-v1.yaml")
    };
    std::fs::read_to_string(path).ok()
}

fn merged_mocker_manifest_yaml() -> String {
    let mocker = parse_promotion_manifest(&load_mocker_v1_manifest()).expect("mocker-v1 parses");
    let PromotionManifest::TelemetryColumns(mocker_spec) = mocker else {
        panic!("expected telemetry manifest");
    };

    let mut manifests = vec![mocker_spec];
    if let Some(llm_yaml) = load_llm_v1_manifest_optional() {
        let llm = parse_promotion_manifest(&llm_yaml).expect("llm-v1 parses");
        let PromotionManifest::TelemetryColumns(llm_spec) = llm else {
            panic!("expected llm telemetry manifest");
        };
        manifests.insert(0, llm_spec);
    }

    let merged = merge_telemetry_columns_manifests(&manifests).expect("llm ∪ mocker merge");
    telemetry_columns_manifest_to_yaml(&merged)
}

fn mocker_span(
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    parent_span_id: Option<&[u8; 8]>,
    category: &str,
    operation: &str,
    request_body: &str,
    response_body: &str,
) -> Span {
    Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_span_id
            .map(|id| id.to_vec())
            .unwrap_or_default(),
        name: operation.to_string(),
        kind: if category == "Servlet" {
            span::SpanKind::Server as i32
        } else {
            span::SpanKind::Client as i32
        },
        start_time_unix_nano: 1_721_349_720_000_000_000,
        end_time_unix_nano: 1_721_349_721_000_000_000,
        attributes: vec![
            string_kv("sp.session.id", "sess-mocker-v1"),
            string_kv("sp_category_type", category),
            string_kv("sp_operation_name", operation),
            string_kv("sp_record_id", "rec-mocker-1"),
            int_kv("sp_record_environment", 1),
            string_kv("sp_record_version", "v1"),
            string_kv("sp_creation_time", "1721349720000"),
            string_kv("http.request.method", "POST"),
            string_kv("http.request.path", "/api/book"),
            int_kv("http.response.status_code", 200),
        ],
        events: vec![
            span::Event {
                time_unix_nano: 1_721_349_720_500_000_000,
                name: "http.request".to_string(),
                attributes: vec![string_kv("http.request.body", request_body)],
                dropped_attributes_count: 0,
            },
            span::Event {
                time_unix_nano: 1_721_349_720_900_000_000,
                name: "http.response".to_string(),
                attributes: vec![string_kv("http.response.body", response_body)],
                dropped_attributes_count: 0,
            },
        ],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn servlet_and_httpclient_request(trace_id: [u8; 16]) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "demo-ota")],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe-agent".to_string(),
                    version: "2.1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![
                    mocker_span(
                        &trace_id,
                        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                        None,
                        "Servlet",
                        "POST /api/book",
                        r#"{"route":"servlet-req"}"#,
                        r#"{"route":"servlet-res"}"#,
                    ),
                    mocker_span(
                        &trace_id,
                        &[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18],
                        Some(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
                        "HttpClient",
                        "POST http://upstream/book",
                        r#"{"route":"client-req"}"#,
                        r#"{"route":"client-res"}"#,
                    ),
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn canonical_mocker_v1_manifest_promotes_record_fields_and_http_bodies() {
    let manifest_yaml = merged_mocker_manifest_yaml();
    assert!(
        manifest_yaml.contains("record_category"),
        "merged manifest must include record_category"
    );

    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();

    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_tenant));

    let apply = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/promotions/apply")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": manifest_yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply");
    let apply_status = apply.status();
    let apply_body = apply
        .into_body()
        .collect()
        .await
        .expect("apply body")
        .to_bytes();
    assert_eq!(
        apply_status,
        StatusCode::OK,
        "apply failed: {}",
        String::from_utf8_lossy(&apply_body)
    );

    let trace_id = Uuid::new_v4();
    let trace_bytes: [u8; 16] = *trace_id.as_bytes();
    let mut body = Vec::new();
    servlet_and_httpclient_request(trace_bytes)
        .encode(&mut body)
        .expect("encode");

    let ingest = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("ingest");
    assert_eq!(ingest.status(), StatusCode::OK);

    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");

    for column in [
        "record_category",
        "record_operation",
        "record_id",
        "record_env",
        "record_version",
        "http_request_body",
        "http_response_body",
    ] {
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_catalog = 'softprobe' AND table_name = 'traces' \
                     AND column_name = '{column}'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("column exists query");
        assert!(count > 0, "expected column {column}");
    }

    let trace_hex = hex::encode(trace_bytes);
    let servlet = connection
        .query_row(
            &format!(
                "SELECT record_category, record_operation, http_request_body, http_response_body \
                 FROM softprobe.traces \
                 WHERE trace_id = '{trace_hex}' AND record_category = 'Servlet'"
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .expect("servlet row");

    assert_eq!(servlet.0.as_deref(), Some("Servlet"));
    assert_eq!(servlet.1.as_deref(), Some("POST /api/book"));
    assert!(servlet.2.as_deref().unwrap().contains("servlet-req"));
    assert!(servlet.3.as_deref().unwrap().contains("servlet-res"));

    let client = connection
        .query_row(
            &format!(
                "SELECT record_category, record_operation, http_request_body, http_response_body \
                 FROM softprobe.traces \
                 WHERE trace_id = '{trace_hex}' AND record_category = 'HttpClient'"
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .expect("httpclient row");

    assert_eq!(client.0.as_deref(), Some("HttpClient"));
    assert_eq!(client.1.as_deref(), Some("POST http://upstream/book"));
    assert!(client.2.as_deref().unwrap().contains("client-req"));
    assert!(client.3.as_deref().unwrap().contains("client-res"));

    let servlet_only_count: i64 = connection
        .query_row(
            &format!(
                "SELECT count(*) FROM softprobe.traces \
                 WHERE trace_id = '{trace_hex}' AND record_category = 'Servlet'"
            ),
            [],
            |row| row.get(0),
        )
        .expect("servlet count");
    assert_eq!(servlet_only_count, 1);

    let client_only_count: i64 = connection
        .query_row(
            &format!(
                "SELECT count(*) FROM softprobe.traces \
                 WHERE trace_id = '{trace_hex}' AND record_category = 'HttpClient'"
            ),
            [],
            |row| row.get(0),
        )
        .expect("client count");
    assert_eq!(client_only_count, 1);
}
