//! Verify the canonical mocker-v1 promotion profile and Rolling span ingest.
//!
//! Covers HTTP bodies from `http.request` / `http.response` events (not sp_target_* attrs)
//! and typed `record_category` filters after promotion apply.
//!
//! Lifecycle (router / apply / ingest / DuckLake attach) lives in
//! [`crate::util::promotion_file_backed`]; this module keeps mocker fixtures + assertions.

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use softprobe_runtime::promotion::{
    merge_telemetry_columns_manifests, parse_promotion_manifest, telemetry_columns_manifest_to_yaml,
    PromotionManifest,
};
use std::path::PathBuf;
use uuid::Uuid;

use crate::util::otlp::{int_kv, string_kv};
use crate::util::promotion_file_backed::{
    apply_promotion_yaml, assert_traces_columns_exist, attach_softprobe_ducklake,
    ingest_otlp_protobuf, setup_file_backed_promotion_env,
};
use crate::util::sp_llm_manifests::{mocker_v1_manifest_path, sp_llm_manifest_path};

fn load_mocker_v1_manifest() -> String {
    std::fs::read_to_string(mocker_v1_manifest_path()).expect("mocker-v1 manifest readable")
}

fn load_llm_v1_manifest() -> String {
    let path = if let Ok(path) = std::env::var("SP_LLM_MANIFEST") {
        PathBuf::from(path)
    } else {
        sp_llm_manifest_path("llm-v1.yaml")
    };
    std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "Softprobe shared-schema proof requires llm-v1 at {}: {err}",
            path.display()
        )
    })
}

fn merged_mocker_manifest_yaml() -> String {
    let mocker = parse_promotion_manifest(&load_mocker_v1_manifest()).expect("mocker-v1 parses");
    let PromotionManifest::TelemetryColumns(mocker_spec) = mocker else {
        panic!("expected telemetry manifest");
    };

    let llm = parse_promotion_manifest(&load_llm_v1_manifest()).expect("llm-v1 parses");
    let PromotionManifest::TelemetryColumns(llm_spec) = llm else {
        panic!("expected llm telemetry manifest");
    };

    let merged =
        merge_telemetry_columns_manifests(&[llm_spec, mocker_spec]).expect("llm ∪ mocker merge");
    assert!(
        merged.columns.iter().any(|c| c.name == "observation_type"),
        "merged Softprobe schema must include llm observation_type"
    );
    assert!(
        merged.columns.iter().any(|c| c.name == "record_category"),
        "merged Softprobe schema must include mocker record_category"
    );
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
            // Explicit Softprobe app id must win over resource service.name.
            string_kv("sp_app_id", "travel-ota"),
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

    let env = setup_file_backed_promotion_env().await;
    apply_promotion_yaml(&env.router, &manifest_yaml).await;

    let trace_id = Uuid::new_v4();
    let trace_bytes: [u8; 16] = *trace_id.as_bytes();
    let mut body = Vec::new();
    servlet_and_httpclient_request(trace_bytes)
        .encode(&mut body)
        .expect("encode");
    ingest_otlp_protobuf(env.router.clone(), body).await;

    let connection = attach_softprobe_ducklake(&env.metadata_path, &env.data_path);
    assert_traces_columns_exist(
        &connection,
        &[
            "record_category",
            "record_operation",
            "record_id",
            "record_environment",
            "record_version",
            "http_request_body",
            "http_response_body",
        ],
    );

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

    let servlet_app_id: Option<String> = connection
        .query_row(
            &format!(
                "SELECT app_id FROM softprobe.traces \
                 WHERE trace_id = '{trace_hex}' AND record_category = 'Servlet'"
            ),
            [],
            |row| row.get(0),
        )
        .expect("servlet app_id");
    assert_eq!(
        servlet_app_id.as_deref(),
        Some("travel-ota"),
        "span sp_app_id must win over resource service.name"
    );

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
