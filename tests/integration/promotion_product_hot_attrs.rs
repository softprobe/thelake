//! Apply shipped product-hot promotion manifests and verify ingest fills columns.
//!
//! Manifests live under `docs/promotion/` and are what grafana-up / bench apply.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use uuid::Uuid;

use crate::util::otlp::{double_kv, int_kv, string_kv};
use crate::util::promotion_file_backed::{
    apply_promotion_yaml, assert_logs_columns_exist, assert_traces_columns_exist,
    attach_softprobe_ducklake, ingest_otlp_logs_protobuf, ingest_otlp_protobuf,
    setup_file_backed_promotion_env,
};

fn traces_manifest() -> &'static str {
    include_str!("../../docs/promotion/traces-query-hot-attrs.yaml")
}

fn logs_manifest() -> &'static str {
    include_str!("../../docs/promotion/logs-query-hot-attrs.yaml")
}

fn generation_request(session_id: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "llm-gateway")],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.llm".to_string(),
                    version: "0.1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: Uuid::new_v4().as_bytes().to_vec(),
                    span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
                    name: "chat.completions".to_string(),
                    kind: span::SpanKind::Client as i32,
                    start_time_unix_nano: 1_721_349_720_000_000_000,
                    end_time_unix_nano: 1_721_349_721_000_000_000,
                    attributes: vec![
                        string_kv("sp.session.id", session_id),
                        string_kv("sp.observation.type", "generation"),
                        string_kv("sp.user.id", "user-product-1"),
                        string_kv("gen_ai.provider.name", "openai"),
                        string_kv("gen_ai.request.model", "gpt-4o"),
                        int_kv("gen_ai.usage.input_tokens", 11),
                        int_kv("gen_ai.usage.output_tokens", 22),
                        int_kv("gen_ai.usage.total_tokens", 33),
                        double_kv("sp.cost.total", 0.003),
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
    }
}

fn log_request(session_id: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout"),
                    string_kv("deployment.environment", "staging"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_721_349_720_000_000_000,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("hello product hot".into())),
                    }),
                    attributes: vec![
                        string_kv("logger_name", "com.softprobe.checkout"),
                        string_kv("sp.session.id", session_id),
                        string_kv("sp.user.id", "user-product-1"),
                    ],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn product_traces_hot_attrs_manifest_promotes_on_ingest() {
    let env = setup_file_backed_promotion_env().await;
    apply_promotion_yaml(&env.router, traces_manifest()).await;

    let session_id = format!("sess-product-traces-{}", Uuid::new_v4());
    let mut body = Vec::new();
    generation_request(&session_id)
        .encode(&mut body)
        .expect("encode");
    ingest_otlp_protobuf(env.router.clone(), body).await;

    let connection = attach_softprobe_ducklake(&env.metadata_path, &env.data_path);
    assert_traces_columns_exist(
        &connection,
        &[
            "observation_type",
            "model_name",
            "model_provider",
            "user_id",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "total_cost",
            "session_attr_id",
            "service_name",
        ],
    );

    let sql = format!(
        "SELECT observation_type, model_name, model_provider, user_id, \
                input_tokens, output_tokens, total_tokens, total_cost, \
                session_attr_id, service_name \
         FROM softprobe.traces WHERE session_attr_id = '{}' OR session_id = '{}'",
        session_id.replace('\'', "''"),
        session_id.replace('\'', "''"),
    );
    let row = connection
        .query_row(&sql, [], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<f64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .expect("promoted row");
    assert_eq!(row.0.as_deref(), Some("generation"));
    assert_eq!(row.1.as_deref(), Some("gpt-4o"));
    assert_eq!(row.2.as_deref(), Some("openai"));
    assert_eq!(row.3.as_deref(), Some("user-product-1"));
    assert_eq!(row.4, Some(11));
    assert_eq!(row.5, Some(22));
    assert_eq!(row.6, Some(33));
    assert!((row.7.unwrap_or(0.0) - 0.003).abs() < 1e-9);
    assert_eq!(row.8.as_deref(), Some(session_id.as_str()));
    assert_eq!(row.9.as_deref(), Some("llm-gateway"));
}

#[tokio::test]
async fn product_logs_hot_attrs_manifest_promotes_on_ingest() {
    let env = setup_file_backed_promotion_env().await;
    apply_promotion_yaml(&env.router, logs_manifest()).await;

    let session_id = format!("sess-product-logs-{}", Uuid::new_v4());
    let mut body = Vec::new();
    log_request(&session_id).encode(&mut body).expect("encode");
    ingest_otlp_logs_protobuf(env.router.clone(), body).await;

    let connection = attach_softprobe_ducklake(&env.metadata_path, &env.data_path);
    assert_logs_columns_exist(
        &connection,
        &[
            "logger_name",
            "service_name",
            "deployment_environment",
            "session_attr_id",
            "user_id",
        ],
    );

    let sql = format!(
        "SELECT logger_name, service_name, deployment_environment, session_attr_id, user_id \
         FROM softprobe.logs WHERE session_attr_id = '{}' OR body LIKE '%product hot%'",
        session_id.replace('\'', "''"),
    );
    let row = connection
        .query_row(&sql, [], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .expect("promoted log row");
    assert_eq!(row.0.as_deref(), Some("com.softprobe.checkout"));
    assert_eq!(row.1.as_deref(), Some("checkout"));
    assert_eq!(row.2.as_deref(), Some("staging"));
    assert_eq!(row.3.as_deref(), Some(session_id.as_str()));
    assert_eq!(row.4.as_deref(), Some("user-product-1"));
}
