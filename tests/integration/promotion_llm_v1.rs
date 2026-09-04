//! Verify simulated LLM generation promotion (attribute → traces columns).
//!
//! Fixture YAML lives under `tests/fixtures/promotion/` so CI is self-contained.
//! Lifecycle (router / apply / ingest / DuckLake attach) lives in
//! [`crate::util::promotion_file_backed`].

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use uuid::Uuid;

use crate::util::otlp::{double_kv, int_kv, string_kv};
use crate::util::promotion_file_backed::{
    apply_promotion_yaml, assert_traces_columns_exist, attach_softprobe_ducklake,
    ingest_otlp_protobuf, setup_file_backed_promotion_env,
};
use crate::util::promotion_fixtures::LLM_GENERATION_V1_YAML;

fn generation_request(session_id: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "llm-gateway"),
                    string_kv("deployment.environment.name", "staging"),
                    string_kv("service.version", "1.2.3"),
                ],
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
                        string_kv("sp.user.id", "user-promo-1"),
                        string_kv("gen_ai.provider.name", "openai"),
                        string_kv("gen_ai.request.model", "gpt-4o"),
                        string_kv("gen_ai.operation.name", "chat"),
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

#[tokio::test]
async fn simulated_llm_generation_manifest_promotes_generation_fields() {
    let manifest_yaml = LLM_GENERATION_V1_YAML;
    assert!(
        manifest_yaml.contains("observation_type"),
        "unexpected llm generation fixture contents"
    );

    let env = setup_file_backed_promotion_env().await;
    apply_promotion_yaml(&env.router, manifest_yaml).await;

    let session_id = format!("sess-llm-gen-{}", Uuid::new_v4());
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
            "environment",
            "release",
        ],
    );

    let sql = format!(
        "SELECT observation_type, model_name, model_provider, user_id, \
                input_tokens, output_tokens, total_tokens, total_cost, \
                environment, release \
         FROM softprobe.traces WHERE session_id = '{}'",
        session_id.replace('\'', "''")
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
        .expect("query promoted generation");

    assert_eq!(row.0.as_deref(), Some("generation"));
    assert_eq!(row.1.as_deref(), Some("gpt-4o"));
    assert_eq!(row.2.as_deref(), Some("openai"));
    assert_eq!(row.3.as_deref(), Some("user-promo-1"));
    assert_eq!(row.4, Some(11));
    assert_eq!(row.5, Some(22));
    assert_eq!(row.6, Some(33));
    assert_eq!(row.7, Some(0.003));
    assert_eq!(row.8.as_deref(), Some("staging"));
    assert_eq!(row.9.as_deref(), Some("1.2.3"));
}
