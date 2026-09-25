//! Catalog.schema qualification must never elide `main`, and TWCS/maintenance
//! probes must see real rows on both main and named DuckLake schemas.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use softprobe_runtime::compaction::ActionStatus;
use softprobe_runtime::config::Config;
use softprobe_runtime::sql::maintenance::logical_table_row_count_sql;
use softprobe_runtime::storage::ducklake::open_attached_from_config;
use softprobe_runtime::workspace_scope::WorkspaceScopeMode;
use std::sync::Arc;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::string_kv;

fn span_request(
    session_id: &str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> ExportTraceServiceRequest {
    let generation = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: vec![],
        name: "chat.completions".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: 1_720_000_000_000_000_000,
        end_time_unix_nano: 1_720_000_001_000_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("gen_ai.operation.name", "chat"),
        ],
        status: Some(Status {
            message: String::new(),
            code: 0,
        }),
        ..Default::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "qualification-contract")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "qualification".into(),
                    ..Default::default()
                }),
                spans: vec![generation],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn ingest_one_span(config: Arc<Config>, session_id: &str) {
    let (router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");
    let mut buf = Vec::new();
    span_request(session_id, [0xA1; 16], [0xB1; 8])
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);
    state
        .engine_for_id("")
        .await
        .expect("engine")
        .force_flush_spans()
        .await
        .expect("flush");
}

fn assert_three_part_probe(config: &Config) {
    let alias = &config.ducklake.catalog_alias;
    let schema = &config.ducklake.metadata_schema;
    let qualified = format!("{alias}.{schema}.traces");
    let parts: Vec<_> = qualified.split('.').collect();
    assert_eq!(
        parts.len(),
        3,
        "product path must be catalog.schema.table, got {qualified}"
    );
    assert_eq!(parts[0], alias.as_str());
    assert_eq!(parts[1], schema.as_str());
    assert_eq!(parts[2], "traces");

    let conn = open_attached_from_config(&config.ducklake, config.ducklake.data_inlining_row_limit);
    let row_sql = logical_table_row_count_sql(&qualified);
    assert!(
        row_sql.contains(&format!("FROM {qualified}")),
        "probe SQL must keep three-part name: {row_sql}"
    );
    let logical_rows: i64 = conn
        .query_row(&row_sql, [], |row| row.get(0))
        .unwrap_or_else(|err| panic!("TWCS-style logical-row probe failed for {qualified}: {err}"));
    assert!(
        logical_rows >= 1,
        "qualified probe must see ingested rows on {qualified}, got {logical_rows}"
    );

    let describe_ok = conn
        .execute_batch(&format!("DESCRIBE {qualified};"))
        .is_ok();
    assert!(
        describe_ok,
        "DESCRIBE must resolve three-part {qualified} after ATTACH"
    );
}

#[tokio::test]
async fn isolated_main_schema_uses_three_part_qualification() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.metadata_schema = "main".to_string();
    config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Isolated;
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    let config = Arc::new(config);

    ingest_one_span(config.clone(), "sess-qualify-main").await;
    assert_eq!(config.ducklake.metadata_schema, "main");
    assert_three_part_probe(&config);

    let (_router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router for maintenance");
    let maintenance = state
        .engines
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    let summary = maintenance
        .run_pass(true)
        .await
        .expect("maintenance pass with compaction");
    let traces = summary
        .tables
        .iter()
        .find(|t| t.table.ends_with(".traces") || t.table == "traces")
        .unwrap_or_else(|| panic!("expected traces maintenance entry: {summary:?}"));
    assert_ne!(
        traces.compaction.status,
        ActionStatus::Failed,
        "compaction must not fail when three-part probe sees rows: {traces:?}"
    );
}

#[tokio::test]
async fn shared_named_schema_probe_sees_ingested_rows() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Shared;
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    assert_ne!(
        config.ducklake.metadata_schema, "main",
        "shared fixture must use a named schema"
    );
    let config = Arc::new(config);

    ingest_one_span(config.clone(), "sess-qualify-shared").await;
    assert_three_part_probe(&config);

    let (_router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");
    let maintenance = state
        .engines
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    let summary = maintenance
        .run_pass(true)
        .await
        .expect("shared maintenance pass");
    let traces = summary
        .tables
        .iter()
        .find(|t| t.table.ends_with(".traces"))
        .unwrap_or_else(|| panic!("expected shared traces entry: {summary:?}"));
    assert_ne!(
        traces.compaction.status,
        ActionStatus::Failed,
        "shared TWCS path must not fail when rows exist: {traces:?}"
    );
}
