//! Strict storage and SQL-shape checks that replace the removed ad hoc scripts
//! (`verify_e2e.sh`, `verify_iceberg.sql`, `verify_session.sql`). Assertions run against the
//! runtime **DuckLake** query path (`traces` / `logs`), not Iceberg REST scans.
//!
//! Requirements: `integration-e2e` feature, local MinIO on the configured S3 endpoint, and
//! `tests/config/test.yaml` (see `make test-e2e`).

use crate::util::pipeline::TestPipeline;
use crate::util::poll::wait_for;
use crate::util::storage_config::load_test_config;
use chrono::Utc;
use softprobe_runtime::models::{Log as LogData, Span as SpanData};
use softprobe_runtime::query::{LogCountFilter, TraceCountFilter};
use std::collections::HashMap;
use std::time::Duration;

/// DuckLake / union view contract: non-empty counts, HTTP columns, day-of-timestamp partition,
/// and distinct session scope (replaces former ad hoc Iceberg SQL checks).
#[tokio::test]
async fn strict_trace_union_shape_ducklake_contract() {
    let config = load_test_config();

    let session_id = format!("strict-trace-{}", uuid::Uuid::new_v4());
    let trace_id = format!("strict-tr-{}", uuid::Uuid::new_v4());
    let now = Utc::now();

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.clone(),
        span_id: "strict-span-1".to_string(),
        parent_span_id: None,
        app_id: "strict-app".to_string(),
        organization_id: None,
        tenant_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "server".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: Some("GET".to_string()),
        http_request_path: Some("/api/strict-contract".to_string()),
        http_request_headers: Some(r#"{"X-Test":"1"}"#.to_string()),
        http_request_body: None,
        http_response_status_code: Some(201),
        http_response_headers: Some(r#"{"X-Resp":"ok"}"#.to_string()),
        http_response_body: None,
        status_code: None,
        status_message: None,
    };

    pipeline
        .add_spans(vec![span], 4096)
        .await
        .expect("add span");
    pipeline.force_flush_spans().await.expect("flush spans");

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let c = test_pipeline
                .query_engine()
                .count_traces(TraceCountFilter {
                    session_id: Some(session_id.clone()),
                    ..Default::default()
                })
                .await?;
            Ok(c >= 1)
        },
    )
    .await
    .expect("traces should show the flushed span");

    let row = test_pipeline
        .query_engine()
        .find_http_span(&session_id)
        .await
        .expect("detail query");
    let row = row.expect("expected one row for session");
    assert_eq!(row.request_method.as_deref(), Some("GET"));
    assert_eq!(row.request_path.as_deref(), Some("/api/strict-contract"));
    assert_eq!(row.response_status_code, Some(201));

    let partitions = test_pipeline
        .query_engine()
        .count_trace_days(&session_id)
        .await
        .expect("partition query");
    assert!(
        partitions >= 1,
        "expected at least one calendar-day partition"
    );
}

/// Mirrors former `verify_session.sql`: same `session_id` must appear in both traces and logs with
/// shared trace correlation.
#[tokio::test]
async fn strict_session_correlates_traces_and_logs() {
    let config = load_test_config();

    let session_id = format!("strict-sess-{}", uuid::Uuid::new_v4());
    let trace_id = format!("strict-tid-{}", uuid::Uuid::new_v4());
    let now = Utc::now();

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.clone(),
        span_id: "strict-span-a".to_string(),
        parent_span_id: None,
        app_id: "strict-app".to_string(),
        organization_id: None,
        tenant_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "server".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: None,
        status_message: None,
    };

    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now + chrono::Duration::milliseconds(1),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: "strict contract log".to_string(),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.clone()),
        span_id: Some("strict-span-a".to_string()),
        tenant_id: None,
        agent_id: None,
        agent_name: None,
    };

    pipeline
        .add_spans(vec![span], 4096)
        .await
        .expect("add span");
    pipeline.add_logs(vec![log], 4096).await.expect("add log");
    pipeline.force_flush_spans().await.expect("flush spans");
    pipeline.force_flush_logs().await.expect("flush logs");

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            Ok(test_pipeline
                .query_engine()
                .count_traces(TraceCountFilter {
                    session_id: Some(session_id.clone()),
                    ..Default::default()
                })
                .await?
                >= 1)
        },
    )
    .await
    .expect("traces row for session");

    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            Ok(test_pipeline
                .query_engine()
                .count_logs(LogCountFilter {
                    session_id: Some(session_id.clone()),
                    ..Default::default()
                })
                .await?
                >= 1)
        },
    )
    .await
    .expect("logs row for session");

    let cr = test_pipeline
        .query_engine()
        .count_logs(LogCountFilter {
            session_id: Some(session_id),
            trace_id: Some(trace_id),
            ..Default::default()
        })
        .await
        .expect("correlate");
    assert!(
        cr >= 1,
        "log must carry the same trace_id as the span for session-level drill-down"
    );
}
