//! Soft coalesce: N>0 ack-on-enqueue + force_flush then query.

use crate::util::pipeline::TestPipeline;
use crate::util::storage_config::load_test_config;
use chrono::Utc;
use softprobe_runtime::models::Log as LogData;
use std::collections::HashMap;

#[tokio::test]
async fn coalesce_force_flush_makes_logs_queryable() {
    let mut config = load_test_config();
    config.ingest.flush_interval_seconds = 60; // only force_flush should commit
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.pipeline;

    let mut attributes = HashMap::new();
    attributes.insert("logger_name".to_string(), "coalesce-test".to_string());
    let log = LogData {
        session_id: None,
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: "coalesce force_flush body".to_string(),
        attributes,
        resource_attributes: HashMap::new(),
        trace_id: Some("trace-coalesce-1".to_string()),
        span_id: Some("span-coalesce-1".to_string()),
    };

    pipeline
        .add_logs(vec![log], 128)
        .await
        .expect("enqueue logs");

    let before = test_pipeline
        .execute_query("SELECT count(*) AS c FROM logs WHERE body = 'coalesce force_flush body'")
        .await
        .expect("query before flush");
    let before_count = before.rows[0][0].as_i64().unwrap_or(-1);
    assert_eq!(
        before_count, 0,
        "rows must not be visible before force_flush"
    );

    pipeline.force_flush_logs().await.expect("force_flush");

    let after = test_pipeline
        .execute_query("SELECT count(*) AS c FROM logs WHERE body = 'coalesce force_flush body'")
        .await
        .expect("query after flush");
    let after_count = after.rows[0][0].as_i64().unwrap_or(0);
    assert_eq!(after_count, 1, "force_flush must commit coalesced logs");
}
