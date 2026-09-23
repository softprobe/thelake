//! Contract test: After warm bootstrap, N consecutive writes perform zero
//! DESCRIBE / partition-info / sort-info probes (Issue #51).

use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::config::DuckLakeConfig;
use softprobe_runtime::models::{Log as LogData, Span as SpanData};
use softprobe_runtime::query;
use softprobe_runtime::runtime_engine::{RuntimeEngine, RuntimeEngineManager};
use softprobe_runtime::storage::schema::{
    describe_probe_count, partition_sort_probe_count, total_schema_probe_count,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;

static HOTPATH_CONTRACT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn sample_span(i: usize, tenant_id: Option<&str>) -> SpanData {
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("test.iteration".to_string(), i.to_string());
    SpanData {
        session_id: format!("sess-{i}"),
        trace_id: format!("trace-{i:016x}"),
        span_id: format!("span-{i:016x}"),
        parent_span_id: None,
        app_id: "hotpath-app".to_string(),
        organization_id: None,
        tenant_id: tenant_id.map(|s| s.to_string()),
        agent_id: None,
        agent_name: None,
        message_type: "chat".to_string(),
        span_kind: Some("INTERNAL".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".to_string()),
        status_message: None,
    }
}

fn sample_log(i: usize) -> LogData {
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("log.iteration".to_string(), i.to_string());
    LogData {
        session_id: Some(format!("sess-{i}")),
        timestamp: now,
        observed_timestamp: Some(now),
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: format!("log message {i}"),
        attributes,
        resource_attributes: HashMap::new(),
        trace_id: Some(format!("trace-{i:016x}")),
        span_id: Some(format!("span-{i:016x}")),
        tenant_id: None,
        agent_id: None,
        agent_name: None,
    }
}

async fn assert_warm_writes_zero_probes_contract(
    runtime: &RuntimeEngine,
    query_dk: DuckLakeConfig,
    tenant_id: Option<&str>,
) {
    let _guard = HOTPATH_CONTRACT_LOCK.lock().await;

    // Perform one initial write across signals to ensure cold paths / pool creation are complete.
    runtime
        .add_spans(vec![sample_span(0, tenant_id)], 0)
        .await
        .expect("warm span write");
    runtime
        .add_logs(vec![sample_log(0)], 0)
        .await
        .expect("warm log write");

    // Record baseline probe count after warm bootstrap.
    let desc_before = describe_probe_count();
    let part_before = partition_sort_probe_count();
    let total_before = total_schema_probe_count();

    const N: usize = 5;
    for i in 1..=N {
        runtime
            .add_spans(vec![sample_span(i, tenant_id)], 0)
            .await
            .unwrap_or_else(|e| panic!("span write {i} failed: {e}"));
        runtime
            .add_logs(vec![sample_log(i)], 0)
            .await
            .unwrap_or_else(|e| panic!("log write {i} failed: {e}"));
    }

    let desc_after = describe_probe_count();
    let part_after = partition_sort_probe_count();
    let total_after = total_schema_probe_count();

    let desc_delta = desc_after - desc_before;
    let part_delta = part_after - part_before;
    let total_delta = total_after - total_before;

    assert_eq!(
        desc_delta, 0,
        "DESCRIBE probes during {N} warm writes: expected 0, got {desc_delta}"
    );
    assert_eq!(
        part_delta, 0,
        "partition/sort catalog probes during {N} warm writes: expected 0, got {part_delta}"
    );
    assert_eq!(
        total_delta, 0,
        "total schema probes during {N} warm writes: expected 0, got {total_delta}"
    );

    // Verify all rows were committed and queryable through the query engine.
    let mut query_config = Config {
        ducklake: query_dk,
        ..Config::default()
    };
    query_config.shrink_pools_for_tests();
    let query_engine = query::create_query_engine(&query_config)
        .await
        .expect("query engine");
    let span_result = query_engine
        .execute_query_uninstrumented(
            "SELECT count(*) FROM traces \
             WHERE timestamp >= TIMESTAMP '1970-01-01' \
               AND timestamp < TIMESTAMP '2100-01-01'",
        )
        .await
        .expect("query traces");
    let span_n = span_result.rows[0][0].as_i64().expect("trace count");
    assert_eq!(span_n, (N + 1) as i64, "all traces must be committed");

    let log_result = query_engine
        .execute_query_uninstrumented(
            "SELECT count(*) FROM logs \
             WHERE timestamp >= TIMESTAMP '1970-01-01' \
               AND timestamp < TIMESTAMP '2100-01-01'",
        )
        .await
        .expect("query logs");
    let log_n = log_result.rows[0][0].as_i64().expect("log count");
    assert_eq!(log_n, (N + 1) as i64, "all logs must be committed");
}

#[tokio::test]
#[ignore = "global DESCRIBE probe counter races other tests under --test-threads>1; run alone to verify"]
async fn warm_writes_perform_zero_schema_probes_postgres() {
    let pg_host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let pg_port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".to_string());
    let conn_str =
        format!("host={pg_host} port={pg_port} dbname=ducklake user=ducklake password=ducklake");

    // Check if Postgres is reachable; skip if not running in local environment
    if tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .is_err()
    {
        eprintln!(
            "skipping warm_writes_perform_zero_schema_probes_postgres: PostgreSQL not reachable at {conn_str}"
        );
        return;
    }

    let temp = TempDir::new().expect("temp");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.metadata_path = conn_str;
    let suffix = uuid::Uuid::new_v4().to_string().replace('-', "_");
    config.ducklake.metadata_schema = format!("hotpath_reg_{suffix}");

    let manager = RuntimeEngineManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("connect runtime engines");

    let tenant_id = format!("tenant-hotpath-{suffix}");
    let tenant_schema = format!("hotpath_tenant_{suffix}");
    let tenant_data = temp.path().join("data").to_string_lossy().to_string();

    manager
        .provision_scope(
            softprobe_runtime::runtime_engine::ScopeProvisioningRequest {
                scope_id: tenant_id.clone(),
                metadata_schema: tenant_schema.clone(),
                data_path: tenant_data.clone(),
            },
        )
        .await
        .expect("provision scope");

    let runtime = manager.engine_for(&tenant_id).await.expect("tenant engine");

    let mut query_dk = config.ducklake.clone();
    query_dk.metadata_schema = tenant_schema;
    query_dk.data_path = tenant_data;

    assert_warm_writes_zero_probes_contract(runtime.as_ref(), query_dk, Some(&tenant_id)).await;
}
