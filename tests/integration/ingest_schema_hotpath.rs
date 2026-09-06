//! Contract test: After warm bootstrap, N consecutive writes perform zero
//! DESCRIBE / partition-info / sort-info probes (Issue #51).

use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::{Log as LogData, Metric as MetricData, Span as SpanData};
use softprobe_runtime::storage::ducklake::open_and_attach_ducklake;
use softprobe_runtime::storage::schema::{
    describe_probe_count, partition_sort_probe_count, total_schema_probe_count,
};
use std::collections::HashMap;
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
    }
}

fn sample_metric(i: usize) -> MetricData {
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("metric.iteration".to_string(), i.to_string());
    MetricData {
        metric_name: "test_counter".to_string(),
        description: "hotpath test counter".to_string(),
        unit: "1".to_string(),
        metric_type: "counter".to_string(),
        timestamp: now,
        value: i as f64,
        attributes,
        ..Default::default()
    }
}

async fn assert_warm_writes_zero_probes_contract(config: Config, tenant_id: Option<&str>) {
    let _guard = HOTPATH_CONTRACT_LOCK.lock().await;
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");

    // Perform one initial write across signals to ensure cold paths / pool creation are complete.
    pipeline
        .write_span_batches(vec![vec![sample_span(0, tenant_id)]])
        .await
        .expect("warm span write");
    pipeline
        .write_log_batches(vec![vec![sample_log(0)]])
        .await
        .expect("warm log write");
    pipeline
        .write_metric_batches(vec![vec![sample_metric(0)]])
        .await
        .expect("warm metric write");

    // Record baseline probe count after warm bootstrap.
    let desc_before = describe_probe_count();
    let part_before = partition_sort_probe_count();
    let total_before = total_schema_probe_count();

    const N: usize = 5;
    for i in 1..=N {
        pipeline
            .write_span_batches(vec![vec![sample_span(i, tenant_id)]])
            .await
            .unwrap_or_else(|e| panic!("span write {i} failed: {e}"));
        pipeline
            .write_log_batches(vec![vec![sample_log(i)]])
            .await
            .unwrap_or_else(|e| panic!("log write {i} failed: {e}"));
        pipeline
            .write_metric_batches(vec![vec![sample_metric(i)]])
            .await
            .unwrap_or_else(|e| panic!("metric write {i} failed: {e}"));
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

    // Verify all rows were committed and queryable.
    let query_dk = if let Some(tid) = tenant_id {
        let resolver = softprobe_runtime::runtime_engine::DuckLakeScopeResolver::connect(&config)
            .await
            .expect("resolver")
            .expect("postgres resolver");
        let (scope, _) = resolver
            .load_active_telemetry_columns_manifests(tid)
            .await
            .expect("scope");
        let mut t_dk = config.ducklake.clone();
        t_dk.metadata_schema = scope.metadata_schema;
        t_dk.data_path = scope.data_path;
        t_dk
    } else {
        config.ducklake.clone()
    };

    let (conn, catalog) = open_and_attach_ducklake(&query_dk).expect("query attach");

    let span_n: i64 = conn
        .query_row(&format!("SELECT count(*) FROM {catalog}.traces"), [], |r| {
            r.get(0)
        })
        .expect("query traces");
    assert_eq!(span_n, (N + 1) as i64, "all traces must be committed");

    let log_n: i64 = conn
        .query_row(&format!("SELECT count(*) FROM {catalog}.logs"), [], |r| {
            r.get(0)
        })
        .expect("query logs");
    assert_eq!(log_n, (N + 1) as i64, "all logs must be committed");

    let metric_n: i64 = conn
        .query_row(
            &format!("SELECT count(*) FROM {catalog}.metric_samples"),
            [],
            |r| r.get(0),
        )
        .expect("query metric_samples");
    assert_eq!(
        metric_n,
        (N + 1) as i64,
        "all metric samples must be committed"
    );
}

#[tokio::test]
async fn warm_writes_perform_zero_schema_probes_sqlite() {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    assert_warm_writes_zero_probes_contract(config, None).await;
}

#[tokio::test]
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
    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path = conn_str;
    let suffix = uuid::Uuid::new_v4().to_string().replace('-', "_");
    config.ducklake.metadata_schema = format!("hotpath_reg_{suffix}");

    // Connect resolver and provision tenant scope
    let resolver = softprobe_runtime::runtime_engine::DuckLakeScopeResolver::connect(&config)
        .await
        .expect("connect resolver")
        .expect("postgres resolver");

    let tenant_id = format!("tenant-hotpath-{suffix}");
    let tenant_schema = format!("hotpath_tenant_{suffix}");
    let tenant_data = temp.path().join("data").to_string_lossy().to_string();

    resolver
        .provision_scope(
            softprobe_runtime::runtime_engine::ScopeProvisioningRequest {
                scope_id: tenant_id.clone(),
                metadata_schema: tenant_schema,
                data_path: tenant_data,
            },
        )
        .await
        .expect("provision scope");

    assert_warm_writes_zero_probes_contract(config, Some(&tenant_id)).await;
}
