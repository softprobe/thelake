//! Catalog.schema qualification must never elide `main`, and TWCS/maintenance
//! probes must see real rows on both main and named DuckLake schemas.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use softprobe_runtime::config::Config;
use softprobe_runtime::storage::ducklake::open_attached_from_config;
use softprobe_runtime::workspace_scope::WorkspaceScopeMode;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::string_kv;

/// Wipe Postgres schema `main` so DuckLake ATTACH can bind a fresh temp DATA_PATH.
/// Prior e2e runs leave `main` pointing at a deleted /tmp warehouse and ATTACH 503s.
async fn reset_postgres_schema_main(metadata_path: &str) {
    let (client, connection) = tokio_postgres::connect(metadata_path, NoTls)
        .await
        .expect("connect ducklake postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("DROP SCHEMA IF EXISTS main CASCADE; CREATE SCHEMA main;")
        .await
        .expect("reset postgres schema main");
}

async fn maintenance_registry_client(config: &Config) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&config.ducklake.metadata_path, NoTls)
        .await
        .expect("connect maintenance registry");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn span_request(
    session_id: &str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> ExportTraceServiceRequest {
    let start_time_unix_nano = chrono::Utc::now()
        .timestamp_nanos_opt()
        .expect("current timestamp fits nanoseconds") as u64;
    let generation = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: vec![],
        name: "chat.completions".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano,
        end_time_unix_nano: start_time_unix_nano + 1_000_000_000,
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
    let (router, state) = softprobe_runtime::api::create_router(config.clone(), None)
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
    let status = resp.status();
    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("body");
    assert_eq!(
        status,
        StatusCode::OK,
        "ingest status={status} body={}",
        String::from_utf8_lossy(&body_bytes)
    );
    state
        .workspace_for_id("")
        .await
        .expect("workspace context")
        .ingest()
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
    let row_sql = format!(
        "SELECT count(*) FROM {qualified} WHERE {}",
        crate::util::query_window().timestamp_filter_sql("")
    );
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
    reset_postgres_schema_main(&config.ducklake.metadata_path).await;
    let config = Arc::new(config);

    ingest_one_span(config.clone(), "sess-qualify-main").await;
    assert_eq!(config.ducklake.metadata_schema, "main");
    assert_three_part_probe(&config);

    let (_router, state) = softprobe_runtime::api::create_router(config.clone(), None)
        .await
        .expect("router for maintenance");
    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    let registry = maintenance_registry_client(&config).await;
    let watermark_table_exists: bool = registry
        .query_one(
            "SELECT to_regclass('main.compaction_watermark') IS NOT NULL",
            &[],
        )
        .await
        .expect("check bootstrap watermark state")
        .get(0);
    assert!(
        !watermark_table_exists,
        "the first pass must bootstrap watermarks"
    );
    maintenance
        .run_pass()
        .await
        .expect("maintenance pass with compaction");
    let scope_key: String = registry
        .query_one(
            "SELECT scope_key FROM main.maintenance_scope_config WHERE lease_epoch = 0",
            &[],
        )
        .await
        .expect("read maintenance scope config")
        .get(0);
    let trace_merge = registry
        .query_one(
            "SELECT status, pass_started_at FROM main.maintenance_outcome \
             WHERE scope_key = $1 AND table_name = 'traces' AND action = 'merge'",
            &[&scope_key],
        )
        .await
        .expect("read trace merge outcome");
    assert_eq!(trace_merge.get::<_, String>(0), "completed");
    let first_pass_at: chrono::DateTime<chrono::Utc> = trace_merge.get(1);
    let first_watermark: chrono::DateTime<chrono::Utc> = registry
        .query_one(
            "SELECT watermark FROM main.compaction_watermark \
             WHERE scope_key = $1 AND table_name = 'traces'",
            &[&scope_key],
        )
        .await
        .expect("read bootstrap watermark")
        .get(0);
    assert_eq!(first_watermark, first_pass_at);

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    maintenance
        .run_pass()
        .await
        .expect("incremental maintenance pass");
    let next_watermark: chrono::DateTime<chrono::Utc> = registry
        .query_one(
            "SELECT watermark FROM main.compaction_watermark \
             WHERE scope_key = $1 AND table_name = 'traces'",
            &[&scope_key],
        )
        .await
        .expect("read incremental watermark")
        .get(0);
    assert!(next_watermark > first_watermark);

    // An invalid merge boundary makes the SQL merge fail before any cleanup.
    // The saved watermark and cleanup outcome must remain at the last success.
    let cleanup_after_success: chrono::DateTime<chrono::Utc> = registry
        .query_one(
            "SELECT pass_started_at FROM main.maintenance_outcome \
             WHERE scope_key = $1 AND table_name = '*' AND action = 'cleanup_scheduled_files'",
            &[&scope_key],
        )
        .await
        .expect("read cleanup outcome before merge failure")
        .get(0);
    registry
        .execute(
            "UPDATE main.compaction_watermark SET watermark = 'infinity' \
             WHERE scope_key = $1 AND table_name = 'traces'",
            &[&scope_key],
        )
        .await
        .expect("inject invalid merge boundary");
    assert!(
        maintenance.run_pass().await.is_err(),
        "invalid newer_than must fail the maintenance pass"
    );
    let failed_watermark: String = registry
        .query_one(
            "SELECT watermark::TEXT FROM main.compaction_watermark \
             WHERE scope_key = $1 AND table_name = 'traces'",
            &[&scope_key],
        )
        .await
        .expect("read watermark after merge failure")
        .get(0);
    assert_eq!(failed_watermark, "infinity");
    let cleanup_after_failure: chrono::DateTime<chrono::Utc> = registry
        .query_one(
            "SELECT pass_started_at FROM main.maintenance_outcome \
             WHERE scope_key = $1 AND table_name = '*' AND action = 'cleanup_scheduled_files'",
            &[&scope_key],
        )
        .await
        .expect("read cleanup outcome after merge failure")
        .get(0);
    assert_eq!(cleanup_after_failure, cleanup_after_success);

    registry
        .execute(
            "UPDATE main.compaction_watermark SET watermark = $2 \
             WHERE scope_key = $1 AND table_name = 'traces'",
            &[&scope_key, &next_watermark],
        )
        .await
        .expect("restore merge boundary");
    maintenance
        .run_pass()
        .await
        .expect("retry maintenance after merge failure");
}

#[tokio::test]
async fn sql_maintenance_merge_preserves_ducklake_layout() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.metadata_schema = format!("maintenance_layout_{}", Uuid::new_v4().simple());
    config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Isolated;
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.enabled = true;
    config.maintenance.interval_seconds = 3600;
    config.maintenance.metadata_enabled = true;
    let config = Arc::new(config);

    ingest_one_span(config.clone(), "maintenance-layout-bootstrap").await;
    // Scope initialization creates the canonical DuckLake tables. A first pass
    // establishes their bootstrap watermarks before the candidate files arrive.
    let (_router, state) = softprobe_runtime::api::create_router(config.clone(), None)
        .await
        .expect("router");
    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    maintenance.run_pass().await.expect("bootstrap pass");

    let catalog = config.ducklake.catalog_alias.as_str();
    let schema = config.ducklake.metadata_schema.as_str();
    let conn = open_attached_from_config(&config.ducklake, Some(0));
    conn.execute_batch("SET preserve_insertion_order = false;")
        .expect("apply shared row-group session setting");
    conn.execute_batch(&format!(
        "CALL {catalog}.set_option('data_inlining_row_limit', 0, \
         schema => '{schema}', table_name => 'traces');"
    ))
    .expect("disable inlining for merge fixtures");
    for (offset, day) in [(7503, 1), (5002, 1), (2501, 1)] {
        let sql = format!(
            "INSERT INTO {catalog}.{schema}.traces \
             (session_id, trace_id, span_id, app_id, message_type, timestamp, resource_attributes) \
             SELECT 'session-' || lpad(({} - i)::VARCHAR, 4, '0'), \
                    'trace-' || ({} - i)::VARCHAR, 'span-' || ({} - i)::VARCHAR, \
                    'maintenance-layout', 'INTERNAL', \
                    TIMESTAMP_NS '2026-01-{day:02} 12:00:00', \
                    MAP {{'wide_payload': repeat(md5((i + {offset})::VARCHAR), 128)}} \
             FROM range(2501) AS rows(i);",
            offset, offset, offset,
        );
        conn.execute_batch(&sql)
            .unwrap_or_else(|error| panic!("write merge fixture for day {day}: {error}"));
    }
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.{schema}.traces \
         (session_id, trace_id, span_id, app_id, message_type, timestamp, resource_attributes) \
         VALUES ('session-next-day', 'trace-next-day', 'span-next-day', \
                 'maintenance-layout', 'INTERNAL', \
                 TIMESTAMP_NS '2026-01-02 12:00:00', MAP {{'wide_payload': 'next-day'}});"
    ))
    .expect("write separate partition fixture");
    maintenance
        .run_pass()
        .await
        .expect("incremental merge pass");
    let registry = maintenance_registry_client(&config).await;
    let scope_key: String = registry
        .query_one(
            &format!(
                "SELECT scope_key FROM {}.maintenance_scope_config WHERE lease_epoch = 0",
                config.ducklake.metadata_schema
            ),
            &[],
        )
        .await
        .expect("read scope key")
        .get(0);
    let merge = registry
        .query_one(
            &format!(
                "SELECT status, files_processed, files_created \
                 FROM {}.maintenance_outcome \
                 WHERE scope_key = $1 AND table_name = 'traces' AND action = 'merge'",
                config.ducklake.metadata_schema
            ),
            &[&scope_key],
        )
        .await
        .expect("read traces merge result");
    assert_eq!(merge.get::<_, String>(0), "completed");
    assert!(
        merge.get::<_, i64>(1) >= 3,
        "expected all three files to merge"
    );
    assert!(
        merge.get::<_, i64>(2) >= 1,
        "merge must create output files"
    );

    // DuckLake stores the shared partition and sort definitions as active
    // metadata and groups merge candidates by their physical day partition.
    let metadata = format!("__ducklake_metadata_{catalog}");
    let (partition, sorted): (String, String) = conn
        .query_row(
            &format!(
                "SELECT \
                   coalesce((SELECT string_agg( \
                     CASE WHEN pc.transform = 'identity' THEN c.column_name \
                          ELSE pc.transform || '(' || c.column_name || ')' END, \
                     ', ' ORDER BY pc.partition_key_index) \
                     FROM {metadata}.ducklake_partition_info pi \
                     JOIN {metadata}.ducklake_table t ON t.table_id = pi.table_id \
                     JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
                     JOIN {metadata}.ducklake_partition_column pc \
                       ON pc.partition_id = pi.partition_id AND pc.table_id = pi.table_id \
                     JOIN {metadata}.ducklake_column c \
                       ON c.column_id = pc.column_id AND c.table_id = pc.table_id \
                     WHERE t.table_name = 'traces' AND s.schema_name = ? \
                       AND t.end_snapshot IS NULL AND pi.end_snapshot IS NULL \
                       AND c.end_snapshot IS NULL), ''), \
                   coalesce((SELECT string_agg(se.expression, ', ' ORDER BY se.sort_key_index) \
                     FROM {metadata}.ducklake_sort_info si \
                     JOIN {metadata}.ducklake_table t ON t.table_id = si.table_id \
                     JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
                     JOIN {metadata}.ducklake_sort_expression se \
                       ON se.sort_id = si.sort_id AND se.table_id = si.table_id \
                     WHERE t.table_name = 'traces' AND s.schema_name = ? \
                       AND t.end_snapshot IS NULL AND si.end_snapshot IS NULL), '')"
            ),
            [&schema, &schema],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read active trace layout");
    assert_eq!(
        partition.replace(' ', ""),
        "year(timestamp),month(timestamp),day(timestamp)"
    );
    assert_eq!(
        sorted.replace(' ', "").replace('"', ""),
        "session_id,trace_id,timestamp"
    );

    let settings: std::collections::HashMap<String, String> = {
        let table_options = format!(
            "SELECT key, value FROM {metadata}.ducklake_metadata \
             WHERE scope = 'table' AND scope_id = ( \
               SELECT t.table_id::VARCHAR FROM {metadata}.ducklake_table t \
               JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
               WHERE t.table_name = 'traces' AND s.schema_name = '{}' \
                 AND t.end_snapshot IS NULL) \
             AND key IN ('target_file_size', 'parquet_row_group_size_bytes', \
                         'parquet_compression', 'parquet_compression_level')",
            schema.replace('\'', "''")
        );
        let mut statement = conn.prepare(&table_options).expect("prepare table options");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("read table options")
            .collect::<duckdb::Result<_>>()
            .expect("collect table options")
    };
    assert_eq!(
        settings.get("target_file_size").map(String::as_str),
        Some("134217728")
    );
    assert_eq!(
        settings
            .get("parquet_row_group_size_bytes")
            .map(String::as_str),
        Some("8388608")
    );
    assert_eq!(
        settings.get("parquet_compression").map(String::as_str),
        Some("zstd")
    );
    assert_eq!(
        settings
            .get("parquet_compression_level")
            .map(String::as_str),
        Some("3")
    );

    let active_day_files: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM {metadata}.ducklake_data_file df \
                 JOIN {metadata}.ducklake_table t ON t.table_id = df.table_id \
                 JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
                 WHERE t.table_name = 'traces' AND s.schema_name = ? \
                   AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL \
                   AND df.path LIKE 'year=2026/month=1/day=1/%'"
            ),
            [&schema],
            |row| row.get(0),
        )
        .expect("count active day-one files");
    let active_next_day_files: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM {metadata}.ducklake_data_file df \
                 JOIN {metadata}.ducklake_table t ON t.table_id = df.table_id \
                 JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
                 WHERE t.table_name = 'traces' AND s.schema_name = ? \
                   AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL \
                   AND df.path LIKE 'year=2026/month=1/day=2/%'"
            ),
            [&schema],
            |row| row.get(0),
        )
        .expect("count active next-day files");
    assert_eq!(
        active_day_files, 1,
        "same-day candidates should merge together"
    );
    assert_eq!(
        active_next_day_files, 1,
        "a different day must remain a separate file"
    );

    let (file_path, file_relative, table_path, table_relative, schema_path, schema_relative): (
        String,
        bool,
        String,
        bool,
        String,
        bool,
    ) = conn
        .query_row(
            &format!(
                "SELECT df.path, df.path_is_relative, t.path, t.path_is_relative, \
                        s.path, s.path_is_relative \
                 FROM {metadata}.ducklake_data_file df \
                 JOIN {metadata}.ducklake_table t ON t.table_id = df.table_id \
                 JOIN {metadata}.ducklake_schema s ON s.schema_id = t.schema_id \
                 WHERE t.table_name = 'traces' AND s.schema_name = ? \
                   AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL \
                   AND df.path LIKE 'year=2026/month=1/day=1/%'"
            ),
            [&schema],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .expect("read merged day-one file");
    let schema_base = if schema_relative {
        std::path::Path::new(&config.ducklake.data_path).join(schema_path)
    } else {
        std::path::PathBuf::from(schema_path)
    };
    let table_base = if table_relative {
        schema_base.join(table_path)
    } else {
        std::path::PathBuf::from(table_path)
    };
    let full_path = if file_relative {
        table_base.join(file_path)
    } else {
        std::path::PathBuf::from(file_path)
    }
    .to_string_lossy()
    .into_owned();
    let compression: Vec<String> = {
        let mut statement = conn
            .prepare("SELECT DISTINCT compression FROM parquet_metadata(?)")
            .expect("prepare output compression query");
        statement
            .query_map([&full_path], |row| row.get(0))
            .expect("read output compression")
            .collect::<duckdb::Result<_>>()
            .expect("collect output compression")
    };
    assert!(!compression.is_empty());
    assert!(
        compression
            .iter()
            .all(|codec| codec.eq_ignore_ascii_case("zstd")),
        "merged file must use shared ZSTD compression: {compression:?}"
    );

    let (row_groups, largest_group_bytes): (i64, i64) = conn
        .query_row(
            "SELECT count(*), max(uncompressed_bytes) FROM ( \
               SELECT row_group_id, sum(total_uncompressed_size) AS uncompressed_bytes \
               FROM parquet_metadata(?) GROUP BY row_group_id)",
            [&full_path],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read merged row groups");
    assert!(row_groups > 0, "merged output must contain a row group");
    assert!(
        largest_group_bytes > 0,
        "merged output row-group metadata must include its size"
    );

    let unsorted_rows: i64 = conn
        .query_row(
            "SELECT count(*) FROM ( \
               SELECT session_id, lag(session_id) OVER (ORDER BY file_row_number) AS previous \
               FROM read_parquet(?, file_row_number = true)) \
             WHERE previous > session_id",
            [&full_path],
            |row| row.get(0),
        )
        .expect("check merged physical row order");
    assert_eq!(
        unsorted_rows, 0,
        "merged file must retain session/trace/time order"
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

    let (_router, state) = softprobe_runtime::api::create_router(config.clone(), None)
        .await
        .expect("router");
    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("maintenance engine");
    maintenance
        .run_pass()
        .await
        .expect("shared maintenance pass");
}
