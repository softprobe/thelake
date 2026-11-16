use chrono::{Duration, Utc};
use softprobe_runtime::ingest::IngestEngine;
use softprobe_runtime::models::{Log, Score, ScoreConfig, ScoreDataType, ScoreSource, Span};
use softprobe_runtime::query::{self, LogCountFilter, TraceCountFilter};
use softprobe_runtime::sql::QueryWindow;
use softprobe_runtime::storage::ducklake::{
    migrate_ducklake_catalog_from_config, open_attached_from_config,
};
use std::collections::HashMap;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;

const VERSION_SQL: &str = include_str!("../fixtures/ducklake_catalog_migration_version.sql");
const SIMULATE_1_0_SQL: &str =
    include_str!("../fixtures/ducklake_catalog_migration_simulate_1_0.sql");
const HAS_ROW_GROUP_COUNT_SQL: &str =
    include_str!("../fixtures/ducklake_catalog_migration_has_row_group_count.sql");
const ROW_GROUP_COUNT_SQL: &str =
    include_str!("../fixtures/ducklake_catalog_migration_row_group_count.sql");
const MIN_TRACE_ID_SQL: &str =
    include_str!("../fixtures/ducklake_catalog_migration_min_trace_id.sql");

async fn pg_connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("connect to postgres");
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });
    client
}

async fn ducklake_catalog_version(client: &tokio_postgres::Client, schema: &str) -> String {
    let sql = VERSION_SQL.replace("{{SCHEMA}}", schema);
    client
        .query_one(&sql, &[])
        .await
        .expect("query ducklake_metadata version")
        .get::<_, String>(0)
}

fn sample_span(session_id: &str, trace_id: &str, span_id: &str, label: &str) -> Span {
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("test.label".to_string(), label.to_string());
    Span {
        session_id: session_id.to_string(),
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: None,
        app_id: "migration-app".to_string(),
        organization_id: None,
        workspace_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "chat".to_string(),
        span_kind: Some("INTERNAL".to_string()),
        timestamp: now,
        end_timestamp: Some(now + Duration::milliseconds(5)),
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

fn sample_log(session_id: &str, trace_id: &str, body: &str) -> Log {
    Log {
        session_id: Some(session_id.to_string()),
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.to_string()),
        span_id: None,
        workspace_id: None,
        agent_id: None,
        agent_name: None,
    }
}

fn sample_score_config(config_id: &str) -> ScoreConfig {
    ScoreConfig {
        config_id: config_id.to_string(),
        timestamp: Utc::now(),
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        description: Some("catalog migration test".to_string()),
        min_value: Some(0.0),
        max_value: Some(1.0),
        categories: Vec::new(),
        author_id: None,
        metadata: HashMap::new(),
        workspace_id: None,
    }
}

fn sample_score(score_id: &str, trace_id: &str, config_id: &str) -> Score {
    Score {
        score_id: score_id.to_string(),
        timestamp: Utc::now(),
        trace_id: Some(trace_id.to_string()),
        span_id: None,
        session_id: None,
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        numeric_value: Some(0.95),
        string_value: None,
        boolean_value: None,
        source: ScoreSource::Evaluator,
        comment: None,
        config_id: Some(config_id.to_string()),
        author_id: None,
        metadata: HashMap::new(),
        workspace_id: None,
    }
}

#[tokio::test]
async fn automatic_migration_upgrades_postgres_catalog_to_1_1_and_preserves_reads_and_writes() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    let start_ts = Utc::now() - Duration::minutes(5);
    let start = start_ts.to_rfc3339();
    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");

    let session_1 = format!("mig-sess-1-{}", Uuid::new_v4());
    let trace_1 = format!("mig-tr-1-{}", Uuid::new_v4());
    let cfg_1 = format!("mig-cfg-1-{}", Uuid::new_v4());
    let score_1 = format!("mig-sc-1-{}", Uuid::new_v4());
    let sc1 = sample_score(&score_1, &trace_1, &cfg_1);
    let sc1_ts = sc1.timestamp;

    pipeline
        .add_spans(vec![sample_span(&session_1, &trace_1, "sp-1", "before")], 0)
        .await
        .expect("ingest pre-migration span");
    pipeline
        .add_logs(
            vec![sample_log(&session_1, &trace_1, "pre-migration log")],
            0,
        )
        .await
        .expect("ingest pre-migration log");
    pipeline
        .add_score_configs(vec![sample_score_config(&cfg_1)])
        .await
        .expect("ingest pre-migration score_config");
    pipeline
        .add_scores(vec![sc1])
        .await
        .expect("ingest pre-migration score");

    let pg = pg_connect(&config.ducklake.metadata_path).await;
    let meta_schema = if config.ducklake.metadata_schema == "main" {
        format!("__ducklake_metadata_{}", config.ducklake.catalog_alias)
    } else {
        config.ducklake.metadata_schema.clone()
    };
    let version_fresh = ducklake_catalog_version(&pg, &meta_schema).await;
    assert_eq!(
        version_fresh, "1.1",
        "expected fresh DuckLake 1.1 catalog to start at 1.1"
    );

    pg.batch_execute(&SIMULATE_1_0_SQL.replace("{{SCHEMA}}", &meta_schema))
        .await
        .expect("simulate 1.0 catalog state");

    let version_before = ducklake_catalog_version(&pg, &meta_schema).await;
    assert_eq!(
        version_before, "1.0",
        "expected simulated catalog to be at 1.0 before migration"
    );

    let migrated_session = migrate_ducklake_catalog_from_config(&config.ducklake);
    drop(migrated_session);

    let version_after = ducklake_catalog_version(&pg, &meta_schema).await;
    assert_ne!(
        version_after, "1.0",
        "expected migration to upgrade catalog past 1.0, got {version_after}"
    );
    assert!(
        version_after.starts_with("1.1"),
        "expected DuckLake 1.1 catalog version after migration, got {version_after}"
    );

    let has_row_group_count: bool = pg
        .query_one(HAS_ROW_GROUP_COUNT_SQL, &[&meta_schema])
        .await
        .expect("check row_group_count column")
        .get(0);
    assert!(
        has_row_group_count,
        "expected ducklake_data_file.row_group_count column after 1.1 migration"
    );

    // Verify post-migration writes across all four tables succeed.
    let session_2 = format!("mig-sess-2-{}", Uuid::new_v4());
    let trace_2 = format!("mig-tr-2-{}", Uuid::new_v4());
    let cfg_2 = format!("mig-cfg-2-{}", Uuid::new_v4());
    let score_2 = format!("mig-sc-2-{}", Uuid::new_v4());
    let sc2 = sample_score(&score_2, &trace_2, &cfg_2);
    let sc2_ts = sc2.timestamp;

    pipeline
        .add_spans(vec![sample_span(&session_2, &trace_2, "sp-2", "after")], 0)
        .await
        .expect("ingest post-migration span");
    pipeline
        .add_logs(
            vec![sample_log(&session_2, &trace_2, "post-migration log")],
            0,
        )
        .await
        .expect("ingest post-migration log");
    pipeline
        .add_score_configs(vec![sample_score_config(&cfg_2)])
        .await
        .expect("ingest post-migration score_config");
    pipeline
        .add_scores(vec![sc2])
        .await
        .expect("ingest post-migration score");

    let end_ts = Utc::now() + Duration::minutes(5);
    let end = end_ts.to_rfc3339();
    let window = QueryWindow::try_new(start_ts, end_ts).expect("query window");
    let query_engine = query::create_query_engine(&config)
        .await
        .expect("query engine");

    let trace_count = query_engine
        .count_traces(TraceCountFilter {
            time_window: window,
            session_id: None,
            app_id: None,
            span_id: None,
        })
        .await
        .expect("count traces after migration");
    assert_eq!(trace_count, 2);

    let log_count = query_engine
        .count_logs(LogCountFilter {
            time_window: window,
            session_id: None,
            trace_id: None,
            body: None,
        })
        .await
        .expect("count logs after migration");
    assert_eq!(log_count, 2);

    assert!(
        query_engine
            .score_exists(&score_1, sc1_ts)
            .await
            .expect("pre-migration score exists"),
        "pre-migration score missing"
    );
    assert!(
        query_engine
            .score_exists(&score_2, sc2_ts)
            .await
            .expect("post-migration score exists"),
        "post-migration score missing"
    );
    assert!(
        query_engine
            .score_config_exists(&cfg_1)
            .await
            .expect("pre-migration score_config exists"),
        "pre-migration score_config missing"
    );
    assert!(
        query_engine
            .score_config_exists(&cfg_2)
            .await
            .expect("post-migration score_config exists"),
        "post-migration score_config missing"
    );

    let populated_row_groups: i64 = pg
        .query_one(
            &ROW_GROUP_COUNT_SQL.replace("{{SCHEMA}}", &meta_schema),
            &[],
        )
        .await
        .expect("query row_group_count")
        .get(0);
    assert!(
        populated_row_groups >= 1,
        "expected row_group_count > 0 on post-migration Parquet files"
    );

    let attached = open_attached_from_config(&config.ducklake, Some(0));
    let min_sql = MIN_TRACE_ID_SQL
        .replace("{{START}}", &start)
        .replace("{{END}}", &end);
    let exact_trace_min: String = attached
        .query_row(&min_sql, [], |r| r.get(0))
        .expect("MIN(trace_id)");
    assert_eq!(exact_trace_min, trace_1.clone().min(trace_2.clone()));
}
