//! Workspace deletion drains authenticated requests, removes one tenant's
//! shared DuckLake rows, and leaves other tenants' rows available.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::Value;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::{Log, Score, ScoreConfig, ScoreDataType, ScoreSource, Span};
use softprobe_runtime::storage::ducklake::open_attached_from_config;
use softprobe_runtime::workspace::ScopeProvisioningRequest;
use softprobe_runtime::workspace_scope::WorkspaceScopeMode;
use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

struct RestoreEnv {
    name: &'static str,
    previous: Option<OsString>,
}

impl RestoreEnv {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, previous }
    }

    fn unset(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        std::env::remove_var(name);
        Self { name, previous }
    }
}

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

fn test_config(temp: &TempDir) -> Config {
    let mut config = crate::util::config::file_backed_test_config(temp);
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Shared;
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.reader_safety_grace_seconds = 1;
    config
}

fn span(workspace_id: &str, trace_id: &str, session_id: &str) -> Span {
    Span {
        session_id: session_id.to_string(),
        trace_id: trace_id.to_string(),
        span_id: format!("span-{trace_id}"),
        parent_span_id: None,
        app_id: "workspace-purge-test".to_string(),
        organization_id: None,
        workspace_id: Some(workspace_id.to_string()),
        agent_id: None,
        agent_name: Some(format!("agent-{workspace_id}")),
        message_type: "op".to_string(),
        span_kind: Some("SPAN_KIND_INTERNAL".to_string()),
        timestamp: Utc::now(),
        end_timestamp: None,
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}

fn log(workspace_id: &str, trace_id: &str, session_id: &str) -> Log {
    Log {
        session_id: Some(session_id.to_string()),
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: format!("log-{workspace_id}"),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.to_string()),
        span_id: Some(format!("span-{trace_id}")),
        workspace_id: Some(workspace_id.to_string()),
        agent_id: None,
        agent_name: None,
    }
}

fn score_config(config_id: &str, workspace_id: &str) -> ScoreConfig {
    ScoreConfig {
        config_id: config_id.to_string(),
        timestamp: Utc::now(),
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        description: Some(format!("workspace purge {workspace_id}")),
        min_value: Some(0.0),
        max_value: Some(1.0),
        categories: Vec::new(),
        author_id: None,
        metadata: HashMap::new(),
        workspace_id: Some(workspace_id.to_string()),
    }
}

fn score(score_id: &str, trace_id: &str, config_id: &str, workspace_id: &str) -> Score {
    Score {
        score_id: score_id.to_string(),
        timestamp: Utc::now(),
        trace_id: Some(trace_id.to_string()),
        span_id: None,
        session_id: None,
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        numeric_value: Some(0.9),
        string_value: None,
        boolean_value: None,
        source: ScoreSource::Evaluator,
        comment: None,
        config_id: Some(config_id.to_string()),
        author_id: None,
        metadata: HashMap::new(),
        workspace_id: Some(workspace_id.to_string()),
    }
}

async fn json_response(response: axum::response::Response<Body>) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("read response body")
        .to_bytes();
    let value = serde_json::from_slice(&body).unwrap_or_else(|error| {
        panic!(
            "JSON response status={status}: {error}; body={}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, value)
}

fn workspace_row_count(config: &Config, table: &str, workspace_id: &str) -> i64 {
    let conn = open_attached_from_config(&config.ducklake, config.ducklake.data_inlining_row_limit);
    let sql = include_str!("../fixtures/workspace_purge_row_count.sql")
        .replace("{{catalog}}", &config.ducklake.catalog_alias)
        .replace("{{schema}}", &config.ducklake.metadata_schema)
        .replace("{{table}}", table);
    conn.query_row(&sql, [workspace_id], |row| row.get(0))
        .expect("count shared catalog workspace rows")
}

fn active_data_files(config: &Config) -> Vec<std::path::PathBuf> {
    let conn = open_attached_from_config(&config.ducklake, config.ducklake.data_inlining_row_limit);
    let metadata_catalog = format!("__ducklake_metadata_{}", config.ducklake.catalog_alias);
    let sql = include_str!("../fixtures/workspace_purge_active_files.sql")
        .replace("{{metadata_catalog}}", &metadata_catalog)
        .replace("{{schema}}", &config.ducklake.metadata_schema);
    let mut statement = conn
        .prepare(&sql)
        .expect("prepare active workspace data files");
    statement
        .query_map([], |row| {
            let file_path: String = row.get(0)?;
            let file_relative: bool = row.get(1)?;
            let table_path: String = row.get(2)?;
            let table_relative: bool = row.get(3)?;
            let schema_path: String = row.get(4)?;
            let schema_relative: bool = row.get(5)?;
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
            Ok(if file_relative {
                table_base.join(file_path)
            } else {
                std::path::PathBuf::from(file_path)
            })
        })
        .expect("read active workspace data files")
        .collect::<Result<Vec<_>, _>>()
        .expect("decode active workspace data file paths")
}

async fn delete_workspace(router: &axum::Router, workspace_id: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/workspaces/{workspace_id}"))
        .header(header::AUTHORIZATION, "Bearer workspace-purge-test-key")
        .body(Body::empty())
        .expect("workspace delete request");
    json_response(router.clone().oneshot(request).await.expect("delete route")).await
}

async fn workspace_deletion_status(
    router: &axum::Router,
    workspace_id: &str,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/workspaces/{workspace_id}/deletion"))
        .header(header::AUTHORIZATION, "Bearer workspace-purge-test-key")
        .body(Body::empty())
        .expect("workspace deletion status request");
    json_response(
        router
            .clone()
            .oneshot(request)
            .await
            .expect("workspace deletion status route"),
    )
    .await
}

#[tokio::test]
async fn admin_workspace_delete_purges_target_and_preserves_other_workspace() {
    // `make test-e2e` enables this destructive reset for fresh startup; this
    // test owns its data from seed through cleanup and must keep the catalog.
    let _reset = RestoreEnv::unset("SPLAKE_RESET_DUCKLAKE");
    let temp = TempDir::new().expect("temporary workspace catalog");
    let config = test_config(&temp);
    let (router, state) = softprobe_runtime::api::create_router(Arc::new(config.clone()), None)
        .await
        .expect("shared runtime router");
    let workspaces = &state.workspaces;
    let workspace_a = Uuid::new_v4().to_string();
    let workspace_b = Uuid::new_v4().to_string();
    let storage_hints = ScopeProvisioningRequest {
        scope_id: workspace_a.clone(),
        metadata_schema: config.ducklake.metadata_schema.clone(),
        data_path: config.ducklake.data_path.clone(),
    };
    workspaces
        .provision_scope(storage_hints.clone())
        .await
        .expect("provision workspace A");
    workspaces
        .provision_scope(ScopeProvisioningRequest {
            scope_id: workspace_b.clone(),
            ..storage_hints
        })
        .await
        .expect("provision workspace B");
    let workspace_a_context = workspaces
        .workspace_for(&workspace_a)
        .await
        .expect("workspace A context");
    let workspace_b_context = workspaces
        .workspace_for(&workspace_b)
        .await
        .expect("workspace B context");

    let trace_a = format!("purge-a-{}", Uuid::new_v4().simple());
    let trace_b = format!("purge-b-{}", Uuid::new_v4().simple());
    let session = format!("purge-session-{}", Uuid::new_v4().simple());
    let config_a = format!("purge-config-a-{}", Uuid::new_v4().simple());
    let config_b = format!("purge-config-b-{}", Uuid::new_v4().simple());

    workspace_a_context
        .ingest()
        .add_spans(vec![span(&workspace_a, &trace_a, &session)], 0)
        .await
        .expect("seed workspace A trace");
    workspace_a_context
        .ingest()
        .add_logs(vec![log(&workspace_a, &trace_a, &session)], 0)
        .await
        .expect("seed workspace A log");
    workspace_a_context
        .ingest()
        .add_score_configs(vec![score_config(&config_a, &workspace_a)])
        .await
        .expect("seed workspace A score config");
    workspace_a_context
        .ingest()
        .add_scores(vec![score(
            &format!("score-a-{}", Uuid::new_v4().simple()),
            &trace_a,
            &config_a,
            &workspace_a,
        )])
        .await
        .expect("seed workspace A score");
    let workspace_a_files = active_data_files(&config);
    assert!(
        !workspace_a_files.is_empty(),
        "workspace A must create data files"
    );

    workspace_b_context
        .ingest()
        .add_spans(vec![span(&workspace_b, &trace_b, &session)], 0)
        .await
        .expect("seed workspace B trace");
    workspace_b_context
        .ingest()
        .add_logs(vec![log(&workspace_b, &trace_b, &session)], 0)
        .await
        .expect("seed workspace B log");
    workspace_b_context
        .ingest()
        .add_score_configs(vec![score_config(&config_b, &workspace_b)])
        .await
        .expect("seed workspace B score config");
    workspace_b_context
        .ingest()
        .add_scores(vec![score(
            &format!("score-b-{}", Uuid::new_v4().simple()),
            &trace_b,
            &config_b,
            &workspace_b,
        )])
        .await
        .expect("seed workspace B score");
    let all_workspace_files = active_data_files(&config);
    let workspace_b_files: Vec<_> = all_workspace_files
        .into_iter()
        .filter(|path| !workspace_a_files.contains(path))
        .collect();
    assert!(
        !workspace_b_files.is_empty(),
        "workspace B must create data files"
    );

    for table in ["traces", "logs", "scores", "score_configs"] {
        assert!(workspace_row_count(&config, table, &workspace_a) > 0);
        assert!(workspace_row_count(&config, table, &workspace_b) > 0);
    }

    let unauthorized = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/workspaces/{workspace_a}"))
        .body(Body::empty())
        .expect("unauthorized delete request");
    let unauthorized_response = router
        .clone()
        .oneshot(unauthorized)
        .await
        .expect("unauthorized delete route");
    assert_eq!(unauthorized_response.status(), StatusCode::UNAUTHORIZED);

    let _admin_key = RestoreEnv::set("SOFTPROBE_ADMIN_API_KEY", "workspace-purge-test-key");
    let (access_started_tx, access_started_rx) = tokio::sync::oneshot::channel();
    let (release_access_tx, release_access_rx) = tokio::sync::oneshot::channel();
    let guarded_workspaces = state.workspaces.clone();
    let guarded_workspace_id = workspace_a.clone();
    let access_task = tokio::spawn(async move {
        guarded_workspaces
            .with_active_workspace_access(&guarded_workspace_id, async move {
                let _ = access_started_tx.send(());
                let _ = release_access_rx.await;
            })
            .await
    });
    access_started_rx
        .await
        .expect("workspace access lock acquired");
    let delete_router = router.clone();
    let delete_workspace_id = workspace_a.clone();
    let delete_task =
        tokio::spawn(async move { delete_workspace(&delete_router, &delete_workspace_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !delete_task.is_finished(),
        "workspace tombstone must wait for in-flight access to release its row lock"
    );
    release_access_tx
        .send(())
        .expect("release workspace access");
    access_task
        .await
        .expect("workspace access task")
        .expect("workspace access lock release");
    let (status, response) = delete_task.await.expect("workspace delete request");
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "delete workspace A: {response}"
    );
    assert_eq!(response["status"], "deleting");
    assert!(workspaces.workspace_for(&workspace_a).await.is_err());
    assert!(workspaces
        .with_active_workspace_access(&workspace_a, async {})
        .await
        .is_err());
    assert!(workspaces.workspace_for(&workspace_b).await.is_ok());

    let (pending_status, pending_response) = workspace_deletion_status(&router, &workspace_a).await;
    assert_eq!(pending_status, StatusCode::OK);
    assert_eq!(pending_response["status"], "deleting");
    assert!(workspaces
        .pending_shared_workspace_deletions()
        .await
        .expect("durable purge queue")
        .contains(&workspace_a));
    let restarted_workspaces =
        softprobe_runtime::workspace::WorkspaceManager::connect(Arc::new(config.clone()), None)
            .await
            .expect("restarted workspace manager");
    assert!(restarted_workspaces
        .pending_shared_workspace_deletions()
        .await
        .expect("durable purge queue after restart")
        .contains(&workspace_a));
    assert!(workspace_row_count(&config, "traces", &workspace_a) > 0);

    let lake = open_attached_from_config(&config.ducklake, config.ducklake.data_inlining_row_limit);
    let partial_delete = include_str!("../fixtures/workspace_purge_partial_delete.sql")
        .replace("{{catalog}}", &config.ducklake.catalog_alias)
        .replace("{{schema}}", &config.ducklake.metadata_schema)
        .replace("{{workspace_id}}", &workspace_a);
    lake.execute_batch(&partial_delete)
        .expect("simulate interruption after row deletion but before rewrite");
    drop(lake);
    let purge_started = std::time::Instant::now();
    let runner = softprobe_runtime::compaction::scheduler::start_maintenance_scheduler(
        state.workspaces.clone(),
    )
    .await
    .expect("start shared job runner")
    .expect("workspace purge runner");
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let (_, response) = workspace_deletion_status(&router, &workspace_a).await;
            if response["status"] == "deleted" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("durable runner should finish workspace purge");
    runner.abort();
    assert!(
        purge_started.elapsed() >= std::time::Duration::from_secs(2),
        "a retry with no visible rows must still honor the reader grace"
    );
    let (completed_status, completed_response) =
        workspace_deletion_status(&router, &workspace_a).await;
    assert_eq!(completed_status, StatusCode::OK);
    assert_eq!(completed_response["status"], "deleted");
    assert!(!workspaces
        .pending_shared_workspace_deletions()
        .await
        .expect("completed purge leaves queue")
        .contains(&workspace_a));

    for table in ["traces", "logs", "scores", "score_configs"] {
        assert_eq!(workspace_row_count(&config, table, &workspace_a), 0);
        assert!(workspace_row_count(&config, table, &workspace_b) > 0);
    }
    let (registry, registry_connection) =
        tokio_postgres::connect(&config.ducklake.metadata_path, tokio_postgres::NoTls)
            .await
            .expect("connect workspace purge registry");
    tokio::spawn(async move {
        let _ = registry_connection.await;
    });
    let cleanup_sql = include_str!("../fixtures/workspace_purge_cleanup_outcomes.sql")
        .replace("{{schema}}", &config.ducklake.metadata_schema);
    let cleanup_outcomes = registry
        .query(&cleanup_sql, &[])
        .await
        .expect("read purge snapshot and file cleanup outcomes");
    assert_eq!(cleanup_outcomes.len(), 2);
    for outcome in cleanup_outcomes {
        let action: String = outcome.get(0);
        let status: String = outcome.get(1);
        let files_processed: i64 = outcome.get(2);
        assert_eq!(status, "completed", "workspace purge {action} must run");
        if action == "cleanup_scheduled_files" {
            assert!(
                files_processed > 0,
                "workspace purge {action} must physically remove snapshot/file records; reported {files_processed}"
            );
        }
    }
    for path in &workspace_a_files {
        assert!(
            !path.exists(),
            "deleted workspace file remains on disk: {path:?}"
        );
    }
    for path in &workspace_b_files {
        assert!(path.exists(), "other workspace file was removed: {path:?}");
    }

    let (retry_status, retry_response) = delete_workspace(&router, &workspace_a).await;
    assert_eq!(
        retry_status,
        StatusCode::OK,
        "retry delete: {retry_response}"
    );
    let (cleanup_status, cleanup_response) = delete_workspace(&router, &workspace_b).await;
    assert_eq!(
        cleanup_status,
        StatusCode::ACCEPTED,
        "delete workspace B test data: {cleanup_response}"
    );
    let runner = softprobe_runtime::compaction::scheduler::start_maintenance_scheduler(
        state.workspaces.clone(),
    )
    .await
    .expect("restart shared job runner")
    .expect("workspace purge runner");
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let (_, response) = workspace_deletion_status(&router, &workspace_b).await;
            if response["status"] == "deleted" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("durable runner should finish workspace B cleanup");
    runner.abort();
    for table in ["traces", "logs", "scores", "score_configs"] {
        assert_eq!(workspace_row_count(&config, table, &workspace_b), 0);
    }
}
