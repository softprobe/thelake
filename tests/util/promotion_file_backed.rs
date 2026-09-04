//! Shared file-backed DuckLake lifecycle for promotion profile integration tests.
//!
//! `promotion_llm_v1` and `promotion_mocker_v1` share router setup, apply, OTLP ingest,
//! and DuckLake attach/query primitives. Profile-specific simulated manifests, OTLP
//! fixtures, and assertions stay in each test module (see `promotion_fixtures`).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use serde_json::json;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant as inject_tenant;

pub struct FileBackedPromotionEnv {
    pub _temp: TempDir,
    pub router: Router,
    pub metadata_path: String,
    pub data_path: String,
}

pub async fn setup_file_backed_promotion_env() -> FileBackedPromotionEnv {
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();

    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_tenant));

    FileBackedPromotionEnv {
        _temp: temp,
        router,
        metadata_path,
        data_path,
    }
}

pub async fn apply_promotion_yaml(router: &Router, manifest_yaml: &str) {
    let apply = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/promotions/apply")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": manifest_yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply");
    let apply_status = apply.status();
    let apply_body = apply
        .into_body()
        .collect()
        .await
        .expect("apply body")
        .to_bytes();
    assert_eq!(
        apply_status,
        StatusCode::OK,
        "apply failed: {}",
        String::from_utf8_lossy(&apply_body)
    );
}

pub async fn ingest_otlp_protobuf(router: Router, body: Vec<u8>) {
    let ingest = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("ingest");
    assert_eq!(ingest.status(), StatusCode::OK);
}

pub fn attach_softprobe_ducklake(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");
    connection
}

pub fn assert_traces_columns_exist(connection: &duckdb::Connection, columns: &[&str]) {
    for column in columns {
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_catalog = 'softprobe' AND table_name = 'traces' \
                     AND column_name = '{column}'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("column exists query");
        assert!(count > 0, "expected column {column}");
    }
}
