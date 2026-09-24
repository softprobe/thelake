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
use softprobe_runtime::config::DuckLakeConfig;
use softprobe_runtime::runtime_api::runtime_control_routes;
use softprobe_runtime::storage::ducklake::{open_attached_from_config, AttachedSession};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::tenant::{
    inject_local_sqlite_tenant as inject_tenant, provision_local_sqlite_tenant,
};

pub struct FileBackedPromotionEnv {
    pub _temp: TempDir,
    pub router: Router,
    ducklake: DuckLakeConfig,
}

impl FileBackedPromotionEnv {
    pub fn open_attached(&self) -> AttachedSession {
        open_attached_from_config(&self.ducklake, Some(0))
    }
}

pub async fn setup_file_backed_promotion_env() -> FileBackedPromotionEnv {
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let ducklake = config.ducklake.clone();

    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    provision_local_sqlite_tenant(&state).await;
    let router = router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_tenant));

    FileBackedPromotionEnv {
        _temp: temp,
        router,
        ducklake,
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
    let status = ingest.status();
    let bytes = axum::body::to_bytes(ingest.into_body(), usize::MAX)
        .await
        .expect("ingest body");
    assert_eq!(
        status,
        StatusCode::OK,
        "ingest failed: {}",
        String::from_utf8_lossy(&bytes)
    );
}

pub async fn ingest_otlp_logs_protobuf(router: Router, body: Vec<u8>) {
    let ingest = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("ingest logs");
    assert_eq!(ingest.status(), StatusCode::OK);
}

pub fn assert_traces_columns_exist(connection: &duckdb::Connection, columns: &[&str]) {
    assert_table_columns_exist(connection, "traces", columns);
}

pub fn assert_logs_columns_exist(connection: &duckdb::Connection, columns: &[&str]) {
    assert_table_columns_exist(connection, "logs", columns);
}

fn assert_table_columns_exist(connection: &duckdb::Connection, table: &str, columns: &[&str]) {
    for column in columns {
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_catalog = 'softprobe' AND table_name = '{table}' \
                     AND column_name = '{column}'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("column exists query");
        assert!(count > 0, "expected {table}.{column}");
    }
}
