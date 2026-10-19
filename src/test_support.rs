//! Local router + config helpers for `#[cfg(test)]` modules (`make test` / `cargo test --lib` / `llvm-cov --lib`).
//! Mirrors `tests/util/config.rs` so unit tests do not depend on the integration-test crate.

use crate::api::{create_router, AppState};
use crate::config::Config;
use crate::ingest::IngestEngine;
use axum::Router;
use std::sync::Arc;
use tempfile::TempDir;

/// The test-stack runner supplies the mapped Postgres port for ignored tests.
/// Return `None` outside that isolated stack; never fall back to a developer DB.
pub fn test_postgres_port() -> Option<u16> {
    std::env::var("THELAKE_TEST_POSTGRES_PORT")
        .ok()?
        .parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
}

/// Point tests using production configuration at the test stack's published services.
/// Postgres is always required; MinIO is required only for configs that opt into
/// the default local object-store endpoint.
pub fn apply_test_service_endpoints(config: &mut Config) {
    let port = test_postgres_port()
        .expect("THELAKE_TEST_POSTGRES_PORT must be set by the isolated test stack");
    config.ducklake.metadata_path =
        format!("host=127.0.0.1 port={port} dbname=ducklake user=ducklake password=ducklake");
    if config
        .object_store
        .endpoint
        .as_deref()
        .is_some_and(|endpoint| endpoint == "http://localhost:9000")
    {
        config.object_store.endpoint = Some(
            std::env::var("THELAKE_TEST_MINIO_ENDPOINT")
                .expect("THELAKE_TEST_MINIO_ENDPOINT must be set by the isolated test stack"),
        );
    }
}

/// File-backed DuckLake under `temp`; compaction/metadata off; test-sized pools
/// via [`Config::shrink_pools_for_tests`].
pub fn file_backed_test_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    apply_test_service_endpoints(&mut config);
    // Test fixtures use the single ingest path with synchronous coalescing.
    config.ingest.flush_interval_seconds = 0;
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into_owned());

    let duck_dir = temp.path().join("ducklake");
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");

    // Isolate concurrent test runs in the shared local Postgres catalog.
    config.ducklake.metadata_schema = format!("thelake_test_{}", uuid::Uuid::new_v4().simple());
    config.ducklake.data_path = duck_dir.join("data").to_string_lossy().into_owned() + "/";

    config
}

/// Router + [`AppState`] from [`create_router`] (lazy tenant-bound engines).
pub async fn local_router_and_state() -> anyhow::Result<(Router, AppState, TempDir)> {
    let temp = TempDir::new()?;
    let config = Arc::new(file_backed_test_config(&temp));
    let (router, state) = create_router(config, None).await?;
    Ok((router, state, temp))
}

/// Builds the local test router (lazy [`WorkspaceManager`], same as production HTTP wiring).
pub async fn local_router() -> anyhow::Result<(Router, TempDir)> {
    let (router, _, temp) = local_router_and_state().await?;
    Ok((router, temp))
}

/// Ingest facade for unit tests that need durable DuckLake access without building a router.
pub async fn sample_ingest() -> anyhow::Result<(std::sync::Arc<IngestEngine>, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_test_config(&temp);
    let ingest = IngestEngine::bound_default(&config).await?;
    Ok((ingest, temp))
}
