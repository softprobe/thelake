//! Local router + config helpers for `#[cfg(test)]` modules (`make test` / `cargo test --lib` / `llvm-cov --lib`).
//! Mirrors `tests/util/config.rs` so unit tests do not depend on the integration-test crate.

use crate::api::ingestion::traces::ingest_traces;
use crate::api::{create_router, AppState};
use crate::config::Config;
use crate::ingest_engine::IngestEngine;
use axum::routing::post;
use axum::Router;
use std::sync::Arc;
use tempfile::TempDir;

/// File-backed DuckLake under `temp`; compaction/metadata off; test-sized pools
/// via [`Config::shrink_pools_for_tests`].
pub fn file_backed_test_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
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
    let (router, state) = create_router(config, post(ingest_traces), None).await?;
    Ok((router, state, temp))
}

/// Builds the local test router (lazy [`RuntimeEngineManager`], same as production HTTP wiring).
pub async fn local_router() -> anyhow::Result<(Router, TempDir)> {
    let (router, _, temp) = local_router_and_state().await?;
    Ok((router, temp))
}

/// Ingest facade for unit tests that need durable DuckLake access without building a router.
pub async fn sample_ingest() -> anyhow::Result<(std::sync::Arc<IngestEngine>, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_test_config(&temp);
    let pipeline = crate::ingest_engine::IngestPipeline::new(&config).await?;
    Ok((pipeline.ingest_engine(), temp))
}
