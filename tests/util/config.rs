//! Shared [`softprobe_runtime::config::Config`] for integration tests (local DuckLake, no MinIO).

use softprobe_runtime::config::Config;
use tempfile::TempDir;

/// Minimal file-backed DuckLake config under `temp`, maintenance/compaction off for quiet tests.
/// Twin of `softprobe_runtime::test_support::file_backed_test_config` (lib `cfg(test)` cannot
/// be imported from the integration crate); pool limits fold into [`Config::shrink_pools_for_tests`].
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
