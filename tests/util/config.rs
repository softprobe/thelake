//! Shared [`softprobe_runtime::config::Config`] for integration tests (local DuckLake, no MinIO).

use softprobe_runtime::config::Config;
use tempfile::TempDir;

/// Local DuckLake Postgres used by `make setup` / docker-compose.
pub const TEST_DUCKLAKE_POSTGRES_DSN: &str =
    "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake";

/// Minimal file-backed DuckLake config under `temp`, maintenance/compaction off for quiet tests.
/// Twin of `softprobe_runtime::test_support::file_backed_test_config` (lib `cfg(test)` cannot
/// be imported from the integration crate); pool limits fold into [`Config::shrink_pools_for_tests`].
pub fn file_backed_test_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into_owned());

    let duck_dir = temp.path().join("ducklake");
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");

    config.ducklake.catalog_type = "sqlite".to_string();
    config.ducklake.metadata_path = duck_dir
        .join("metadata.sqlite")
        .to_string_lossy()
        .into_owned();
    config.ducklake.data_path = duck_dir.join("data").to_string_lossy().into_owned() + "/";

    config
}

/// PostgreSQL catalog + local filesystem `data_path` under `temp`.
///
/// Uses a unique `metadata_schema` so parallel e2e tests do not collide on the
/// shared `ducklake-postgres` instance from `make setup`.
pub fn postgres_backed_test_config(temp: &TempDir, metadata_schema: &str) -> Config {
    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into_owned());

    let data_path = temp.path().join("ducklake").join("data");
    std::fs::create_dir_all(&data_path).expect("ducklake data");

    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path = TEST_DUCKLAKE_POSTGRES_DSN.to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    config.ducklake.metadata_schema = metadata_schema.to_string();
    config.ducklake.data_path = data_path.to_string_lossy().into_owned() + "/";

    config
}
