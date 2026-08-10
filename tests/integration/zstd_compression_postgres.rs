//! PostgreSQL adapter for the shared DuckLake Parquet ZSTD compression contract.

use async_trait::async_trait;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::AppState;
use softprobe_runtime::config::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

use crate::util::config::postgres_backed_test_config;
use crate::util::zstd_compression_contract::{
    contract_compaction_keeps_parquet_zstd, contract_ingest_parquet_zstd, ZstdCompressionBackend,
};

struct PostgresBackend {
    _temp: TempDir,
    router: Router,
    state: AppState,
    config: Arc<Config>,
    data_path: PathBuf,
}

async fn setup() -> PostgresBackend {
    let temp = TempDir::new().expect("tempdir");
    let short = &Uuid::new_v4().simple().to_string()[..8];
    let mut config = postgres_backed_test_config(&temp, &format!("sp_zstd_{short}"));
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.target_file_size_bytes = 64 * 1024 * 1024;
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    let data_path = PathBuf::from(&config.ducklake.data_path);
    let config = Arc::new(config);
    let (router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");

    PostgresBackend {
        _temp: temp,
        router,
        state,
        config,
        data_path,
    }
}

#[async_trait]
impl ZstdCompressionBackend for PostgresBackend {
    fn router(&self) -> Router {
        self.router.clone()
    }

    fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    fn data_path(&self) -> &Path {
        &self.data_path
    }

    fn bearer_token(&self) -> Option<&str> {
        None
    }

    async fn flush_spans(&self) {
        self.state
            .engine_for_id("")
            .await
            .expect("engine")
            .ingest
            .force_flush_spans()
            .await
            .expect("flush spans");
    }
}

#[tokio::test]
async fn postgres_ingest_parquet_zstd_contract() {
    contract_ingest_parquet_zstd(&setup().await).await;
}

#[tokio::test]
async fn postgres_compaction_keeps_parquet_zstd_contract() {
    contract_compaction_keeps_parquet_zstd(&setup().await).await;
}
