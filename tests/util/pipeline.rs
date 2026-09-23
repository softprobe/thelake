use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::query::{self, QueryEngine};
use tempfile::TempDir;
use uuid::Uuid;

pub struct TestPipeline {
    pub cache_dir: TempDir,
    pub pipeline: IngestPipeline,
    query_engine: QueryEngine,
}

impl TestPipeline {
    pub async fn new(mut config: Config) -> Self {
        let cache_dir = TempDir::new().expect("tempdir");
        config.query.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
        let run_id = Uuid::new_v4();
        // Isolate concurrent test runs in the shared local Postgres catalog.
        config.ducklake.metadata_schema = format!("perf_{}", run_id.simple());
        if config.ducklake.data_path.contains("://") {
            // Keep object-storage-backed paths for integration validation, but isolate each run.
            let base = config.ducklake.data_path.trim_end_matches('/');
            config.ducklake.data_path = format!("{}/tests/{}/", base, run_id);
        } else {
            // Default to object storage for integration tests to validate committed data persistence.
            config.ducklake.data_path = format!("s3://warehouse/ducklake/tests/{}/", run_id);
        }
        let pipeline = IngestPipeline::new(&config).await.expect("ingest pipeline");

        // Query-engine worker start can flake under CI load (ATTACH / Postgres
        // catalog busy after a long suite). One retry is enough in practice.
        let query_engine = match query::create_query_engine(&config).await {
            Ok(engine) => engine,
            Err(first) => {
                eprintln!("query engine start failed ({first}); retrying once...");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                query::create_query_engine(&config)
                    .await
                    .unwrap_or_else(|second| {
                        panic!("query engine: {first}; retry: {second}");
                    })
            }
        };

        Self {
            cache_dir,
            pipeline,
            query_engine,
        }
    }

    pub async fn execute_query(
        &self,
        sql: &str,
    ) -> anyhow::Result<softprobe_runtime::query::duckdb::QueryResult> {
        self.query_engine.execute_query(sql).await
    }

    pub fn query_engine(&self) -> &QueryEngine {
        &self.query_engine
    }
}
