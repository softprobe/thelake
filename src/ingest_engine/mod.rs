mod coalesce;

use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::models::{Log, Metric, Span};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::storage::ducklake::DuckLakeWriter;
use crate::storage::Storage;
use anyhow::Result;
use coalesce::CoalesceBuf;
use std::path::PathBuf;
use std::sync::Arc;

/// Operational ingest surface for one tenant-bound [`Storage`].
#[derive(Clone)]
pub struct IngestEngine {
    storage: Arc<Storage>,
    flush_interval_seconds: u64,
    logs: Option<Arc<CoalesceBuf<Log>>>,
    spans: Option<Arc<CoalesceBuf<Span>>>,
    metrics: Option<Arc<CoalesceBuf<Metric>>>,
}

impl IngestEngine {
    pub fn from_storage(storage: Arc<Storage>, flush_interval_seconds: u64) -> Self {
        let logs = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    Box::pin(async move { w.write_log_batches(batches).await })
                }),
            )
        });
        let spans = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    Box::pin(async move { w.write_span_batches(batches).await })
                }),
            )
        });
        let metrics = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    Box::pin(async move { w.write_metric_batches(batches).await })
                }),
            )
        });
        Self {
            storage,
            flush_interval_seconds,
            logs,
            spans,
            metrics,
        }
    }

    pub fn writer(&self) -> Arc<DuckLakeWriter> {
        self.storage.writer.clone()
    }

    pub async fn add_spans(&self, items: Vec<Span>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(buf) = &self.spans {
            buf.enqueue(items).await
        } else {
            self.storage.writer.write_span_batches(vec![items]).await
        }
    }

    pub async fn add_logs(&self, items: Vec<Log>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(buf) = &self.logs {
            buf.enqueue(items).await
        } else {
            self.storage.writer.write_log_batches(vec![items]).await
        }
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(buf) = &self.metrics {
            buf.enqueue(items).await
        } else {
            self.storage.writer.write_metric_batches(vec![items]).await
        }
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        if let Some(buf) = &self.spans {
            buf.force_flush().await
        } else {
            Ok(())
        }
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        if let Some(buf) = &self.logs {
            buf.force_flush().await
        } else {
            Ok(())
        }
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        if let Some(buf) = &self.metrics {
            buf.force_flush().await
        } else {
            Ok(())
        }
    }

    pub fn flush_interval_seconds(&self) -> u64 {
        self.flush_interval_seconds
    }
}

/// Test / single-tenant pipeline with a long-lived [`IngestEngine`] (shared coalesce state).
#[derive(Clone)]
pub struct IngestPipeline {
    pub storage: Storage,
    /// Shared with maintenance scheduler for TTL prune.
    pub dropdown_catalog: Option<Arc<DropdownCatalog>>,
    cache_dir: Option<PathBuf>,
    ingest: Arc<IngestEngine>,
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let dropdown_catalog = DropdownCatalog::connect(config).await?;
        let tenant_ducklake = DuckLakeScopeResolver::connect(config).await?;
        let writer =
            Arc::new(DuckLakeWriter::new(config, dropdown_catalog.clone(), tenant_ducklake).await?);
        let cache_dir = config.query.cache_dir.as_ref().map(PathBuf::from);
        let storage = Storage::new(writer);
        let ingest = Arc::new(IngestEngine::from_storage(
            Arc::new(storage.clone()),
            config.ingest.flush_interval_seconds,
        ));

        Ok(Self {
            storage,
            dropdown_catalog,
            cache_dir,
            ingest,
        })
    }

    /// Build [`Storage`] (tenant-bound writer) for one registry row.
    pub async fn build_tenant_storage(
        config: &Config,
        dropdown_catalog: Option<Arc<DropdownCatalog>>,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
        _tenant_id: String,
        scope: DuckLakeScope,
    ) -> Result<Storage> {
        let mut scoped_config = config.clone();
        scoped_config.ducklake.metadata_schema = scope.metadata_schema;
        scoped_config.ducklake.data_path = scope.data_path;
        let writer = Arc::new(
            DuckLakeWriter::new_scope_bound(&scoped_config, dropdown_catalog, tenant_ducklake)
                .await?,
        );
        Ok(Storage::new(writer))
    }

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        self.ingest.add_spans(items, request_size).await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        self.ingest.add_logs(items, request_size).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        self.ingest.add_metrics(items, request_size).await
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        self.storage.writer.write_span_batches(batches).await
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        self.storage.writer.write_log_batches(batches).await
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        self.storage.writer.write_metric_batches(batches).await
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.ingest.force_flush_spans().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.ingest.force_flush_logs().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.ingest.force_flush_metrics().await
    }

    pub fn writer(&self) -> Arc<DuckLakeWriter> {
        self.storage.writer.clone()
    }

    pub fn cache_dir(&self) -> Option<PathBuf> {
        self.cache_dir.clone()
    }

    pub fn ingest_engine(&self) -> Arc<IngestEngine> {
        self.ingest.clone()
    }
}
