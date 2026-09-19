//! Soft coalesce + flush-through ingest for one tenant-bound [`Storage`].
//!
//! # CPU / PromQL coupling
//! When `flush_interval_seconds > 0`, OTLP acks on enqueue and a timer drains
//! capped batches into DuckLake. PromQL range answers stay in the HTTP cache
//! across commits (TTL + start/end buckets); wiping that cache on every flush
//! forced dashboard refreshes to re-scan Parquet and pegged query CPU.

mod coalesce;

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
    tenant_id: String,
    flush_interval_seconds: u64,
    logs: Option<Arc<CoalesceBuf<Log>>>,
    spans: Option<Arc<CoalesceBuf<Span>>>,
    metrics: Option<Arc<CoalesceBuf<Metric>>>,
}

impl IngestEngine {
    pub fn from_storage(
        storage: Arc<Storage>,
        tenant_id: impl Into<String>,
        flush_interval_seconds: u64,
    ) -> Self {
        let tenant_id = tenant_id.into();
        let logs = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            let tenant = tenant_id.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    let tenant = tenant.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        let r = w.write_log_batches(batches).await;
                        if r.is_ok() {
                            crate::self_monitoring::record_ingest_commit(
                                &tenant, "logs", rows, true,
                            );
                        }
                        r
                    })
                }),
            )
        });
        let spans = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            let tenant = tenant_id.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    let tenant = tenant.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        let r = w.write_span_batches(batches).await;
                        if r.is_ok() {
                            crate::self_monitoring::record_ingest_commit(
                                &tenant, "traces", rows, true,
                            );
                        }
                        r
                    })
                }),
            )
        });
        let metrics = (flush_interval_seconds > 0).then(|| {
            let w = storage.writer.clone();
            let tenant = tenant_id.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    let tenant = tenant.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        let r = w.write_metric_batches(batches).await;
                        if r.is_ok() {
                            crate::self_monitoring::record_ingest_commit(
                                &tenant, "metrics", rows, true,
                            );
                            // Do not invalidate PromQL range cache on coalesce
                            // commits — TTL covers freshness; wipe-on-flush pegs
                            // Grafana refresh CPU (see module docs).
                        }
                        r
                    })
                }),
            )
        });
        Self {
            storage,
            tenant_id,
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
            let rows = items.len() as u64;
            let r = self.storage.writer.write_span_batches(vec![items]).await;
            if r.is_ok() {
                crate::self_monitoring::record_ingest_commit(
                    &self.tenant_id,
                    "traces",
                    rows,
                    false,
                );
            }
            r
        }
    }

    pub async fn add_logs(&self, items: Vec<Log>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(buf) = &self.logs {
            buf.enqueue(items).await
        } else {
            let rows = items.len() as u64;
            let r = self.storage.writer.write_log_batches(vec![items]).await;
            if r.is_ok() {
                crate::self_monitoring::record_ingest_commit(&self.tenant_id, "logs", rows, false);
            }
            r
        }
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(buf) = &self.metrics {
            buf.enqueue(items).await
        } else {
            let rows = items.len() as u64;
            let r = self.storage.writer.write_metric_batches(vec![items]).await;
            if r.is_ok() {
                crate::self_monitoring::record_ingest_commit(
                    &self.tenant_id,
                    "metrics",
                    rows,
                    false,
                );
                crate::compat::prometheus::invalidate_range_result_cache();
            }
            r
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
    cache_dir: Option<PathBuf>,
    ingest: Arc<IngestEngine>,
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let tenant_ducklake = DuckLakeScopeResolver::connect(config).await?;
        let writer = Arc::new(DuckLakeWriter::new(config, tenant_ducklake).await?);
        let cache_dir = config.query.cache_dir.as_ref().map(PathBuf::from);
        let storage = Storage::new(writer);
        let ingest = Arc::new(IngestEngine::from_storage(
            Arc::new(storage.clone()),
            "default",
            config.ingest.flush_interval_seconds,
        ));

        Ok(Self {
            storage,
            cache_dir,
            ingest,
        })
    }

    /// Build [`Storage`] (tenant-bound writer) for one registry row.
    pub async fn build_tenant_storage(
        config: &Config,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
        _tenant_id: String,
        scope: DuckLakeScope,
    ) -> Result<Storage> {
        let mut scoped_config = config.clone();
        scoped_config.ducklake.metadata_schema = scope.metadata_schema;
        scoped_config.ducklake.data_path = scope.data_path;
        let writer = Arc::new(
            DuckLakeWriter::new_scope_bound(&scoped_config, tenant_ducklake).await?,
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
