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
use crate::session_summary::{DirtyHint, SessionSummaryDirty};
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
        session_summary_dirty: Option<Arc<SessionSummaryDirty>>,
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
            let dirty = session_summary_dirty.clone();
            CoalesceBuf::new(
                flush_interval_seconds,
                Arc::new(move |batches| {
                    let w = w.clone();
                    let tenant = tenant.clone();
                    let dirty = dirty.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        // Fold refs before moving batches into the writer (no Span clone).
                        let hints = dirty.as_ref().map(|_| {
                            crate::session_summary::fold_dirty_hints(batches.iter().flatten())
                        });
                        let r = w.write_span_batches(batches).await;
                        maybe_after_traces_commit(
                            r.is_ok(),
                            &tenant,
                            rows,
                            true,
                            hints.as_deref().unwrap_or(&[]),
                            dirty.as_deref(),
                        )
                        .await;
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
            // Flush-through: session_summary.enabled is rejected when flush==0, so
            // dirty is never wired here (no second dirty call site).
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

/// Apply traces commit side effects only when the lake write succeeded.
pub(crate) async fn maybe_after_traces_commit(
    write_ok: bool,
    tenant: &str,
    rows: u64,
    coalesced: bool,
    hints: &[DirtyHint],
    dirty: Option<&SessionSummaryDirty>,
) {
    if !write_ok {
        return;
    }
    crate::self_monitoring::record_ingest_commit(tenant, "traces", rows, coalesced);
    if let Some(dirty) = dirty {
        dirty.apply_hints(hints).await;
    }
}

#[cfg(test)]
mod after_commit_tests {
    use super::*;

    #[tokio::test]
    async fn maybe_after_traces_commit_skips_when_write_failed() {
        maybe_after_traces_commit(false, "t", 1, true, &[], None).await;
    }

    #[tokio::test]
    async fn maybe_after_traces_commit_ok_without_dirty() {
        maybe_after_traces_commit(true, "t", 1, true, &[], None).await;
    }

    #[test]
    fn dirty_handle_none_when_disabled() {
        let mut config = Config::default();
        config.session_summary.enabled = false;
        assert!(session_summary_dirty_for(&config, None, "t", "schema").is_none());
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
        let writer = Arc::new(DuckLakeWriter::new(config, tenant_ducklake.clone()).await?);
        let cache_dir = config.query.cache_dir.as_ref().map(PathBuf::from);
        let storage = Storage::new(writer);
        let dirty = session_summary_dirty_for(
            config,
            tenant_ducklake.as_ref(),
            "default",
            &config.ducklake.metadata_schema,
        );
        let ingest = Arc::new(IngestEngine::from_storage(
            Arc::new(storage.clone()),
            "default",
            config.ingest.flush_interval_seconds,
            dirty,
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
        let writer =
            Arc::new(DuckLakeWriter::new_scope_bound(&scoped_config, tenant_ducklake).await?);
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

/// Build dirty handle when session_summary is enabled (implies coalesce + postgres).
pub fn session_summary_dirty_for(
    config: &Config,
    resolver: Option<&DuckLakeScopeResolver>,
    tenant_id: &str,
    metadata_schema: &str,
) -> Option<Arc<SessionSummaryDirty>> {
    if !config.session_summary.enabled {
        return None;
    }
    let resolver = resolver?;
    Some(Arc::new(SessionSummaryDirty::new(
        resolver.pool().clone(),
        metadata_schema,
        tenant_id,
    )))
}
