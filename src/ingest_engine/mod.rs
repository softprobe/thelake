//! Soft coalesce ingest for one tenant-bound [`Storage`].
//!
//! # CPU / PromQL coupling
//! OTLP enqueues into a per-signal coalesce buffer and ticks a background flush
//! worker (`flush_interval_seconds` / eager depth). Enqueue never writes the lake.
//! PromQL range answers stay in the HTTP cache across commits (TTL + start/end
//! buckets); wiping that cache on every flush forced dashboard refreshes to
//! re-scan Parquet and pegged query CPU.

mod coalesce;

use crate::config::{resolve_write_timeout_seconds, Config};
use crate::models::{Log, Metric, Span};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::session_summary::{DirtyHint, SessionSummaryDirty};
use crate::storage::ducklake::DuckLakeWriter;
use crate::storage::Storage;
use anyhow::{anyhow, Result};
use coalesce::CoalesceBuf;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Operational ingest surface for one tenant-bound [`Storage`].
#[derive(Clone)]
pub struct IngestEngine {
    storage: Arc<Storage>,
    flush_interval_seconds: u64,
    logs: Arc<CoalesceBuf<Log>>,
    spans: Arc<CoalesceBuf<Span>>,
    metrics: Arc<CoalesceBuf<Metric>>,
}

/// Bound a DuckLake write so a hung INSERT cannot stall the coalesce worker forever.
/// `timeout_secs == 0` disables the wall clock (tests / explicit opt-out).
pub(crate) async fn ducklake_write_with_timeout<F>(timeout_secs: u64, fut: F) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let secs = resolve_write_timeout_seconds(timeout_secs);
    if secs == 0 {
        return fut.await;
    }
    match tokio::time::timeout(Duration::from_secs(secs), fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow!("DuckLake ingest write timed out after {secs}s")),
    }
}

/// Soft coalesce for one signal: timed write, then either plain ingest monitor
/// (`dirty_sync == None`) or traces after-commit with optional session-summary
/// dirty hints folded **before** the write consumes batches.
fn monitored_signal_buf<T, Fut, W>(
    flush_interval_seconds: u64,
    max_pending: usize,
    eager_pending: usize,
    write_timeout_seconds: u64,
    writer: Arc<DuckLakeWriter>,
    tenant: String,
    signal: &'static str,
    write: W,
    dirty_sync: Option<(Option<Arc<SessionSummaryDirty>>, fn(&[Vec<T>]) -> Vec<DirtyHint>)>,
) -> Arc<CoalesceBuf<T>>
where
    T: Send + 'static,
    W: Fn(Arc<DuckLakeWriter>, Vec<Vec<T>>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    CoalesceBuf::with_limits(
        flush_interval_seconds,
        max_pending,
        eager_pending,
        Arc::new(move |batches| {
            let w = writer.clone();
            let tenant = tenant.clone();
            let write = write.clone();
            let dirty_sync = dirty_sync.clone();
            Box::pin(async move {
                let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                let hints = match &dirty_sync {
                    Some((Some(_), fold)) => fold(&batches),
                    _ => Vec::new(),
                };
                let dirty = dirty_sync.as_ref().and_then(|(d, _)| d.clone());
                let r = ducklake_write_with_timeout(write_timeout_seconds, write(w, batches)).await;
                if dirty_sync.is_some() {
                    maybe_after_traces_commit(
                        r.is_ok(),
                        &tenant,
                        rows,
                        true,
                        &hints,
                        dirty.as_deref(),
                    )
                    .await;
                } else if r.is_ok() {
                    crate::self_monitoring::record_ingest_commit(&tenant, signal, rows, true);
                }
                r
            })
        }),
    )
}

impl IngestEngine {
    pub fn from_storage(
        storage: Arc<Storage>,
        tenant_id: impl Into<String>,
        flush_interval_seconds: u64,
        buffer_size_mb: u64,
        write_timeout_seconds: u64,
        session_summary_dirty: Option<Arc<SessionSummaryDirty>>,
    ) -> Self {
        let tenant_id = tenant_id.into();
        let (max_pending, eager_pending) = coalesce::resolve_byte_limits(buffer_size_mb);
        let write_timeout_seconds = resolve_write_timeout_seconds(write_timeout_seconds);
        let logs = monitored_signal_buf(
            flush_interval_seconds,
            max_pending,
            eager_pending,
            write_timeout_seconds,
            storage.writer.clone(),
            tenant_id.clone(),
            "logs",
            |w, b| async move { w.write_log_batches(b).await },
            None,
        );
        let spans = monitored_signal_buf(
            flush_interval_seconds,
            max_pending,
            eager_pending,
            write_timeout_seconds,
            storage.writer.clone(),
            tenant_id.clone(),
            "traces",
            |w, b| async move { w.write_span_batches(b).await },
            Some((session_summary_dirty, |batches| {
                crate::session_summary::fold_dirty_hints(batches.iter().flatten())
            })),
        );
        // Do not invalidate PromQL range cache on coalesce commits — TTL covers
        // freshness; wipe-on-flush pegs Grafana refresh CPU (see module docs).
        let metrics = monitored_signal_buf(
            flush_interval_seconds,
            max_pending,
            eager_pending,
            write_timeout_seconds,
            storage.writer.clone(),
            tenant_id,
            "metrics",
            |w, b| async move { w.write_metric_batches(b).await },
            None,
        );
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

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.spans.enqueue(items, request_size).await?;
        // Interval 0: callers expect drain before return (HTTP 200 ⇒ readable).
        if self.flush_interval_seconds == 0 {
            self.spans.force_flush().await?;
        }
        Ok(())
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.logs.enqueue(items, request_size).await?;
        if self.flush_interval_seconds == 0 {
            self.logs.force_flush().await?;
        }
        Ok(())
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.metrics.enqueue(items, request_size).await?;
        if self.flush_interval_seconds == 0 {
            self.metrics.force_flush().await?;
        }
        Ok(())
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.spans.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.logs.force_flush().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.metrics.force_flush().await
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
    fn dirty_handle_none_when_sqlite_catalog() {
        let config = Config::default(); // sqlite
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
            config.ingest.buffer_size_mb,
            config.ingest.write_timeout_seconds,
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

/// Build dirty handle when catalog is postgres (session_summary always on).
pub fn session_summary_dirty_for(
    config: &Config,
    resolver: Option<&DuckLakeScopeResolver>,
    tenant_id: &str,
    metadata_schema: &str,
) -> Option<Arc<SessionSummaryDirty>> {
    if !crate::config::SessionSummaryConfig::active_for(&config.ducklake) {
        return None;
    }
    let resolver = resolver?;
    Some(Arc::new(SessionSummaryDirty::new(
        resolver.pool().clone(),
        metadata_schema,
        tenant_id,
    )))
}

#[cfg(test)]
mod write_timeout_tests {
    use super::ducklake_write_with_timeout;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn write_timeout_disabled_waits_for_completion() {
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        ducklake_write_with_timeout(0, async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            d.store(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn write_timeout_fails_hung_write() {
        let err = ducklake_write_with_timeout(1, async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        })
        .await;
        assert!(err.is_err(), "expected timeout error");
        let msg = format!("{:#}", err.unwrap_err());
        assert!(msg.contains("timed out"), "unexpected error message: {msg}");
    }

    #[tokio::test]
    async fn write_timeout_allows_fast_write() {
        ducklake_write_with_timeout(5, async { Ok(()) })
            .await
            .unwrap();
    }
}
