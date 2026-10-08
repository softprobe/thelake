//! Soft coalesce ingest for one tenant-bound writer.
//!
//! OTLP enqueues into a per-signal coalesce buffer and ticks a background flush
//! worker (`flush_interval_seconds` / eager depth). Enqueue never writes the lake.

mod coalesce;

use crate::config::{resolve_write_timeout_seconds, Config};
use crate::models::{Log, Score, ScoreConfig, Span};
use crate::session_summary::{DirtyHint, SessionSummaryDirty};
use crate::storage::ducklake::DuckLakeWriter;
use crate::workspace::DuckLakeScopeResolver;
use crate::workspace_scope::{WorkspaceBinding, WorkspaceScopeMode, DEFAULT_WORKSPACE_ID};
use anyhow::{anyhow, Result};
use coalesce::CoalesceBuf;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Operational ingest surface for one authenticated workspace.
#[derive(Clone)]
pub struct IngestEngine {
    writer: Arc<DuckLakeWriter>,
    workspace_id: String,
    flush_interval_seconds: u64,
    logs: Arc<CoalesceBuf<Log>>,
    spans: Arc<CoalesceBuf<Span>>,
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

impl IngestEngine {
    /// Bind ingest to one workspace: create the writer, optional atomic dev reset,
    /// and coalesce flush workers that load promotion manifests before each write.
    pub(crate) async fn bound(
        config: &Config,
        resolver: DuckLakeScopeResolver,
        binding: WorkspaceBinding,
    ) -> Result<Arc<Self>> {
        let workspace_id = binding.workspace_id.clone();
        let writer = Arc::new(DuckLakeWriter::new(config, binding).await?);

        if std::env::var("SPLAKE_RESET_DUCKLAKE").ok().as_deref() == Some("1") {
            let schema = writer.metadata_schema();
            let telemetry = resolver
                .load_active_telemetry_columns_manifests_for_scope(schema)
                .await?;
            let business = resolver
                .load_active_business_table_manifests_for_scope(schema)
                .await?;
            writer
                .apply_dev_reset_if_requested(&telemetry, &business)
                .await?;
        }

        let dirty =
            session_summary_dirty_for(config, &resolver, &workspace_id, writer.metadata_schema());
        Ok(Arc::new(Self::from_writer(
            writer,
            resolver,
            workspace_id,
            config.ingest.flush_interval_seconds,
            config.ingest.buffer_size_mb,
            config.ingest.write_timeout_seconds,
            dirty,
        )))
    }

    /// Test/default helper: connect the registry and bind [`DEFAULT_WORKSPACE_ID`].
    pub async fn bound_default(config: &Config) -> Result<Arc<Self>> {
        let resolver = DuckLakeScopeResolver::connect(config).await?;
        let binding = WorkspaceBinding::new(
            DEFAULT_WORKSPACE_ID,
            resolver.default_physical_scope().clone(),
            config.ducklake.workspace_scope_mode,
        )
        .map_err(|error| anyhow!(error))?;
        let ingest = Self::bound(config, resolver, binding).await?;
        // Match former IngestPipeline::new: ensure physical schema before any
        // query worker or leftover-table fail-fast checks attach the catalog.
        ingest.ensure_shared_schema().await?;
        Ok(ingest)
    }

    fn from_writer(
        writer: Arc<DuckLakeWriter>,
        resolver: DuckLakeScopeResolver,
        workspace_id: impl Into<String>,
        flush_interval_seconds: u64,
        buffer_size_mb: u64,
        write_timeout_seconds: u64,
        session_summary_dirty: Option<Arc<SessionSummaryDirty>>,
    ) -> Self {
        let workspace_id = workspace_id.into();
        let (max_pending, eager_pending) = coalesce::resolve_byte_limits(buffer_size_mb);
        let write_timeout_seconds = resolve_write_timeout_seconds(write_timeout_seconds);
        let logs = {
            let writer = writer.clone();
            let resolver = resolver.clone();
            let tenant = workspace_id.clone();
            CoalesceBuf::with_limits(
                flush_interval_seconds,
                max_pending,
                eager_pending,
                Arc::new(move |batches| {
                    let w = writer.clone();
                    let resolver = resolver.clone();
                    let tenant = tenant.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        let manifests = resolver
                            .load_active_telemetry_columns_manifests_for_scope(w.metadata_schema())
                            .await?;
                        let commit_started = std::time::Instant::now();
                        let r = ducklake_write_with_timeout(
                            write_timeout_seconds,
                            w.write_log_batches(&manifests, batches),
                        )
                        .await;
                        if r.is_ok() {
                            crate::self_monitoring::record_ingest_commit(
                                &tenant,
                                "logs",
                                rows,
                                true,
                                commit_started.elapsed(),
                            );
                        }
                        r
                    })
                }),
            )
        };
        let spans = {
            let writer = writer.clone();
            let resolver = resolver.clone();
            let tenant = workspace_id.clone();
            let dirty = session_summary_dirty;
            CoalesceBuf::with_limits(
                flush_interval_seconds,
                max_pending,
                eager_pending,
                Arc::new(move |batches| {
                    let w = writer.clone();
                    let resolver = resolver.clone();
                    let tenant = tenant.clone();
                    let dirty = dirty.clone();
                    Box::pin(async move {
                        let rows: u64 = batches.iter().map(|b| b.len() as u64).sum();
                        // Fold before the storage commit because the commit consumes batches.
                        let hints = if dirty.is_some() {
                            crate::session_summary::fold_dirty_hints(batches.iter().flatten())
                        } else {
                            Vec::new()
                        };
                        let manifests = resolver
                            .load_active_telemetry_columns_manifests_for_scope(w.metadata_schema())
                            .await?;
                        let commit_started = std::time::Instant::now();
                        let r = ducklake_write_with_timeout(
                            write_timeout_seconds,
                            w.write_span_batches(&manifests, batches),
                        )
                        .await;
                        maybe_after_traces_commit(
                            r.is_ok(),
                            &tenant,
                            rows,
                            true,
                            commit_started.elapsed(),
                            &hints,
                            dirty.as_deref(),
                        )
                        .await;
                        r
                    })
                }),
            )
        };
        Self {
            writer,
            workspace_id,
            flush_interval_seconds,
            logs,
            spans,
        }
    }

    pub async fn add_spans(&self, mut items: Vec<Span>, request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.bind_spans_to_workspace(&mut items);
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
        let mut items = items;
        self.bind_logs_to_workspace(&mut items);
        self.logs.enqueue(items, request_size).await?;
        if self.flush_interval_seconds == 0 {
            self.logs.force_flush().await?;
        }
        Ok(())
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.spans.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.logs.force_flush().await
    }

    pub fn flush_interval_seconds(&self) -> u64 {
        self.flush_interval_seconds
    }

    pub(crate) async fn ensure_shared_schema(&self) -> Result<()> {
        self.writer.ensure_shared_schema().await
    }

    /// Add scores through the authenticated workspace write path.
    pub async fn add_scores(&self, items: Vec<Score>) -> Result<()> {
        let mut items = items;
        self.bind_scores_to_workspace(&mut items);
        self.writer.write_score_batches(vec![items]).await
    }

    /// Add score configurations through the authenticated workspace write path.
    pub async fn add_score_configs(&self, items: Vec<ScoreConfig>) -> Result<()> {
        let mut items = items;
        self.bind_score_configs_to_workspace(&mut items);
        self.writer.write_score_config_batches(vec![items]).await
    }

    fn bind_spans_to_workspace(&self, spans: &mut [Span]) {
        for span in spans {
            span.workspace_id = Some(self.workspace_id.clone());
        }
    }

    fn bind_logs_to_workspace(&self, logs: &mut [Log]) {
        for log in logs {
            log.workspace_id = Some(self.workspace_id.clone());
        }
    }

    fn bind_scores_to_workspace(&self, scores: &mut [Score]) {
        for score in scores {
            score.workspace_id = Some(self.workspace_id.clone());
        }
    }

    fn bind_score_configs_to_workspace(&self, configs: &mut [ScoreConfig]) {
        for config in configs {
            config.workspace_id = Some(self.workspace_id.clone());
        }
    }

    pub(crate) fn writer_handle(&self) -> &Arc<DuckLakeWriter> {
        &self.writer
    }
}

/// Apply traces commit side effects only when the lake write succeeded.
pub(crate) async fn maybe_after_traces_commit(
    write_ok: bool,
    tenant: &str,
    rows: u64,
    coalesced: bool,
    commit_elapsed: std::time::Duration,
    hints: &[DirtyHint],
    dirty: Option<&SessionSummaryDirty>,
) {
    if !write_ok {
        return;
    }
    crate::self_monitoring::record_ingest_commit(tenant, "traces", rows, coalesced, commit_elapsed);
    if let Some(dirty) = dirty {
        dirty.apply_hints(hints).await;
    }
}

#[cfg(test)]
mod after_commit_tests {
    use super::*;

    #[tokio::test]
    async fn maybe_after_traces_commit_skips_when_write_failed() {
        maybe_after_traces_commit(false, "t", 1, true, std::time::Duration::ZERO, &[], None).await;
    }

    #[tokio::test]
    async fn maybe_after_traces_commit_ok_without_dirty() {
        maybe_after_traces_commit(
            true,
            "t",
            1,
            true,
            std::time::Duration::from_millis(1),
            &[],
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn ingest_rebinds_spans_to_its_workspace() {
        let (engine, _temp) = crate::test_support::sample_ingest()
            .await
            .expect("sample ingest");
        let mut spans = vec![crate::session_summary::test_span::span_at("s", 1)];
        spans[0].workspace_id = Some("spoofed-tenant".to_string());

        engine.bind_spans_to_workspace(&mut spans);

        assert_eq!(spans[0].workspace_id.as_deref(), Some(DEFAULT_WORKSPACE_ID));
    }
}

/// Build the durable dirty handle for one tenant's session_summary queue.
pub(crate) fn session_summary_dirty_for(
    config: &Config,
    resolver: &DuckLakeScopeResolver,
    workspace_id: &str,
    metadata_schema: &str,
) -> Option<Arc<SessionSummaryDirty>> {
    let dirty = if config.ducklake.workspace_scope_mode == WorkspaceScopeMode::Shared {
        SessionSummaryDirty::new_for_workspace(
            resolver.pool().clone(),
            metadata_schema,
            workspace_id,
        )
    } else {
        SessionSummaryDirty::new(resolver.pool().clone(), metadata_schema, workspace_id)
    };
    Some(Arc::new(dirty))
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
