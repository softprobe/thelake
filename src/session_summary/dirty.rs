//! Ephemeral fold of one flush batch → durable dirty UPSERT (process-stateless).

use crate::models::Span;
use crate::workspace::quote_pg_ident;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::warn;

const DIRTY_UPSERT_SQL: &str = include_str!("../sql/session_summary/dirty_upsert.sql");
const WORKSPACE_DIRTY_UPSERT_SQL: &str =
    include_str!("../sql/session_summary/dirty_upsert_workspace.sql");

pub const PER_AGENT_DIRTY_HINT_WINDOW: Duration = Duration::from_secs(60);
pub const PER_AGENT_DIRTY_HINT_MAX_SESSIONS: usize = 1_024;
const MAX_DIRTY_WINDOW_AGENTS: usize = 4_096;

#[derive(Debug)]
struct AgentDirtyWindow {
    window_started_at: Instant,
    seen_sessions: HashSet<String>,
}

#[derive(Default)]
struct DirtyHintLimiter {
    windows: Mutex<HashMap<String, AgentDirtyWindow>>,
}

impl DirtyHintLimiter {
    fn allow_at(&self, span: &Span, now: Instant) -> bool {
        let sid = span.session_id.trim();
        if sid.is_empty() {
            return false;
        }
        let Some(agent_id) = span
            .agent_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        else {
            return true;
        };
        let mut guard = self.windows.lock().expect("dirty hint limiter lock");
        if !guard.contains_key(agent_id) && guard.len() >= MAX_DIRTY_WINDOW_AGENTS {
            guard.retain(|_, win| {
                now.saturating_duration_since(win.window_started_at) < PER_AGENT_DIRTY_HINT_WINDOW
            });
            if guard.len() >= MAX_DIRTY_WINDOW_AGENTS {
                if let Some(oldest_key) = guard
                    .iter()
                    .min_by_key(|(_, win)| win.window_started_at)
                    .map(|(k, _)| k.clone())
                {
                    guard.remove(&oldest_key);
                }
            }
        }
        let win = guard
            .entry(agent_id.to_owned())
            .or_insert_with(|| AgentDirtyWindow {
                window_started_at: now,
                seen_sessions: HashSet::new(),
            });
        if now.saturating_duration_since(win.window_started_at) >= PER_AGENT_DIRTY_HINT_WINDOW {
            win.window_started_at = now;
            win.seen_sessions.clear();
        }
        if win.seen_sessions.contains(sid) {
            return true;
        }
        if win.seen_sessions.len() >= PER_AGENT_DIRTY_HINT_MAX_SESSIONS {
            return false;
        }
        win.seen_sessions.insert(sid.to_owned());
        true
    }
}

/// One dirty-queue row hint from a flush batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyHint {
    pub session_id: String,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

/// Fold distinct `session_id → {min_ts,max_ts}` from this batch only. No retained state.
pub fn fold_dirty_hints<'a, I>(spans: I) -> Vec<DirtyHint>
where
    I: IntoIterator<Item = &'a Span>,
{
    let mut map: HashMap<String, (DateTime<Utc>, DateTime<Utc>)> = HashMap::new();
    for span in spans {
        if crate::models::span::is_recording_span(span) {
            continue;
        }
        let sid = span.session_id.trim();
        if sid.is_empty() {
            continue;
        }
        let ts = span.timestamp;
        map.entry(sid.to_string())
            .and_modify(|(min_ts, max_ts)| {
                if ts < *min_ts {
                    *min_ts = ts;
                }
                if ts > *max_ts {
                    *max_ts = ts;
                }
            })
            .or_insert((ts, ts));
    }
    let mut out: Vec<DirtyHint> = map
        .into_iter()
        .map(|(session_id, (min_ts, max_ts))| DirtyHint {
            session_id,
            min_ts_ns: crate::models::event_time::to_ns(min_ts),
            max_ts_ns: crate::models::event_time::to_ns(max_ts),
        })
        .collect();
    out.sort_by(|a, b| a.session_id.cmp(&b.session_id));
    out
}

/// Postgres dirty-queue writer for one tenant metadata schema.
#[derive(Clone)]
pub struct SessionSummaryDirty {
    pool: Pool,
    metadata_schema: String,
    workspace_id: String,
    workspace_scoped: bool,
    limiter: Arc<DirtyHintLimiter>,
}

impl SessionSummaryDirty {
    pub fn new(
        pool: Pool,
        metadata_schema: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.into(),
            workspace_id: workspace_id.into(),
            workspace_scoped: false,
            limiter: Arc::new(DirtyHintLimiter::default()),
        }
    }

    pub fn new_for_workspace(
        pool: Pool,
        metadata_schema: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.into(),
            workspace_id: workspace_id.into(),
            workspace_scoped: true,
            limiter: Arc::new(DirtyHintLimiter::default()),
        }
    }

    pub fn allow_span(&self, span: &Span) -> bool {
        self.limiter.allow_at(span, Instant::now())
    }

    /// Best-effort: fold + UPSERT. Never fails the caller — logs + metric on error.
    pub async fn mark_after_traces_commit(&self, spans: &[Span]) {
        self.apply_hints(&fold_dirty_hints(spans)).await;
    }

    /// Best-effort UPSERT of pre-folded hints (fold before moving batches into the writer).
    pub async fn apply_hints(&self, hints: &[DirtyHint]) {
        if hints.is_empty() {
            return;
        }
        let started = std::time::Instant::now();
        match self.upsert_dirty(hints).await {
            Ok(()) => {
                crate::self_monitoring::record_session_summary_dirty_upsert(
                    &self.workspace_id,
                    started.elapsed(),
                );
            }
            Err(err) => {
                warn!(
                    tenant = %self.workspace_id,
                    schema = %self.metadata_schema,
                    sessions = hints.len(),
                    error = %err,
                    "session_summary dirty UPSERT failed (ingest still ok)"
                );
                crate::self_monitoring::record_session_summary_dirty_upsert_error(
                    &self.workspace_id,
                );
            }
        }
    }

    /// Multi-row UPSERT with LEAST/GREATEST merge on conflict.
    pub(crate) async fn upsert_dirty(&self, hints: &[DirtyHint]) -> Result<()> {
        if hints.is_empty() {
            return Ok(());
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow!("session_summary dirty pool get: {e}"))?;
        let schema = quote_pg_ident(&self.metadata_schema);
        let template = if self.workspace_scoped {
            WORKSPACE_DIRTY_UPSERT_SQL
        } else {
            DIRTY_UPSERT_SQL
        };
        let sql = template.replace("{{schema_quoted}}", &schema);
        let session_ids: Vec<String> = hints.iter().map(|hint| hint.session_id.clone()).collect();
        let min_ts_ns: Vec<i64> = hints.iter().map(|hint| hint.min_ts_ns).collect();
        let max_ts_ns: Vec<i64> = hints.iter().map(|hint| hint.max_ts_ns).collect();
        let workspace_id = self.workspace_id.clone();
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(4);
        if self.workspace_scoped {
            params.push(&workspace_id as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        params.extend([
            &session_ids as &(dyn tokio_postgres::types::ToSql + Sync),
            &min_ts_ns,
            &max_ts_ns,
        ]);
        let statement = client
            .prepare_cached(&sql)
            .await
            .context("prepare session_summary dirty UPSERT")?;
        client
            .execute(&statement, &params)
            .await
            .context("session_summary dirty UPSERT")?;
        Ok(())
    }
}

impl crate::ingest::SpanCommitHook for SessionSummaryDirty {
    fn prepare(&self, batches: &[Vec<Span>]) -> Option<crate::ingest::CommitWrite> {
        let hints = fold_dirty_hints(
            batches
                .iter()
                .flatten()
                .filter(|span| self.allow_span(span)),
        );
        if hints.is_empty() {
            return None;
        }
        let dirty = self.clone();
        Some(Box::pin(async move {
            dirty.apply_hints(&hints).await;
        }))
    }
}

#[cfg(test)]
mod fold_tests {
    use super::*;
    use crate::session_summary::test_span::span_at;

    #[test]
    fn fold_empty() {
        assert!(fold_dirty_hints([]).is_empty());
    }

    #[test]
    fn fold_one_session_many_spans() {
        let spans = vec![span_at("a", 10), span_at("a", 5), span_at("a", 20)];
        let hints = fold_dirty_hints(&spans);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].session_id, "a");
        assert_eq!(
            crate::models::event_time::from_ns(hints[0].min_ts_ns),
            chrono::TimeZone::timestamp_opt(&Utc, 5, 0).unwrap()
        );
        assert_eq!(
            crate::models::event_time::from_ns(hints[0].max_ts_ns),
            chrono::TimeZone::timestamp_opt(&Utc, 20, 0).unwrap()
        );
    }

    #[test]
    fn fold_many_sessions() {
        let spans = vec![span_at("b", 2), span_at("a", 1), span_at("b", 9)];
        let hints = fold_dirty_hints(&spans);
        assert_eq!(hints.len(), 2);
        assert_eq!(hints[0].session_id, "a");
        assert_eq!(hints[1].session_id, "b");
        assert_eq!(
            crate::models::event_time::from_ns(hints[1].min_ts_ns),
            chrono::TimeZone::timestamp_opt(&Utc, 2, 0).unwrap()
        );
        assert_eq!(
            crate::models::event_time::from_ns(hints[1].max_ts_ns),
            chrono::TimeZone::timestamp_opt(&Utc, 9, 0).unwrap()
        );
    }

    #[test]
    fn fold_skips_empty_session_id() {
        let spans = vec![span_at("", 1), span_at("  ", 2), span_at("ok", 3)];
        let hints = fold_dirty_hints(&spans);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].session_id, "ok");
    }
}
