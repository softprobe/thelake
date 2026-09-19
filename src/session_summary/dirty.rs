//! Ephemeral fold of one flush batch → durable dirty UPSERT (process-stateless).

use crate::models::Span;
use crate::runtime_engine::quote_pg_ident;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use std::collections::HashMap;
use tracing::warn;

/// One dirty-queue row hint from a flush batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyHint {
    pub session_id: String,
    pub min_ts: DateTime<Utc>,
    pub max_ts: DateTime<Utc>,
}

/// Fold distinct `session_id → {min_ts,max_ts}` from this batch only. No retained state.
/// Empty `session_id` is skipped (not invented).
pub fn fold_dirty_hints<'a, I>(spans: I) -> Vec<DirtyHint>
where
    I: IntoIterator<Item = &'a Span>,
{
    let mut map: HashMap<String, (DateTime<Utc>, DateTime<Utc>)> = HashMap::new();
    for span in spans {
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
            min_ts,
            max_ts,
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
    tenant_id: String,
}

impl SessionSummaryDirty {
    pub fn new(
        pool: Pool,
        metadata_schema: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.into(),
            tenant_id: tenant_id.into(),
        }
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
        match self.upsert_dirty(hints).await {
            Ok(()) => {
                crate::self_monitoring::record_session_summary_dirty_upsert(&self.tenant_id);
            }
            Err(err) => {
                warn!(
                    tenant = %self.tenant_id,
                    schema = %self.metadata_schema,
                    sessions = hints.len(),
                    error = %err,
                    "session_summary dirty UPSERT failed (ingest still ok)"
                );
                crate::self_monitoring::record_session_summary_dirty_upsert_error(&self.tenant_id);
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
        let mut sql = format!(
            "INSERT INTO {schema}.session_summary_dirty (session_id, min_ts, max_ts, updated_at) VALUES "
        );
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let now = Utc::now();
        for (i, h) in hints.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let base = i * 4;
            sql.push_str(&format!(
                "(${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4
            ));
            params.push(Box::new(h.session_id.clone()));
            params.push(Box::new(h.min_ts));
            params.push(Box::new(h.max_ts));
            params.push(Box::new(now));
        }
        sql.push_str(
            " ON CONFLICT (session_id) DO UPDATE SET \
             min_ts = LEAST(session_summary_dirty.min_ts, EXCLUDED.min_ts), \
             max_ts = GREATEST(session_summary_dirty.max_ts, EXCLUDED.max_ts), \
             updated_at = EXCLUDED.updated_at",
        );
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        client
            .execute(&sql, &param_refs[..])
            .await
            .context("session_summary dirty UPSERT")?;
        Ok(())
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
            hints[0].min_ts,
            chrono::TimeZone::timestamp_opt(&Utc, 5, 0).unwrap()
        );
        assert_eq!(
            hints[0].max_ts,
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
            hints[1].min_ts,
            chrono::TimeZone::timestamp_opt(&Utc, 2, 0).unwrap()
        );
        assert_eq!(
            hints[1].max_ts,
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
