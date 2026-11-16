//! Post-commit Postgres tag index (`tag_index`) for fast `/api/search/tags`.

use crate::compat::projection::tempo::project_tempo_link_attributes;
use crate::ingest::{CommitWrite, SpanCommitHook};
use crate::models::span::{INSTRUMENTATION_SCOPE_ATTRIBUTE, LINKS_ATTRIBUTE};
use crate::models::Span;
use crate::workspace::quote_pg_ident;
use anyhow::{anyhow, Context, Result};
use deadpool_postgres::Pool;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::warn;

const TAG_INDEX_DDL: &str = include_str!("../sql/schema/tag_index.sql");
const TAG_INDEX_UPSERT_SQL: &str = include_str!("../sql/tag_index/upsert.sql");
const TAG_INDEX_LIST_SQL: &str = include_str!("../sql/tag_index/list.sql");
const TAG_INDEX_LIST_SINCE_SQL: &str = include_str!("../sql/tag_index/list_since.sql");
const TAG_INDEX_SOURCE_MASK_SQL: &str = include_str!("../sql/tag_index/source_mask.sql");

pub const TAG_FLUSH_WINDOW: Duration = Duration::from_secs(60);
pub const TAG_FLUSH_WINDOW_NS: i64 = 60_000_000_000;
pub const TAG_SOURCE_SPAN_OR_RESOURCE: i16 = 1;
pub const TAG_SOURCE_EVENT_OR_LINK: i16 = 2;
const MAX_CACHED_TAGS: usize = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagEntry {
    pub tag: String,
    pub last_seen_ns: i64,
    pub source_mask: i16,
}

#[derive(Debug)]
pub struct TagFlushThrottle {
    window: Duration,
    max_entries: usize,
    last_flushed: Mutex<HashMap<String, (Instant, i16, i64)>>,
}

impl Default for TagFlushThrottle {
    fn default() -> Self {
        Self::with_limits(TAG_FLUSH_WINDOW, MAX_CACHED_TAGS)
    }
}

impl TagFlushThrottle {
    pub fn with_limits(window: Duration, max_entries: usize) -> Self {
        Self {
            window,
            max_entries: max_entries.max(1),
            last_flushed: Mutex::new(HashMap::new()),
        }
    }

    pub fn filter_due(&self, entries: Vec<TagEntry>, now: Instant) -> Vec<TagEntry> {
        let mut guard = self.last_flushed.lock().expect("tag flush throttle lock");
        let window_ns = i64::try_from(self.window.as_nanos()).unwrap_or(i64::MAX);
        let mut due = Vec::new();
        for mut entry in entries {
            let (is_due, merged_mask, merged_ts) = match guard.get(&entry.tag) {
                Some(&(prev_at, prev_mask, prev_ts)) => {
                    let window_elapsed = now.saturating_duration_since(prev_at) >= self.window;
                    let new_source_bit = (entry.source_mask & !prev_mask) != 0;
                    let newer_event_time = entry.last_seen_ns > prev_ts.saturating_add(window_ns);
                    (
                        window_elapsed || new_source_bit || newer_event_time,
                        prev_mask | entry.source_mask,
                        prev_ts.max(entry.last_seen_ns),
                    )
                }
                None => (true, entry.source_mask, entry.last_seen_ns),
            };
            if !is_due {
                continue;
            }
            if !guard.contains_key(&entry.tag) && guard.len() >= self.max_entries {
                guard.retain(|_, (ts, _, _)| now.saturating_duration_since(*ts) < self.window);
                if guard.len() >= self.max_entries {
                    if let Some(oldest) = guard
                        .iter()
                        .min_by_key(|(_, (ts, _, _))| *ts)
                        .map(|(k, _)| k.clone())
                    {
                        guard.remove(&oldest);
                    }
                }
            }
            entry.source_mask = merged_mask;
            guard.insert(entry.tag.clone(), (now, merged_mask, merged_ts));
            due.push(entry);
        }
        due
    }
}

fn record_tag(map: &mut HashMap<String, (i64, i16)>, key: &str, ts_ns: i64, source_mask: i16) {
    if key.is_empty() {
        return;
    }
    map.entry(key.to_owned())
        .and_modify(|(max_ts, mask)| {
            if ts_ns > *max_ts {
                *max_ts = ts_ns;
            }
            *mask |= source_mask;
        })
        .or_insert((ts_ns, source_mask));
}

/// Fold distinct tag keys, max span timestamp (ns), and source bitmask from committed batches.
pub fn fold_tag_entries(batches: &[Vec<Span>]) -> Vec<TagEntry> {
    let mut map: HashMap<String, (i64, i16)> = HashMap::new();
    for span in batches.iter().flatten() {
        let ts_ns = crate::models::event_time::to_ns(span.timestamp);
        for key in span.attributes.keys() {
            if key != INSTRUMENTATION_SCOPE_ATTRIBUTE && key != LINKS_ATTRIBUTE {
                record_tag(&mut map, key, ts_ns, TAG_SOURCE_SPAN_OR_RESOURCE);
            }
        }
        for key in span.resource_attributes.keys() {
            record_tag(&mut map, key, ts_ns, TAG_SOURCE_SPAN_OR_RESOURCE);
        }
        if !span.app_id.is_empty() {
            record_tag(&mut map, "service.name", ts_ns, TAG_SOURCE_SPAN_OR_RESOURCE);
        }
        for event in &span.events {
            for key in event.attributes.keys() {
                record_tag(&mut map, key, ts_ns, TAG_SOURCE_EVENT_OR_LINK);
            }
        }
        if let Some(raw_links) = span.attributes.get(LINKS_ATTRIBUTE) {
            if let Ok(Value::Array(links)) = serde_json::from_str::<Value>(raw_links) {
                for key in project_tempo_link_attributes(&links).keys() {
                    record_tag(&mut map, key, ts_ns, TAG_SOURCE_EVENT_OR_LINK);
                }
            }
        }
    }
    let mut out: Vec<TagEntry> = map
        .into_iter()
        .map(|(tag, (last_seen_ns, source_mask))| TagEntry {
            tag,
            last_seen_ns,
            source_mask,
        })
        .collect();
    out.sort_by(|a, b| a.tag.cmp(&b.tag));
    out
}

pub async fn ensure_tag_index_table(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<()> {
    let ddl = TAG_INDEX_DDL.replace("{{schema}}", &quote_pg_ident(tenant_schema));
    client
        .batch_execute(&ddl)
        .await
        .with_context(|| format!("failed tag_index DDL in {tenant_schema}"))?;
    Ok(())
}

pub async fn list_tags(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let client = pool
        .get()
        .await
        .map_err(|e| anyhow!("tag_index pool get: {e}"))?;
    let sql = TAG_INDEX_LIST_SQL.replace("{{schema_quoted}}", &quote_pg_ident(metadata_schema));
    let statement = client
        .prepare_cached(&sql)
        .await
        .context("prepare tag_index list")?;
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = client
        .query(&statement, &[&workspace_id, &limit_i64])
        .await
        .context("query tag_index list")?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

#[derive(Clone)]
pub struct TagIndexReader {
    pool: Pool,
    metadata_schema: String,
    workspace_id: String,
}

impl TagIndexReader {
    pub fn new(
        pool: Pool,
        metadata_schema: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.into(),
            workspace_id: workspace_id.into(),
        }
    }

    pub async fn list(&self, limit: usize) -> Result<Vec<String>> {
        list_tags(&self.pool, &self.metadata_schema, &self.workspace_id, limit).await
    }

    pub async fn list_since(
        &self,
        min_last_seen_ns: i64,
        limit: usize,
    ) -> Result<Option<Vec<String>>> {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow!("tag_index pool get: {e}"))?;
        let sql = TAG_INDEX_LIST_SINCE_SQL
            .replace("{{schema_quoted}}", &quote_pg_ident(&self.metadata_schema));
        let statement = client
            .prepare_cached(&sql)
            .await
            .context("prepare tag_index list_since")?;
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = client
            .query(
                &statement,
                &[&self.workspace_id, &min_last_seen_ns, &limit_i64],
            )
            .await
            .context("query tag_index list_since")?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        let populated: bool = first.get(0);
        if !populated {
            return Ok(None);
        }
        let tags = rows
            .into_iter()
            .filter_map(|row| row.get::<_, Option<String>>(1))
            .collect();
        Ok(Some(tags))
    }

    pub async fn source_mask(&self, tag: &str) -> Result<Option<i16>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow!("tag_index pool get: {e}"))?;
        let sql = TAG_INDEX_SOURCE_MASK_SQL
            .replace("{{schema_quoted}}", &quote_pg_ident(&self.metadata_schema));
        let statement = client
            .prepare_cached(&sql)
            .await
            .context("prepare tag_index source_mask")?;
        let row = client
            .query_opt(&statement, &[&self.workspace_id, &tag])
            .await
            .context("query tag_index source_mask")?;
        Ok(row.map(|r| r.get::<_, i16>(0)))
    }
}

#[derive(Clone)]
pub struct TagIndexWriter {
    pool: Pool,
    metadata_schema: String,
    workspace_id: String,
    throttle: Arc<TagFlushThrottle>,
}

impl TagIndexWriter {
    pub fn new(
        pool: Pool,
        metadata_schema: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self::with_throttle(
            pool,
            metadata_schema,
            workspace_id,
            Arc::new(TagFlushThrottle::default()),
        )
    }

    pub fn with_throttle(
        pool: Pool,
        metadata_schema: impl Into<String>,
        workspace_id: impl Into<String>,
        throttle: Arc<TagFlushThrottle>,
    ) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.into(),
            workspace_id: workspace_id.into(),
            throttle,
        }
    }

    pub async fn apply_entries(&self, entries: &[TagEntry]) {
        if let Err(err) = self.upsert_entries(entries).await {
            warn!(
                tenant = %self.workspace_id,
                schema = %self.metadata_schema,
                tags = entries.len(),
                error = %err,
                "tag_index UPSERT failed (ingest still ok)"
            );
        }
    }

    pub async fn upsert_entries(&self, entries: &[TagEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow!("tag_index pool get: {e}"))?;
        let sql = TAG_INDEX_UPSERT_SQL
            .replace("{{schema_quoted}}", &quote_pg_ident(&self.metadata_schema));
        let tags: Vec<String> = entries.iter().map(|e| e.tag.clone()).collect();
        let last_seen_ns: Vec<i64> = entries.iter().map(|e| e.last_seen_ns).collect();
        let source_masks: Vec<i16> = entries.iter().map(|e| e.source_mask).collect();
        let statement = client
            .prepare_cached(&sql)
            .await
            .context("prepare tag_index UPSERT")?;
        client
            .execute(
                &statement,
                &[&self.workspace_id, &tags, &last_seen_ns, &source_masks],
            )
            .await
            .context("tag_index UPSERT")?;
        Ok(())
    }
}

impl SpanCommitHook for TagIndexWriter {
    fn prepare(&self, batches: &[Vec<Span>]) -> Option<CommitWrite> {
        let folded = fold_tag_entries(batches);
        if folded.is_empty() {
            return None;
        }
        let due = self.throttle.filter_due(folded, Instant::now());
        if due.is_empty() {
            return None;
        }
        let writer = self.clone();
        Some(Box::pin(async move {
            writer.apply_entries(&due).await;
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::SpanEvent;
    use crate::session_summary::test_span::span_at;
    use chrono::{TimeZone, Utc};
    use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};
    use tokio_postgres::NoTls;

    #[test]
    fn fold_tag_entries_extracts_attributes_resources_service_events_and_links() {
        let mut s1 = span_at("s1", 10);
        s1.attributes.insert("http.method".into(), "GET".into());
        s1.attributes.insert("".into(), "ignored".into());
        s1.attributes
            .insert(INSTRUMENTATION_SCOPE_ATTRIBUTE.into(), "{}".into());
        s1.attributes.insert(
            LINKS_ATTRIBUTE.into(),
            serde_json::json!([{"attributes": {"peer.service": "auth"}}]).to_string(),
        );
        s1.resource_attributes
            .insert("deployment.environment".into(), "staging".into());
        s1.resource_attributes.insert("".into(), "ignored".into());
        let mut ev_attrs = HashMap::new();
        ev_attrs.insert("exception.type".into(), "Timeout".into());
        ev_attrs.insert("".into(), "ignored".into());
        s1.events.push(SpanEvent {
            name: "exception".into(),
            timestamp: Utc.timestamp_opt(10, 0).unwrap(),
            attributes: ev_attrs,
        });

        let mut s2 = span_at("s2", 25);
        s2.app_id = String::new();
        s2.attributes.insert("http.method".into(), "POST".into());
        s2.attributes
            .insert(LINKS_ATTRIBUTE.into(), "not-json".into());

        // Older timestamp for http.method must not overwrite 25s.
        let mut s3 = span_at("s3", 5);
        s3.app_id = String::new();
        s3.attributes.insert("http.method".into(), "PUT".into());

        let entries = fold_tag_entries(&[vec![s1], vec![s2, s3]]);
        let map: HashMap<String, i64> = entries
            .into_iter()
            .map(|e| (e.tag, e.last_seen_ns))
            .collect();

        assert_eq!(map.get("http.method").copied(), Some(25_000_000_000));
        assert_eq!(
            map.get("deployment.environment").copied(),
            Some(10_000_000_000)
        );
        assert_eq!(map.get("service.name").copied(), Some(10_000_000_000));
        assert_eq!(map.get("exception.type").copied(), Some(10_000_000_000));
        assert_eq!(map.get("link.peer.service").copied(), Some(10_000_000_000));
        assert!(!map.contains_key(""));
        assert!(!map.contains_key(INSTRUMENTATION_SCOPE_ATTRIBUTE));
        assert!(!map.contains_key(LINKS_ATTRIBUTE));
    }

    #[test]
    fn fold_tag_entries_handles_non_array_links_json_and_empty_batches() {
        assert!(fold_tag_entries(&[]).is_empty());
        let mut s = span_at("s1", 5);
        s.app_id = String::new();
        s.attributes.insert(LINKS_ATTRIBUTE.into(), "{}".into());
        assert!(fold_tag_entries(&[vec![s]]).is_empty());
    }

    #[test]
    fn throttle_emits_new_suppresses_within_window_and_reemits_after_window() {
        let throttle = TagFlushThrottle::with_limits(Duration::from_secs(60), 2);
        let t0 = Instant::now();
        let due1 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 1,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0,
        );
        assert_eq!(due1.len(), 1);

        let due2 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 2,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0 + Duration::from_secs(30),
        );
        assert!(due2.is_empty());

        let due3 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 3,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0 + Duration::from_secs(61),
        );
        assert_eq!(due3.len(), 1);

        // Exceed max_entries=2: inserting b and c triggers retain + oldest eviction.
        let due4 = throttle.filter_due(
            vec![
                TagEntry {
                    tag: "b".into(),
                    last_seen_ns: 4,
                    source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
                },
                TagEntry {
                    tag: "c".into(),
                    last_seen_ns: 5,
                    source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
                },
                TagEntry {
                    tag: "d".into(),
                    last_seen_ns: 6,
                    source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
                },
            ],
            t0 + Duration::from_secs(62),
        );
        assert_eq!(due4.len(), 3);

        // Once existing entries expire, retain frees capacity without needing oldest eviction.
        let due5 = throttle.filter_due(
            vec![TagEntry {
                tag: "e".into(),
                last_seen_ns: 7,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0 + Duration::from_secs(130),
        );
        assert_eq!(due5.len(), 1);
    }

    fn unreachable_pool() -> Pool {
        let mut pg = tokio_postgres::Config::new();
        pg.host("127.0.0.1");
        pg.port(1);
        pg.dbname("x");
        pg.user("x");
        let mgr = Manager::from_config(
            pg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        Pool::builder(mgr).max_size(1).build().unwrap()
    }

    #[tokio::test]
    async fn writer_prepare_returns_none_when_empty_and_apply_logs_on_error() {
        let writer = TagIndexWriter::new(unreachable_pool(), "s", "ws");
        assert!(writer.prepare(&[]).is_none());
        let mut s = span_at("s1", 1);
        s.attributes.insert("k".into(), "v".into());
        let write = writer.prepare(&[vec![s]]).expect("due write");
        // Best-effort: must not panic even if PG is unreachable (and exercise warn! formatting).
        let sub = tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .finish();
        let _guard = tracing::subscriber::set_default(sub);
        write.await;
        // Empty upsert_entries is a no-op.
        writer.upsert_entries(&[]).await.unwrap();
        // Second prepare within 60s is throttled.
        let mut s2 = span_at("s2", 2);
        s2.attributes.insert("k".into(), "v2".into());
        assert!(writer.prepare(&[vec![s2]]).is_none());
    }

    #[tokio::test]
    async fn list_tags_zero_limit_returns_empty() {
        let out = list_tags(&unreachable_pool(), "s", "ws", 0).await.unwrap();
        assert!(out.is_empty());
        let reader = TagIndexReader::new(unreachable_pool(), "s", "ws");
        assert!(reader.list(0).await.unwrap().is_empty());
        assert_eq!(reader.list_since(0, 0).await.unwrap(), Some(Vec::new()));
        assert!(reader.list(5).await.is_err());
    }

    async fn pg_pool(schema: &str) -> Pool {
        let mut pg = tokio_postgres::Config::new();
        pg.host("localhost");
        pg.port(5432);
        pg.dbname("ducklake");
        pg.user("ducklake");
        pg.password("ducklake");
        let mgr = Manager::from_config(
            pg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr).max_size(4).build().unwrap();
        let client = tokio::time::timeout(Duration::from_secs(2), pool.get())
            .await
            .expect("ducklake-postgres timeout")
            .expect("ducklake-postgres required");
        ensure_tag_index_table(&client, schema).await.unwrap();
        let q = quote_pg_ident(schema);
        client
            .execute(&format!("TRUNCATE {q}.tag_index"), &[])
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_tag_index_upsert_greatest_and_list_respects_workspace_and_limit() {
        let schema = "thelake_tag_idx_test";
        let pool = pg_pool(schema).await;
        let w1 = TagIndexWriter::new(pool.clone(), schema, "ws1");
        let w2 = TagIndexWriter::new(pool.clone(), schema, "ws2");

        w1.upsert_entries(&[
            TagEntry {
                tag: "z.tag".into(),
                last_seen_ns: 200,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            },
            TagEntry {
                tag: "a.tag".into(),
                last_seen_ns: 100,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            },
        ])
        .await
        .unwrap();
        // Smaller timestamp must not regress last_seen_ns.
        w1.upsert_entries(&[TagEntry {
            tag: "a.tag".into(),
            last_seen_ns: 50,
            source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
        }])
        .await
        .unwrap();
        w2.upsert_entries(&[TagEntry {
            tag: "other.ws".into(),
            last_seen_ns: 300,
            source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
        }])
        .await
        .unwrap();

        let all_w1 = list_tags(&pool, schema, "ws1", 10).await.unwrap();
        assert_eq!(all_w1, vec!["a.tag", "z.tag"]);

        let limited_w1 = list_tags(&pool, schema, "ws1", 1).await.unwrap();
        assert_eq!(limited_w1, vec!["a.tag"]);

        let all_w2 = list_tags(&pool, schema, "ws2", 10).await.unwrap();
        assert_eq!(all_w2, vec!["other.ws"]);

        let reader = TagIndexReader::new(pool.clone(), schema, "ws1");
        assert_eq!(reader.list(10).await.unwrap(), vec!["a.tag", "z.tag"]);

        let client = pool.get().await.unwrap();
        let q = quote_pg_ident(schema);
        let seen_a: i64 = client
            .query_one(
                &format!("SELECT last_seen_ns FROM {q}.tag_index WHERE workspace_id = 'ws1' AND tag = 'a.tag'"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(seen_a, 100);
    }

    #[test]
    fn fold_tag_entries_tracks_and_ors_source_mask() {
        let mut s = span_at("s1", 10);
        s.app_id = String::new();
        s.attributes.insert("span.only".into(), "1".into());
        s.attributes.insert("shared.attr".into(), "1".into());
        s.resource_attributes.insert("res.only".into(), "1".into());
        s.events.push(SpanEvent {
            name: "evt".into(),
            timestamp: Utc.timestamp_opt(10, 0).unwrap(),
            attributes: [
                ("evt.only".into(), "1".into()),
                ("shared.attr".into(), "2".into()),
            ]
            .into(),
        });
        s.attributes.insert(
            LINKS_ATTRIBUTE.into(),
            serde_json::json!([{"attributes": {"only": "1"}}]).to_string(),
        );

        let entries: std::collections::BTreeMap<String, (i64, i16)> = fold_tag_entries(&[vec![s]])
            .into_iter()
            .map(|e| (e.tag, (e.last_seen_ns, e.source_mask)))
            .collect();
        let expected_ts = 10_000_000_000;
        assert_eq!(
            entries.get("span.only"),
            Some(&(expected_ts, TAG_SOURCE_SPAN_OR_RESOURCE))
        );
        assert_eq!(
            entries.get("res.only"),
            Some(&(expected_ts, TAG_SOURCE_SPAN_OR_RESOURCE))
        );
        assert_eq!(
            entries.get("evt.only"),
            Some(&(expected_ts, TAG_SOURCE_EVENT_OR_LINK))
        );
        assert_eq!(
            entries.get("link.only"),
            Some(&(expected_ts, TAG_SOURCE_EVENT_OR_LINK))
        );
        assert_eq!(
            entries.get("shared.attr"),
            Some(&(
                expected_ts,
                TAG_SOURCE_SPAN_OR_RESOURCE | TAG_SOURCE_EVENT_OR_LINK
            ))
        );
    }

    #[test]
    fn throttle_bypasses_window_when_source_mask_gains_new_bit() {
        let throttle = TagFlushThrottle::with_limits(Duration::from_secs(60), 10);
        let t0 = Instant::now();
        let due1 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 100,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0,
        );
        assert_eq!(due1.len(), 1);

        let due2 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 110,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0 + Duration::from_secs(5),
        );
        assert!(due2.is_empty());

        let due3 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 120,
                source_mask: TAG_SOURCE_EVENT_OR_LINK,
            }],
            t0 + Duration::from_secs(6),
        );
        assert_eq!(due3.len(), 1);
        assert_eq!(
            due3[0].source_mask,
            TAG_SOURCE_SPAN_OR_RESOURCE | TAG_SOURCE_EVENT_OR_LINK
        );

        let due4 = throttle.filter_due(
            vec![TagEntry {
                tag: "a".into(),
                last_seen_ns: 60_000_000_200,
                source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
            }],
            t0 + Duration::from_secs(7),
        );
        assert_eq!(due4.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_tag_index_list_since_and_source_mask() {
        let schema = "thelake_tag_idx_mask_test";
        let pool = pg_pool(schema).await;
        let reader = TagIndexReader::new(pool.clone(), schema, "ws-mask");
        let writer = TagIndexWriter::new(pool.clone(), schema, "ws-mask");

        assert_eq!(reader.list_since(0, 10).await.unwrap(), None);
        assert_eq!(reader.source_mask("s.tag").await.unwrap(), None);

        writer
            .upsert_entries(&[
                TagEntry {
                    tag: "s.tag".into(),
                    last_seen_ns: 100,
                    source_mask: TAG_SOURCE_SPAN_OR_RESOURCE,
                },
                TagEntry {
                    tag: "e.tag".into(),
                    last_seen_ns: 500,
                    source_mask: TAG_SOURCE_EVENT_OR_LINK,
                },
            ])
            .await
            .unwrap();

        assert_eq!(
            reader.list_since(400, 10).await.unwrap(),
            Some(vec!["e.tag".to_string()])
        );
        assert_eq!(reader.list_since(900, 10).await.unwrap(), Some(Vec::new()));
        assert_eq!(
            reader.source_mask("s.tag").await.unwrap(),
            Some(TAG_SOURCE_SPAN_OR_RESOURCE)
        );
        assert_eq!(
            reader.source_mask("e.tag").await.unwrap(),
            Some(TAG_SOURCE_EVENT_OR_LINK)
        );
        assert_eq!(reader.source_mask("missing").await.unwrap(), None);

        writer
            .upsert_entries(&[TagEntry {
                tag: "s.tag".into(),
                last_seen_ns: 600,
                source_mask: TAG_SOURCE_EVENT_OR_LINK,
            }])
            .await
            .unwrap();
        assert_eq!(
            reader.source_mask("s.tag").await.unwrap(),
            Some(TAG_SOURCE_SPAN_OR_RESOURCE | TAG_SOURCE_EVENT_OR_LINK)
        );
    }
}
