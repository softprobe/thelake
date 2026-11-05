//! Durable, workspace-bound staging for trace-to-session correlation.

use super::{assign_session_id, classify_capture_span, CaptureObservation};
use crate::ingest::IngestEngine;
use crate::models::Span;
use crate::sql::capture_correlation as sql;
use anyhow::{bail, Context, Result};
use deadpool_postgres::Pool;
use tracing::{info, warn};

const MAX_STAGED_SPANS: i64 = 256;
const MAX_STAGED_BYTES: i64 = 16 * 1024 * 1024;
const MAX_MAPPINGS: i64 = 10_000;
const CLAIM_BATCH_SIZE: i64 = 256;
const EXPIRY_BATCH_SIZE: i64 = 512;

pub(crate) struct CaptureCorrelationStore {
    pool: Pool,
    metadata_schema: String,
    workspace_id: String,
}

impl CaptureCorrelationStore {
    pub(crate) fn new(pool: Pool, metadata_schema: &str, workspace_id: &str) -> Self {
        Self {
            pool,
            metadata_schema: metadata_schema.to_owned(),
            workspace_id: workspace_id.to_owned(),
        }
    }

    pub(crate) async fn ensure_tables(
        client: &tokio_postgres::Client,
        metadata_schema: &str,
    ) -> Result<()> {
        client
            .batch_execute(&sql::schema_ddl(metadata_schema))
            .await
            .with_context(|| {
                format!(
                    "initialize capture correlation tables in {}",
                    metadata_schema
                )
            })
    }

    pub(crate) async fn sweep_expired_schema(pool: &Pool, metadata_schema: &str) -> Result<u64> {
        let client = pool.get().await.context("capture expiry pool")?;
        let expire_mappings = sql::render(sql::EXPIRE_MAPPINGS_BATCH, metadata_schema);
        let expire_pending = sql::render(sql::EXPIRE_PENDING_BATCH, metadata_schema);
        let mappings = client
            .execute(&expire_mappings, &[&EXPIRY_BATCH_SIZE])
            .await?;
        let pending = client
            .execute(&expire_pending, &[&EXPIRY_BATCH_SIZE])
            .await?;
        Ok(mappings + pending)
    }

    /// Persist recordings and mapped trace spans directly. Stage unmapped
    /// trace spans until a Session ID for their trace arrives. The DB lease is
    /// acknowledged only after DuckLake flush succeeds; a crash in the narrow
    /// lake-write / ack window may cause an at-least-once duplicate.
    pub(crate) async fn ingest(
        &self,
        ingest: &IngestEngine,
        spans: Vec<Span>,
        request_size: usize,
        agent_id: &str,
    ) -> Result<()> {
        let mut direct_spans = Vec::new();
        let mut mappings = Vec::new();
        let mut unmapped_spans = Vec::new();

        for span in spans {
            match classify_capture_span(&span, &self.workspace_id, Some(agent_id))? {
                Some(CaptureObservation::Trace {
                    key,
                    session_id: Some(session_id),
                }) => {
                    mappings.push((key, session_id));
                    direct_spans.push(span);
                }
                Some(CaptureObservation::Trace {
                    key,
                    session_id: None,
                }) => unmapped_spans.push((key, span)),
                Some(CaptureObservation::Recording) => direct_spans.push(span),
                None => direct_spans.push(span),
            }
        }
        info!(
            direct_span_count = direct_spans.len(),
            session_mapping_count = mappings.len(),
            unmapped_trace_count = unmapped_spans.len(),
            "Classified Agent telemetry batch"
        );
        let advisory_lock = sql::render(sql::ADVISORY_LOCK, &self.metadata_schema);
        let expire_mappings = sql::render(sql::EXPIRE_MAPPINGS, &self.metadata_schema);
        let expire_pending = sql::render(sql::EXPIRE_PENDING, &self.metadata_schema);
        let upsert_mapping = sql::render(sql::UPSERT_MAPPING, &self.metadata_schema);
        let map_pending = sql::render(sql::MAP_PENDING, &self.metadata_schema);
        let find_mapping = sql::render(sql::FIND_MAPPING, &self.metadata_schema);
        let insert_pending = sql::render(sql::INSERT_PENDING, &self.metadata_schema);
        let find_pending = sql::render(sql::FIND_PENDING, &self.metadata_schema);
        let agent_usage = sql::render(sql::AGENT_USAGE, &self.metadata_schema);
        let mapping_count = sql::render(sql::MAPPING_COUNT, &self.metadata_schema);

        {
            let mut client = self.pool.get().await.context("capture correlation pool")?;
            let transaction = client.transaction().await?;
            let lock_key = format!("{}:{}", self.workspace_id, agent_id);
            transaction.query_one(&advisory_lock, &[&lock_key]).await?;
            transaction
                .execute(&expire_mappings, &[&self.workspace_id, &agent_id])
                .await?;
            transaction
                .execute(&expire_pending, &[&self.workspace_id, &agent_id])
                .await?;
            for (key, session_id) in &mappings {
                let row = transaction
                    .query_opt(
                        &upsert_mapping,
                        &[&key.workspace_id, &key.agent_id, &key.trace_id, session_id],
                    )
                    .await?;
                if row.is_none() {
                    bail!("conflicting session mapping for Agent trace");
                }
                transaction
                    .execute(
                        &map_pending,
                        &[&key.workspace_id, &key.agent_id, &key.trace_id, session_id],
                    )
                    .await?;
            }
            for (key, span) in &unmapped_spans {
                let mapping = transaction
                    .query_opt(
                        &find_mapping,
                        &[&key.workspace_id, &key.agent_id, &key.trace_id],
                    )
                    .await?
                    .map(|row| row.get::<_, String>(0));
                let span_json = serde_json::to_string(span)?;
                let byte_size = i64::try_from(span_json.len())?;
                transaction
                    .execute(
                        &insert_pending,
                        &[
                            &key.workspace_id,
                            &key.agent_id,
                            &key.trace_id,
                            &span.span_id,
                            &mapping,
                            &span_json,
                            &byte_size,
                        ],
                    )
                    .await?;
                let stored = transaction
                    .query_one(
                        &find_pending,
                        &[
                            &key.workspace_id,
                            &key.agent_id,
                            &key.trace_id,
                            &span.span_id,
                        ],
                    )
                    .await?;
                let stored_json: String = stored.get(0);
                let stored_bytes: i64 = stored.get(1);
                if stored_json != span_json || stored_bytes != byte_size {
                    bail!("conflicting duplicate trace span in session correlation staging");
                }
            }

            let usage = transaction
                .query_one(&agent_usage, &[&self.workspace_id, &agent_id])
                .await?;
            let staged_count: i64 = usage.get(0);
            let staged_bytes: i64 = usage.get(1);
            if staged_count > MAX_STAGED_SPANS || staged_bytes > MAX_STAGED_BYTES {
                bail!("capture correlation staging capacity exceeded");
            }

            let mapping_count: i64 = transaction
                .query_one(&mapping_count, &[&self.workspace_id, &agent_id])
                .await?
                .get(0);
            if mapping_count > MAX_MAPPINGS {
                bail!("capture session mapping capacity exceeded");
            }
            transaction.commit().await?;
        }

        let claim_token = uuid::Uuid::new_v4().to_string();
        let claim_ready = sql::render(sql::CLAIM_READY, &self.metadata_schema);
        let claimed = {
            let client = self
                .pool
                .get()
                .await
                .context("capture correlation claim pool")?;
            client
                .query(
                    &claim_ready,
                    &[
                        &self.workspace_id,
                        &agent_id,
                        &claim_token,
                        &CLAIM_BATCH_SIZE,
                    ],
                )
                .await?
        };

        let claimed_spans = (|| -> Result<Vec<Span>> {
            let mut spans = Vec::with_capacity(claimed.len());
            for row in &claimed {
                let json: String = row.get(0);
                let session_id: String = row.get(1);
                let mut span: Span = serde_json::from_str(&json)
                    .context("decode staged trace span before lake write")?;
                if span.trace_id != row.get::<_, String>(2)
                    || span.span_id != row.get::<_, String>(3)
                {
                    bail!("staged trace span identity changed before lake write");
                }
                assign_session_id(&mut span, &session_id)?;
                spans.push(span);
            }
            Ok(spans)
        })();
        let claimed_spans = match claimed_spans {
            Ok(spans) => spans,
            Err(error) => {
                self.release_claim(agent_id, &claim_token).await;
                return Err(error);
            }
        };
        let has_lake_writes = !direct_spans.is_empty() || !claimed_spans.is_empty();
        if has_lake_writes {
            let direct_span_count = direct_spans.len();
            let correlated_span_count = claimed_spans.len();
            direct_spans.extend(claimed_spans);
            let write_result = async {
                ingest.add_spans(direct_spans, request_size).await?;
                ingest.force_flush_spans().await?;
                info!(
                    direct_span_count,
                    correlated_span_count, "Flushed Agent spans to DuckLake"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = write_result {
                self.release_claim(agent_id, &claim_token).await;
                return Err(error).context("flush correlated capture spans to DuckLake");
            }
        }

        if !claimed.is_empty() {
            let ack = sql::render(sql::ACK_CLAIM, &self.metadata_schema);
            let client = self
                .pool
                .get()
                .await
                .context("capture correlation ack pool")?;
            let updated = client
                .execute(&ack, &[&self.workspace_id, &agent_id, &claim_token])
                .await?;
            if updated != claimed.len() as u64 {
                warn!(
                    claimed = claimed.len(),
                    acknowledged = updated,
                    "capture correlation ack count differs from claim"
                );
            }
        }

        Ok(())
    }

    async fn release_claim(&self, agent_id: &str, claim_token: &str) {
        let release = sql::render(sql::RELEASE_CLAIM, &self.metadata_schema);
        let result: Result<u64> = match self.pool.get().await {
            Ok(client) => client
                .execute(&release, &[&self.workspace_id, &agent_id, &claim_token])
                .await
                .map_err(Into::into),
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            warn!(%error, "capture correlation lease will expire after write failure");
        }
    }
}
