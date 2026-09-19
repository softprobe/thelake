//! Claim dirty → bounds+clamp → lake aggregate → UPSERT → ack.

use crate::config::DuckLakeConfig;
use crate::runtime_engine::quote_pg_ident;
use crate::session_summary::reduce_sql::compile_session_summary_upsert_sql;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use deadpool_postgres::Pool;
use duckdb::Connection;
use tracing::warn;

/// One claimed dirty row (snapshot of `updated_at` for ack).
#[derive(Debug, Clone)]
pub struct DirtyClaim {
    pub session_id: String,
    pub min_ts: DateTime<Utc>,
    pub max_ts: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One absolute summary row ready for UPSERT.
#[derive(Debug, Clone)]
pub struct SummaryRow {
    pub session_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub observation_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub agent_name: Option<String>,
    pub user_id: Option<String>,
    pub model_name: Option<String>,
}

/// §6.5 window + Stage 2 clamp (no chunking).
pub fn compute_reduce_bounds(
    dirty_min: DateTime<Utc>,
    dirty_max: DateTime<Utc>,
    summary_start: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    max_reduce_span: ChronoDuration,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let to = dirty_max.max(now);
    // least(coalesce(summary.start_time, dirty.min_ts), dirty.min_ts)
    let mut from = summary_start.map(|s| s.min(dirty_min)).unwrap_or(dirty_min);
    let earliest = to - max_reduce_span;
    if from < earliest {
        from = earliest;
    }
    (from, to)
}

/// Union of per-session windows for one DuckLake GROUP BY.
pub fn batch_reduce_window(
    claims: &[DirtyClaim],
    summary_starts: &std::collections::HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
    max_reduce_span: ChronoDuration,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    if claims.is_empty() {
        return None;
    }
    let mut batch_from = None;
    let mut batch_to = None;
    for c in claims {
        let start = summary_starts.get(&c.session_id).copied();
        let (from, to) = compute_reduce_bounds(c.min_ts, c.max_ts, start, now, max_reduce_span);
        batch_from = Some(match batch_from {
            Some(f) if f < from => f,
            _ => from,
        });
        batch_to = Some(match batch_to {
            Some(t) if t > to => t,
            _ => to,
        });
    }
    Some((batch_from?, batch_to?))
}

pub async fn claim_dirty(
    pool: &Pool,
    metadata_schema: &str,
    limit: u64,
) -> Result<(Vec<DirtyClaim>, DateTime<Utc>)> {
    let client = pool.get().await.context("claim_dirty pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let snapshot = Utc::now();
    let rows = client
        .query(
            &format!(
                "SELECT session_id, min_ts, max_ts, updated_at \
                 FROM {schema}.session_summary_dirty \
                 ORDER BY updated_at ASC \
                 LIMIT {limit}"
            ),
            &[],
        )
        .await
        .context("claim dirty SELECT")?;
    let claims = rows
        .into_iter()
        .map(|r| DirtyClaim {
            session_id: r.get(0),
            min_ts: r.get(1),
            max_ts: r.get(2),
            updated_at: r.get(3),
        })
        .collect();
    Ok((claims, snapshot))
}

pub async fn dirty_depth(pool: &Pool, metadata_schema: &str) -> Result<i64> {
    let client = pool.get().await.context("dirty_depth pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let n: i64 = client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {schema}.session_summary_dirty"),
            &[],
        )
        .await
        .context("dirty depth")?
        .get(0);
    Ok(n)
}

pub async fn load_summary_start_times(
    pool: &Pool,
    metadata_schema: &str,
    session_ids: &[String],
) -> Result<std::collections::HashMap<String, DateTime<Utc>>> {
    let mut out = std::collections::HashMap::new();
    if session_ids.is_empty() {
        return Ok(out);
    }
    let client = pool.get().await.context("load summary starts")?;
    let schema = quote_pg_ident(metadata_schema);
    // Build IN list with params.
    let mut sql = format!(
        "SELECT session_id, start_time FROM {schema}.session_summary WHERE session_id IN ("
    );
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    for (i, id) in session_ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("${}", i + 1));
        params.push(id);
    }
    sql.push(')');
    let rows = client
        .query(&sql, &params[..])
        .await
        .context("load summary start_time")?;
    for r in rows {
        out.insert(r.get(0), r.get(1));
    }
    Ok(out)
}

pub async fn ack_dirty(
    pool: &Pool,
    metadata_schema: &str,
    session_ids: &[String],
    snapshot: DateTime<Utc>,
) -> Result<u64> {
    if session_ids.is_empty() {
        return Ok(0);
    }
    let client = pool.get().await.context("ack_dirty pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let mut sql = format!(
        "DELETE FROM {schema}.session_summary_dirty \
         WHERE updated_at <= $1 AND session_id IN ("
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    params.push(Box::new(snapshot));
    for (i, id) in session_ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("${}", i + 2));
        params.push(Box::new(id.clone()));
    }
    sql.push(')');
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let n = client
        .execute(&sql, &param_refs[..])
        .await
        .context("ack dirty DELETE")?;
    Ok(n)
}

pub async fn upsert_summary_rows(
    pool: &Pool,
    metadata_schema: &str,
    rows: &[SummaryRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let client = pool.get().await.context("upsert summary pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let sql = compile_session_summary_upsert_sql(&schema, rows.len());
    let now = Utc::now();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    // Bind order must match SESSION_SUMMARY_UPSERT_COLUMNS in reduce_sql.rs.
    for r in rows {
        params.push(Box::new(r.session_id.clone()));
        params.push(Box::new(r.start_time));
        params.push(Box::new(r.end_time));
        params.push(Box::new(r.observation_count));
        params.push(Box::new(r.error_count));
        params.push(Box::new(r.input_tokens));
        params.push(Box::new(r.output_tokens));
        params.push(Box::new(r.total_tokens));
        params.push(Box::new(r.total_cost));
        params.push(Box::new(r.agent_name.clone()));
        params.push(Box::new(r.user_id.clone()));
        params.push(Box::new(r.model_name.clone()));
        params.push(Box::new(now));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    client
        .execute(&sql, &param_refs[..])
        .await
        .context("session_summary UPSERT")?;
    Ok(())
}

fn attach_ducklake(conn: &Connection, ducklake: &DuckLakeConfig) -> Result<()> {
    let attach_target = crate::storage::ducklake::ducklake_attach_target(ducklake);
    crate::storage::ducklake::prepare_local_ducklake_paths(ducklake, &attach_target)?;
    let opts = crate::storage::ducklake::ducklake_attach_options(ducklake);
    let attach_sql = format!(
        "ATTACH 'ducklake:{}' AS {} ({});",
        crate::storage::ducklake::escape_sql_literal(&attach_target),
        ducklake.catalog_alias,
        opts.join(", ")
    );
    conn.execute_batch(&attach_sql)?;
    Ok(())
}

fn open_reduce_connection(ducklake: &DuckLakeConfig) -> Result<Connection> {
    let conn = Connection::open_in_memory().context("open duckdb for reduce")?;
    crate::storage::ducklake::configure_duckdb_resources(
        &conn,
        crate::storage::ducklake::COMPACTION_DUCKDB_THREADS,
        crate::storage::ducklake::COMPACTION_DUCKDB_MEMORY,
    )
    .ok();
    attach_ducklake(&conn, ducklake)?;
    Ok(conn)
}

fn micros_to_utc(us: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us)
}

fn map_duck_row(row: &duckdb::Row<'_>) -> duckdb::Result<SummaryRow> {
    let start_us: i64 = row.get(1)?;
    let end_us: Option<i64> = row.get(2)?;
    let start_time = micros_to_utc(start_us)
        .ok_or_else(|| duckdb::Error::InvalidParameterName(format!("start_time_us={start_us}")))?;
    let end_time = match end_us {
        Some(us) => Some(
            micros_to_utc(us)
                .ok_or_else(|| duckdb::Error::InvalidParameterName(format!("end_time_us={us}")))?,
        ),
        None => None,
    };
    Ok(SummaryRow {
        session_id: row.get(0)?,
        start_time,
        end_time,
        observation_count: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
        error_count: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
        input_tokens: row.get(5)?,
        output_tokens: row.get(6)?,
        total_tokens: row.get(7)?,
        total_cost: row.get(8)?,
        agent_name: row.get(9)?,
        user_id: row.get(10)?,
        model_name: row.get(11)?,
    })
}

/// Run promoted-only aggregate against DuckLake `traces`.
pub fn aggregate_sessions_from_lake(
    ducklake: &DuckLakeConfig,
    session_ids: &[String],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<SummaryRow>> {
    let from_table = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, "traces");
    let sql = crate::session_summary::reduce_sql::compile_session_summary_reduce_sql(
        &from_table,
        session_ids,
        from,
        to,
    )?;
    let conn = open_reduce_connection(ducklake)?;
    let mut stmt = conn.prepare(&sql).context("prepare reduce SQL")?;
    let mapped = stmt
        .query_map([], map_duck_row)
        .context("query reduce")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("map reduce rows")?;
    Ok(mapped)
}

/// Full reduce pipeline for one tenant. Empty dirty → Ok no-op.
pub async fn reduce_tenant(
    pool: &Pool,
    metadata_schema: &str,
    tenant_id: &str,
    ducklake: &DuckLakeConfig,
    max_sessions: u64,
    max_reduce_span_seconds: u64,
) -> Result<usize> {
    let depth = match dirty_depth(pool, metadata_schema).await {
        Ok(n) => n,
        Err(err) => {
            warn!(
                tenant = %tenant_id,
                error = %err,
                "session_summary dirty_depth query failed"
            );
            0
        }
    };
    crate::self_monitoring::set_session_summary_dirty_depth(tenant_id, depth.max(0) as u64);

    let (claims, snapshot) = claim_dirty(pool, metadata_schema, max_sessions).await?;
    if claims.is_empty() {
        return Ok(0);
    }

    let ids: Vec<String> = claims.iter().map(|c| c.session_id.clone()).collect();
    let starts = load_summary_start_times(pool, metadata_schema, &ids).await?;
    let span = ChronoDuration::seconds(max_reduce_span_seconds as i64);
    let now = Utc::now();
    let (from, to) = batch_reduce_window(&claims, &starts, now, span)
        .ok_or_else(|| anyhow!("empty claims after claim"))?;

    let lag_secs = claims
        .iter()
        .map(|c| (now - c.updated_at).num_seconds().max(0) as u64)
        .max()
        .unwrap_or(0);
    crate::self_monitoring::record_session_summary_reducer_lag(tenant_id, lag_secs);

    let ducklake = ducklake.clone();
    let ids_for_lake = ids.clone();
    let rows = tokio::task::spawn_blocking(move || {
        aggregate_sessions_from_lake(&ducklake, &ids_for_lake, from, to)
    })
    .await
    .map_err(|e| anyhow!("reduce join: {e}"))??;

    upsert_summary_rows(pool, metadata_schema, &rows).await?;
    let acked = ack_dirty(pool, metadata_schema, &ids, snapshot).await?;
    if acked < ids.len() as u64 {
        warn!(
            tenant = %tenant_id,
            claimed = ids.len(),
            acked,
            "session_summary ack deleted fewer rows than claimed (concurrent dirty likely)"
        );
    }
    crate::self_monitoring::record_session_summary_sessions_reduced(tenant_id, rows.len() as u64);
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn bounds_use_summary_start_and_clamp() {
        let dirty_min = Utc.with_ymd_and_hms(2024, 1, 10, 0, 0, 0).unwrap();
        let dirty_max = Utc.with_ymd_and_hms(2024, 1, 10, 12, 0, 0).unwrap();
        let summary_start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2024, 1, 10, 12, 0, 0).unwrap();
        let (from, to) = compute_reduce_bounds(
            dirty_min,
            dirty_max,
            Some(summary_start),
            now,
            ChronoDuration::days(30),
        );
        assert_eq!(to, now);
        assert_eq!(from, summary_start);

        let (from_clamped, _) = compute_reduce_bounds(
            summary_start,
            dirty_max,
            Some(summary_start),
            now,
            ChronoDuration::days(2),
        );
        assert_eq!(from_clamped, now - ChronoDuration::days(2));
    }

    #[test]
    fn batch_window_unions_sessions() {
        let claims = vec![
            DirtyClaim {
                session_id: "a".into(),
                min_ts: Utc.with_ymd_and_hms(2024, 1, 5, 0, 0, 0).unwrap(),
                max_ts: Utc.with_ymd_and_hms(2024, 1, 5, 1, 0, 0).unwrap(),
                updated_at: Utc::now(),
            },
            DirtyClaim {
                session_id: "b".into(),
                min_ts: Utc.with_ymd_and_hms(2024, 1, 8, 0, 0, 0).unwrap(),
                max_ts: Utc.with_ymd_and_hms(2024, 1, 8, 2, 0, 0).unwrap(),
                updated_at: Utc::now(),
            },
        ];
        let now = Utc.with_ymd_and_hms(2024, 1, 8, 3, 0, 0).unwrap();
        let (from, to) =
            batch_reduce_window(&claims, &Default::default(), now, ChronoDuration::days(30))
                .unwrap();
        assert_eq!(from, claims[0].min_ts);
        assert_eq!(to, now);
    }
}
