//! Claim dirty → bounds+clamp → lake aggregate → UPSERT → ack.

use crate::config::Config;
use crate::runtime_engine::quote_pg_ident;
use crate::sql::session_summary::compile_session_summary_upsert_sql;
use crate::workspace_scope::PhysicalScope;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use deadpool_postgres::Pool;
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

#[allow(dead_code)]
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

pub async fn claim_dirty_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
    limit: u64,
) -> Result<(Vec<DirtyClaim>, DateTime<Utc>)> {
    let client = pool.get().await.context("claim workspace dirty pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let snapshot = Utc::now();
    let rows = client
        .query(
            &format!(
                "SELECT session_id, min_ts, max_ts, updated_at \
                 FROM {schema}.session_summary_dirty \
                 WHERE tenant_id = $1 ORDER BY updated_at ASC LIMIT {limit}"
            ),
            &[&workspace_id],
        )
        .await
        .context("claim workspace dirty SELECT")?;
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

#[allow(dead_code)]
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

pub async fn dirty_depth_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
) -> Result<i64> {
    let client = pool.get().await.context("workspace dirty_depth pool")?;
    let schema = quote_pg_ident(metadata_schema);
    Ok(client
        .query_one(
            &format!(
                "SELECT count(*)::bigint FROM {schema}.session_summary_dirty WHERE tenant_id = $1"
            ),
            &[&workspace_id],
        )
        .await
        .context("workspace dirty depth")?
        .get(0))
}

#[allow(dead_code)]
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

pub async fn load_summary_start_times_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
    session_ids: &[String],
) -> Result<std::collections::HashMap<String, DateTime<Utc>>> {
    let mut out = std::collections::HashMap::new();
    if session_ids.is_empty() {
        return Ok(out);
    }
    let client = pool.get().await.context("load workspace summary starts")?;
    let schema = quote_pg_ident(metadata_schema);
    let mut sql = format!(
        "SELECT session_id, start_time FROM {schema}.session_summary WHERE tenant_id = $1 AND session_id IN ("
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(workspace_id.to_string())];
    for (i, id) in session_ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("${}", i + 2));
        params.push(Box::new(id.clone()));
    }
    sql.push(')');
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    for row in client
        .query(&sql, &refs[..])
        .await
        .context("load workspace summary start_time")?
    {
        out.insert(row.get(0), row.get(1));
    }
    Ok(out)
}

#[allow(dead_code)]
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

pub async fn ack_dirty_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
    session_ids: &[String],
    snapshot: DateTime<Utc>,
) -> Result<u64> {
    if session_ids.is_empty() {
        return Ok(0);
    }
    let client = pool.get().await.context("ack workspace dirty pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let mut sql = format!(
        "DELETE FROM {schema}.session_summary_dirty WHERE tenant_id = $1 AND updated_at <= $2 AND session_id IN ("
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(workspace_id.to_string()), Box::new(snapshot)];
    for (i, id) in session_ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("${}", i + 3));
        params.push(Box::new(id.clone()));
    }
    sql.push(')');
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    client
        .execute(&sql, &refs[..])
        .await
        .context("ack workspace dirty DELETE")
}

#[allow(dead_code)]
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
    // Bind order must match SESSION_SUMMARY_UPSERT_COLUMNS in sql/session_summary/reduce_sql.rs.
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

pub async fn upsert_summary_rows_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: &str,
    rows: &[SummaryRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let client = pool.get().await.context("upsert workspace summary pool")?;
    let schema = quote_pg_ident(metadata_schema);
    let sql = crate::sql::session_summary::compile_session_summary_upsert_sql_for_workspace(
        &schema,
        rows.len(),
    );
    let now = Utc::now();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    for r in rows {
        params.push(Box::new(workspace_id.to_string()));
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
        .context("workspace session_summary UPSERT")?;
    Ok(())
}

/// Reject inverted or oversized rebuild windows (ops + periodic share this).
pub fn validate_rebuild_window(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    max_reduce_span_seconds: u64,
) -> Result<(), String> {
    if from > to {
        return Err("`from` must be <= `to`".to_string());
    }
    let span_secs = (to - from).num_seconds();
    if span_secs < 0 {
        return Err("`from` must be <= `to`".to_string());
    }
    if (span_secs as u64) > max_reduce_span_seconds {
        return Err(format!(
            "rebuild window exceeds max_reduce_span_seconds ({max_reduce_span_seconds})"
        ));
    }
    Ok(())
}

/// Window rebuild: lake aggregate (no IN-list) → absolute UPSERT. No dirty claim/ack.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rebuild_tenant_window(
    pool: &Pool,
    metadata_schema: &str,
    config: &Config,
    scope: &PhysicalScope,
    tenant_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    max_reduce_span_seconds: u64,
) -> Result<usize> {
    validate_rebuild_window(from, to, max_reduce_span_seconds).map_err(|msg| anyhow!(msg))?;
    let workspace_scoped =
        config.ducklake.workspace_scope_mode == crate::workspace_scope::WorkspaceScopeMode::Shared;
    let config = config.clone();
    let scope = scope.clone();
    let tenant_id_for_lake = tenant_id.to_string();
    let rows = tokio::task::spawn_blocking(move || {
        let workspace_filter = workspace_scoped.then_some(tenant_id_for_lake.as_str());
        crate::compaction::session_summary_access::aggregate_sessions_from_lake(
            &config,
            &scope,
            None,
            workspace_filter,
            from,
            to,
        )
    })
    .await
    .map_err(|e| anyhow!("rebuild join: {e}"))??;
    if workspace_scoped {
        upsert_summary_rows_for_workspace(pool, metadata_schema, tenant_id, &rows).await?;
    } else {
        upsert_summary_rows(pool, metadata_schema, &rows).await?;
    }
    Ok(rows.len())
}

/// Full reduce pipeline for one tenant. Empty dirty → Ok no-op.
pub(crate) async fn reduce_tenant(
    pool: &Pool,
    metadata_schema: &str,
    tenant_id: &str,
    config: &Config,
    scope: &PhysicalScope,
    max_sessions: u64,
    max_reduce_span_seconds: u64,
) -> Result<usize> {
    let workspace_scoped =
        config.ducklake.workspace_scope_mode == crate::workspace_scope::WorkspaceScopeMode::Shared;
    let depth = match if workspace_scoped {
        dirty_depth_for_workspace(pool, metadata_schema, tenant_id).await
    } else {
        dirty_depth(pool, metadata_schema).await
    } {
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

    let (claims, snapshot) = if workspace_scoped {
        claim_dirty_for_workspace(pool, metadata_schema, tenant_id, max_sessions).await?
    } else {
        claim_dirty(pool, metadata_schema, max_sessions).await?
    };
    if claims.is_empty() {
        return Ok(0);
    }

    let ids: Vec<String> = claims.iter().map(|c| c.session_id.clone()).collect();
    let starts = if workspace_scoped {
        load_summary_start_times_for_workspace(pool, metadata_schema, tenant_id, &ids).await?
    } else {
        load_summary_start_times(pool, metadata_schema, &ids).await?
    };
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

    let config = config.clone();
    let scope = scope.clone();
    let ids_for_lake = ids.clone();
    let tenant_id_for_lake = tenant_id.to_string();
    let rows = tokio::task::spawn_blocking(move || {
        let workspace_filter = workspace_scoped.then_some(tenant_id_for_lake.as_str());
        crate::compaction::session_summary_access::aggregate_sessions_from_lake(
            &config,
            &scope,
            Some(&ids_for_lake),
            workspace_filter,
            from,
            to,
        )
    })
    .await
    .map_err(|e| anyhow!("reduce join: {e}"))??;

    if workspace_scoped {
        upsert_summary_rows_for_workspace(pool, metadata_schema, tenant_id, &rows).await?;
    } else {
        upsert_summary_rows(pool, metadata_schema, &rows).await?;
    }
    let acked = if workspace_scoped {
        ack_dirty_for_workspace(pool, metadata_schema, tenant_id, &ids, snapshot).await?
    } else {
        ack_dirty(pool, metadata_schema, &ids, snapshot).await?
    };
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

    #[test]
    fn rebuild_window_rejects_inverted_and_oversized() {
        let from = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        assert!(validate_rebuild_window(from, to, 86400).is_ok());
        assert!(validate_rebuild_window(to, from, 86400).is_err());
        assert!(validate_rebuild_window(from, to, 3600).is_err());
    }

    #[test]
    fn prepare_reduce_duckdb_installs_gcs_secret_for_gs_data_path() {
        let prev_id = std::env::var("GCS_HMAC_ACCESS_KEY_ID").ok();
        let prev_secret = std::env::var("GCS_HMAC_SECRET").ok();
        std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", "reduce-test-key");
        std::env::set_var("GCS_HMAC_SECRET", "reduce-test-secret");

        let mut config = Config::default();
        config.ducklake.data_path = "gs://softprobe-test/ducklake/".to_string();
        let scope = crate::workspace_scope::PhysicalScope::from_ducklake(&config.ducklake);
        let result = crate::compaction::session_summary_access::prepare_session_summary_duckdb(
            &config, &scope,
        );

        match prev_id {
            Some(v) => std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", v),
            None => std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(v) => std::env::set_var("GCS_HMAC_SECRET", v),
            None => std::env::remove_var("GCS_HMAC_SECRET"),
        }

        let conn = result.expect("prepare_reduce_duckdb");
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_secrets() WHERE name = 'gcs_hmac'",
                [],
                |row| row.get(0),
            )
            .expect("duckdb_secrets");
        assert_eq!(
            n, 1,
            "reduce/rebuild must install GCS secret before scanning Parquet"
        );
    }
}
