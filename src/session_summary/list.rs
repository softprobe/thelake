//! Stage 3: list sessions from catalog Postgres `session_summary`.

use crate::api::llm::query::{
    next_cursor_from_sessions, SessionOrderBy, SessionSearchRequest, SessionSearchResponse,
    SessionSummary, SortDirection,
};
use crate::runtime_engine::quote_pg_ident;
use anyhow::Context;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;

/// List-path errors: filter/cursor contract → 400; storage → 5xx.
#[derive(Debug)]
pub enum SessionSummaryListError {
    BadRequest(String),
    Storage(anyhow::Error),
}

impl From<anyhow::Error> for SessionSummaryListError {
    fn from(value: anyhow::Error) -> Self {
        Self::Storage(value)
    }
}

/// Query Postgres `session_summary` → public list shape (`SessionSummary` only).
///
/// Never joins `traces`, never returns spans or `SessionDetail`. Detail stays on
/// `GET /v1/llm/sessions/{id}` (lake). Empty table → empty page. No lake fallback.
pub async fn search_session_summary(
    pool: &Pool,
    metadata_schema: &str,
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<SessionSearchResponse, SessionSummaryListError> {
    search_session_summary_for_workspace(pool, metadata_schema, None, request, limit).await
}

pub async fn search_session_summary_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: Option<&str>,
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<SessionSearchResponse, SessionSummaryListError> {
    let schema = quote_pg_ident(metadata_schema);
    let sql = crate::sql::session_summary::compile_session_summary_list_sql_for_workspace(
        &schema,
        workspace_id,
        request,
        limit,
    )
    .map_err(SessionSummaryListError::BadRequest)?;
    let client = pool
        .get()
        .await
        .context("summary list pool")
        .map_err(SessionSummaryListError::Storage)?;
    let rows = client
        .query(&sql, &[])
        .await
        .context("session_summary list SELECT")
        .map_err(SessionSummaryListError::Storage)?;

    let cursor_supported =
        request.order_by == SessionOrderBy::StartTime && request.order == SortDirection::Desc;

    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        items
            .push(map_pg_summary_row(row).map_err(|e| SessionSummaryListError::Storage(e.into()))?);
    }

    let next_cursor = if cursor_supported {
        next_cursor_from_sessions(&mut items, limit)
    } else {
        items.truncate(limit);
        None
    };

    Ok(SessionSearchResponse {
        items,
        next_cursor,
        cursor_supported,
    })
}

/// Load `start_time`/`end_time` for one session from Postgres `session_summary`.
///
/// Used by session detail / observations / recording so lake scans use the
/// summary window (D7) — not the Explorer list range.
pub async fn lookup_session_summary_window(
    pool: &Pool,
    metadata_schema: &str,
    session_id: &str,
) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>, SessionSummaryListError> {
    lookup_session_summary_window_for_workspace(pool, metadata_schema, None, session_id).await
}

pub async fn lookup_session_summary_window_for_workspace(
    pool: &Pool,
    metadata_schema: &str,
    workspace_id: Option<&str>,
    session_id: &str,
) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>, SessionSummaryListError> {
    let schema = quote_pg_ident(metadata_schema);
    let ownership = workspace_id
        .map(|id| format!("tenant_id = {} AND ", crate::sql::sql_string_literal(id)))
        .unwrap_or_default();
    let sql = format!(
        "SELECT start_time, end_time FROM {schema}.session_summary WHERE {ownership}session_id = $1 LIMIT 1"
    );
    let client = pool
        .get()
        .await
        .context("summary lookup pool")
        .map_err(SessionSummaryListError::Storage)?;
    let rows = client
        .query(&sql, &[&session_id])
        .await
        .context("session_summary window SELECT")
        .map_err(SessionSummaryListError::Storage)?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let start_time: DateTime<Utc> = row
        .try_get("start_time")
        .map_err(|e| SessionSummaryListError::Storage(e.into()))?;
    let end_time: DateTime<Utc> = row
        .try_get("end_time")
        .map_err(|e| SessionSummaryListError::Storage(e.into()))?;
    Ok(Some((start_time, end_time)))
}

fn map_pg_summary_row(row: &tokio_postgres::Row) -> Result<SessionSummary, tokio_postgres::Error> {
    let user_ids: Vec<String> = row.try_get("user_ids").unwrap_or_default();
    let models: Vec<String> = row.try_get("models").unwrap_or_default();
    Ok(SessionSummary {
        session_id: row.try_get("session_id")?,
        start_time: row.try_get::<_, DateTime<Utc>>("start_time")?,
        end_time: row.try_get("end_time")?,
        trace_count: row.try_get::<_, i64>("trace_count").unwrap_or(0),
        observation_count: row.try_get::<_, i64>("observation_count").unwrap_or(0),
        error_count: row.try_get::<_, i64>("error_count").unwrap_or(0),
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        total_tokens: row.try_get("total_tokens")?,
        total_cost: row.try_get("total_cost")?,
        agent_name: row.try_get("agent_name")?,
        user_ids,
        models,
    })
}
