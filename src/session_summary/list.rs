//! Stage 3: list sessions from catalog Postgres `session_summary`.

use crate::api::llm::query::{
    SessionOrderBy, SessionSearchRequest, SessionSearchResponse, SessionSummary, SortDirection,
};
use crate::api::sql_support::encode_cursor;
use crate::runtime_engine::quote_pg_ident;
use crate::session_summary::list_sql::compile_session_summary_list_sql;
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
    let schema = quote_pg_ident(metadata_schema);
    let sql = compile_session_summary_list_sql(&schema, request, limit)
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

fn next_cursor_from_sessions(items: &mut Vec<SessionSummary>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.session_id))
}
