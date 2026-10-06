//! Postgres `session_summary` list SQL for Stage 3 `sessions/search`.
//!
//! Steady-state list path: no DuckLake scan. Filters use typed summary columns.
//! Postgres stores event-time bounds as signed epoch nanoseconds.

use crate::session_summary::list_query::{SessionOrderBy, SessionSearchRequest, SortDirection};
use crate::sql::literal::sql_string_literal;
use crate::sql::paging::decode_cursor;

/// Compile a single-session window lookup against `{schema}.session_summary`.
///
/// Uses a bind parameter `$1` for `session_id` (caller supplies the value).
pub fn compile_session_summary_window_lookup_sql(
    schema_quoted: &str,
    workspace_id: Option<&str>,
) -> String {
    let ownership = workspace_id
        .map(|id| format!("workspace_id = {} AND ", sql_string_literal(id)))
        .unwrap_or_default();
    format!(
        "SELECT start_time_ns, COALESCE(end_time_ns, start_time_ns) AS end_time_ns \
         FROM {schema_quoted}.session_summary \
         WHERE {ownership}session_id = $1 LIMIT 1"
    )
}

/// Compile a Postgres SELECT against `{schema}.session_summary`.
///
/// Filters covered (must stay in sync with tests):
/// - time range on `start_time_ns`
/// - `has_errors`
/// - `agent_name`
/// - `user_id`
/// - `model_name`
/// - keyset `cursor` on `(start_time_ns, session_id)` desc
///
/// `roots_only` is a lake-only MAP filter; ignored on the summary path (no parent id column).
pub fn compile_session_summary_list_sql(
    schema_quoted: &str,
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<String, String> {
    compile_session_summary_list_sql_for_workspace(schema_quoted, None, request, limit)
}

pub fn compile_session_summary_list_sql_for_workspace(
    schema_quoted: &str,
    workspace_id: Option<&str>,
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<String, String> {
    if request.from > request.to {
        return Err("`from` must be <= `to`".to_string());
    }

    let mut predicates = vec![
        format!("start_time_ns >= {}", timestamp_ns(&request.from)?),
        format!("start_time_ns <= {}", timestamp_ns(&request.to)?),
    ];
    if let Some(workspace_id) = workspace_id {
        predicates.insert(
            0,
            format!("workspace_id = {}", sql_string_literal(workspace_id)),
        );
    }

    if request.has_errors == Some(true) {
        predicates.push("error_count > 0".to_string());
    } else if request.has_errors == Some(false) {
        predicates.push("error_count = 0".to_string());
    }

    if let Some(agent) = request
        .agent_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        predicates.push(format!("agent_name = {}", sql_string_literal(agent.trim())));
    }
    if let Some(user_id) = request.user_id.as_deref().filter(|v| !v.trim().is_empty()) {
        predicates.push(format!("user_id = {}", sql_string_literal(user_id.trim())));
    }
    if let Some(model) = request
        .model_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        predicates.push(format!("model_name = {}", sql_string_literal(model.trim())));
    }

    if let Some(cursor) = request.cursor.as_deref().filter(|v| !v.is_empty()) {
        if request.order_by != SessionOrderBy::StartTime {
            return Err("`cursor` is only supported with order_by=start_time".to_string());
        }
        if request.order != SortDirection::Desc {
            return Err("`cursor` is only supported with order=desc".to_string());
        }
        predicates.push(pg_cursor_predicate(cursor, "start_time_ns", "session_id")?);
    }

    let direction = request.order.as_sql();
    let order_sql = match request.order_by {
        SessionOrderBy::StartTime => {
            format!("start_time_ns {direction}, session_id {direction}")
        }
        SessionOrderBy::ErrorCount => {
            format!("error_count {direction}, start_time_ns DESC, session_id DESC")
        }
        SessionOrderBy::Duration => {
            // end_time_ns may be NULL → treat as start_time_ns (zero duration).
            format!(
                "(COALESCE(end_time_ns, start_time_ns) - start_time_ns) {direction}, \
                 start_time_ns DESC, session_id DESC"
            )
        }
        SessionOrderBy::TotalTokens => {
            format!("total_tokens {direction} NULLS LAST, start_time_ns DESC, session_id DESC")
        }
        SessionOrderBy::TotalCost => {
            format!("total_cost {direction} NULLS LAST, start_time_ns DESC, session_id DESC")
        }
    };

    // Project API-shaped columns. trace_count is not stored on summary → 0.
    // user_ids / models are arrays built from singular typed columns.
    Ok(format!(
        "SELECT \
           session_id, \
           start_time_ns, \
           end_time_ns, \
           0::bigint AS trace_count, \
           observation_count, \
           error_count, \
           input_tokens, \
           output_tokens, \
           total_tokens, \
           total_cost, \
           agent_name, \
           CASE WHEN user_id IS NULL OR user_id = '' THEN ARRAY[]::text[] ELSE ARRAY[user_id] END AS user_ids, \
           CASE WHEN model_name IS NULL OR model_name = '' THEN ARRAY[]::text[] ELSE ARRAY[model_name] END AS models \
         FROM {schema_quoted}.session_summary \
         WHERE {where_sql} \
         ORDER BY {order_sql} \
         LIMIT {fetch}",
        where_sql = predicates.join(" AND "),
        order_sql = order_sql,
        fetch = limit + 1,
    ))
}

fn timestamp_ns(value: &chrono::DateTime<chrono::Utc>) -> Result<i64, String> {
    value
        .timestamp_nanos_opt()
        .ok_or_else(|| "timestamp is outside signed nanosecond range".to_string())
}

/// Keyset cursor uses the same exact epoch-ns representation as the summary.
fn pg_cursor_predicate(cursor: &str, timestamp_col: &str, id_col: &str) -> Result<String, String> {
    let decoded = decode_cursor(cursor)?;
    let ts = timestamp_ns(&decoded.t)?;
    Ok(format!(
        "({timestamp_col} < {ts} OR ({timestamp_col} = {ts} AND {id_col} < {id}))",
        id = sql_string_literal(&decoded.id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn base_request() -> SessionSearchRequest {
        SessionSearchRequest {
            from: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap(),
            has_errors: None,
            user_id: None,
            model_name: None,
            agent_name: None,
            roots_only: true,
            order_by: SessionOrderBy::StartTime,
            order: SortDirection::Desc,
            limit: Some(50),
            cursor: None,
        }
    }

    #[test]
    fn list_sql_time_range_and_limit() {
        let sql = compile_session_summary_list_sql("\"meta\"", &base_request(), 50).unwrap();
        assert!(sql.contains("FROM \"meta\".session_summary"));
        assert!(sql.contains("start_time_ns >="));
        assert!(sql.contains("start_time_ns <="));
        assert!(sql.contains("LIMIT 51"));
        assert!(!sql.to_lowercase().contains("attributes"));
        assert!(!sql.contains("FROM traces"));
    }

    #[test]
    fn list_sql_has_errors_true_and_false() {
        let mut req = base_request();
        req.has_errors = Some(true);
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("error_count > 0"));

        req.has_errors = Some(false);
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("error_count = 0"));
    }

    #[test]
    fn list_sql_agent_user_model_filters() {
        let mut req = base_request();
        req.agent_name = Some("agent-a".into());
        req.user_id = Some("u1".into());
        req.model_name = Some("gpt-4o".into());
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("agent_name = 'agent-a'"));
        assert!(sql.contains("user_id = 'u1'"));
        assert!(sql.contains("model_name = 'gpt-4o'"));
    }

    #[test]
    fn list_sql_cursor_desc_start_time() {
        use crate::sql::paging::encode_cursor;
        let mut req = base_request();
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        req.cursor = Some(encode_cursor(ts, "sess-1"));
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("start_time_ns <"));
        assert!(sql.contains("session_id < 'sess-1'"));
        // Must not use DuckDB TIMESTAMP_NS on the Postgres path.
        assert!(!sql.contains("TIMESTAMP_NS"));
    }

    #[test]
    fn list_sql_rejects_cursor_with_bad_order() {
        use crate::sql::paging::encode_cursor;
        let mut req = base_request();
        req.cursor = Some(encode_cursor(req.from, "x"));
        req.order_by = SessionOrderBy::ErrorCount;
        assert!(compile_session_summary_list_sql("\"meta\"", &req, 10).is_err());

        req.order_by = SessionOrderBy::StartTime;
        req.order = SortDirection::Asc;
        let err = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap_err();
        assert!(err.contains("order=desc"), "{err}");
    }

    #[test]
    fn list_sql_rejects_from_after_to_and_malformed_cursor() {
        let mut req = base_request();
        std::mem::swap(&mut req.from, &mut req.to);
        assert!(compile_session_summary_list_sql("\"meta\"", &req, 10)
            .unwrap_err()
            .contains("from"));

        req = base_request();
        req.cursor = Some("not-a-valid-cursor!!!".into());
        assert!(compile_session_summary_list_sql("\"meta\"", &req, 10)
            .unwrap_err()
            .contains("malformed cursor"));
    }

    #[test]
    fn list_sql_whitespace_filters_ignored_quotes_escaped() {
        let mut req = base_request();
        req.agent_name = Some("  \t  ".into());
        req.user_id = Some("".into());
        req.model_name = Some("   ".into());
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(!sql.contains("AND agent_name ="));
        assert!(!sql.contains("AND user_id ="));
        assert!(!sql.contains("AND model_name ="));

        req.agent_name = Some("  O'Brien  ".into());
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(
            sql.contains("agent_name = 'O''Brien'"),
            "must trim + escape quotes: {sql}"
        );
    }

    #[test]
    fn workspace_list_sql_filters_summary_rows_by_ownership() {
        let sql = compile_session_summary_list_sql_for_workspace(
            "\"meta\"",
            Some("workspace'42"),
            &base_request(),
            10,
        )
        .unwrap();
        assert!(sql.contains("workspace_id = 'workspace''42'"));
    }

    #[test]
    fn list_sql_nulls_last_on_token_cost_orders() {
        let mut req = base_request();
        req.order_by = SessionOrderBy::TotalTokens;
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("NULLS LAST"), "{sql}");
        req.order_by = SessionOrderBy::TotalCost;
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(sql.contains("NULLS LAST"), "{sql}");
    }

    #[test]
    fn list_sql_order_variants() {
        let mut req = base_request();
        for (order_by, needle) in [
            (SessionOrderBy::ErrorCount, "error_count"),
            (
                SessionOrderBy::Duration,
                "COALESCE(end_time_ns, start_time_ns)",
            ),
            (SessionOrderBy::TotalTokens, "total_tokens"),
            (SessionOrderBy::TotalCost, "total_cost"),
        ] {
            req.order_by = order_by;
            let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
            assert!(sql.contains(needle), "missing {needle} in {sql}");
        }
    }

    #[test]
    fn duckdb_helpers_still_available_for_lake_path() {
        // Sanity: lake cursor helpers remain TIMESTAMP_NS for DuckDB.
        use crate::sql::timestamp_ns_literal;
        let _ = timestamp_ns_literal(&Utc::now());
    }

    #[test]
    fn list_sql_keeps_submicrosecond_bounds_as_integer_nanoseconds() {
        let mut req = base_request();
        req.from = chrono::DateTime::from_timestamp(1_700_000_000, 123).unwrap();
        req.to = req.from;
        let sql = compile_session_summary_list_sql("\"meta\"", &req, 10).unwrap();
        assert!(
            sql.contains("start_time_ns >= 1700000000000000123"),
            "{sql}"
        );
        assert!(
            sql.contains("start_time_ns <= 1700000000000000123"),
            "{sql}"
        );
        assert!(!sql.contains("timestamptz"), "{sql}");
    }
}
