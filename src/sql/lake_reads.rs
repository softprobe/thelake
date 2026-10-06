//! Lake read recipes used by the tenant-bound query engine.
//!
//! Predicate assembly and [`approved_query`] live here so the query engine stays
//! execute-only: call a recipe, then `execute_trusted`.

use crate::sql::literal::sql_string_literal;
use crate::sql::trusted::{approved_query, TrustedSql, TrustedSqlError};
use crate::sql::QueryWindow;
use crate::storage::schema::attribute_map::attribute_map_varchar;

#[derive(Debug, Clone)]
pub struct LogCountFilter {
    pub time_window: QueryWindow,
    pub session_id: Option<String>,
    pub body: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TraceCountFilter {
    pub time_window: QueryWindow,
    pub session_id: Option<String>,
    pub app_id: Option<String>,
    pub span_id: Option<String>,
}

pub(crate) fn count_logs(filter: &LogCountFilter) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(filter.time_window);
    if let Some(session_id) = &filter.session_id {
        predicates.push(format!("session_id = {}", sql_string_literal(session_id)));
    }
    if let Some(body) = &filter.body {
        predicates.push(format!("body = {}", sql_string_literal(body)));
    }
    if let Some(trace_id) = &filter.trace_id {
        predicates.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }
    approved_query(count_logs_sql(&predicates.join(" AND ")))
}

pub(crate) fn count_traces(filter: &TraceCountFilter) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(filter.time_window);
    add_trace_filter_predicates(&mut predicates, filter);
    approved_query(count_traces_sql(&predicates.join(" AND ")))
}

pub(crate) fn find_http_span(
    session_id: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!("session_id = {}", sql_string_literal(session_id)));
    predicates.push("http_request_method IS NOT NULL".to_string());
    approved_query(find_http_span_sql(&predicates.join(" AND ")))
}

pub(crate) fn count_trace_days(
    session_id: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!("session_id = {}", sql_string_literal(session_id)));
    approved_query(count_trace_days_sql(&predicates.join(" AND ")))
}

pub(crate) fn count_traces_by_attribute(
    session_id: &str,
    key: &str,
    value: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!("session_id = {}", sql_string_literal(session_id)));
    predicates.push(format!(
        "{} = {}",
        attribute_map_varchar("attributes", key),
        sql_string_literal(value)
    ));
    count_rows("traces", &predicates)
}

pub(crate) fn count_logs_by_attribute(
    key: &str,
    value: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!(
        "{} = {}",
        attribute_map_varchar("attributes", key),
        sql_string_literal(value)
    ));
    count_rows("logs", &predicates)
}

pub(crate) fn trace_attributes_by_attribute(
    session_id: &str,
    key: &str,
    value: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!("session_id = {}", sql_string_literal(session_id)));
    predicates.push(format!(
        "{} = {}",
        attribute_map_varchar("attributes", key),
        sql_string_literal(value)
    ));
    approved_query(trace_attributes_sql(&predicates.join(" AND ")))
}

pub(crate) fn trace_attributes_for_span(
    span_id: &str,
    time_window: QueryWindow,
) -> Result<TrustedSql, TrustedSqlError> {
    let mut predicates = timestamp_predicates(time_window);
    predicates.push(format!("span_id = {}", sql_string_literal(span_id)));
    approved_query(trace_attributes_sql(&predicates.join(" AND ")))
}

fn count_rows(table: &str, predicates: &[String]) -> Result<TrustedSql, TrustedSqlError> {
    approved_query(count_rows_sql(table, &predicates.join(" AND ")))
}

fn timestamp_predicates(window: QueryWindow) -> Vec<String> {
    window
        .timestamp_filter_sql("")
        .split(" AND ")
        .map(str::to_owned)
        .collect()
}

fn add_trace_filter_predicates(predicates: &mut Vec<String>, filter: &TraceCountFilter) {
    for (column, value) in [
        ("session_id", filter.session_id.as_deref()),
        ("app_id", filter.app_id.as_deref()),
        ("span_id", filter.span_id.as_deref()),
    ] {
        if let Some(value) = value {
            predicates.push(format!("{column} = {}", sql_string_literal(value)));
        }
    }
}

fn count_logs_sql(predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT AS count FROM logs WHERE {predicates}")
}

fn count_traces_sql(predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT AS count FROM traces WHERE {predicates}")
}

fn find_http_span_sql(predicates: &str) -> String {
    format!(
        "SELECT http_request_method, http_request_path, http_request_headers, http_request_body, http_response_status_code, http_response_headers, http_response_body FROM traces WHERE {predicates} LIMIT 1"
    )
}

fn count_trace_days_sql(predicates: &str) -> String {
    format!(
        "SELECT COUNT(*)::BIGINT FROM (SELECT strftime(timestamp, '%Y-%m-%d') FROM traces WHERE {predicates} GROUP BY 1) days"
    )
}

fn trace_attributes_sql(predicates: &str) -> String {
    format!("SELECT CAST(attributes AS JSON) FROM traces WHERE {predicates} LIMIT 1")
}

fn count_rows_sql(table: &str, predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE {predicates}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn sample_window() -> QueryWindow {
        QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
        )
        .unwrap()
    }

    fn assert_bare_timestamp_bounds(sql: &str) {
        assert!(sql.contains("timestamp >="), "missing lower bound: {sql}");
        assert!(sql.contains("timestamp <="), "missing upper bound: {sql}");
        assert!(
            !sql.contains("make_timestamp_ns(epoch_ns(timestamp))"),
            "wrapped timestamp breaks day prune: {sql}"
        );
    }

    #[test]
    fn count_logs_recipe_bounds_and_escapes_filters() {
        let sql = count_logs(&LogCountFilter {
            time_window: sample_window(),
            session_id: Some("ses_O'Brien".into()),
            body: Some("it's fine".into()),
            trace_id: Some("tr1".into()),
        })
        .expect("trusted")
        .as_str()
        .to_string();
        assert!(sql.contains("FROM logs"));
        assert_bare_timestamp_bounds(&sql);
        assert!(sql.contains("session_id = 'ses_O''Brien'"));
        assert!(sql.contains("body = 'it''s fine'"));
        assert!(sql.contains("trace_id = 'tr1'"));
    }

    #[test]
    fn count_traces_and_http_span_recipes_are_bounded() {
        let count = count_traces(&TraceCountFilter {
            time_window: sample_window(),
            session_id: Some("s1".into()),
            app_id: Some("app".into()),
            span_id: None,
        })
        .expect("trusted")
        .as_str()
        .to_string();
        assert!(count.contains("FROM traces"));
        assert_bare_timestamp_bounds(&count);
        assert!(count.contains("session_id = 's1'"));
        assert!(count.contains("app_id = 'app'"));

        let http = find_http_span("s1", sample_window())
            .expect("trusted")
            .as_str()
            .to_string();
        assert!(http.contains("FROM traces"));
        assert!(http.contains("http_request_method IS NOT NULL"));
        assert!(http.contains("LIMIT 1"));
        assert_bare_timestamp_bounds(&http);
    }

    #[test]
    fn attribute_count_recipes_use_attribute_map_varchar() {
        let sql = count_traces_by_attribute("s1", "sp.user.id", "u'1", sample_window())
            .expect("trusted")
            .as_str()
            .to_string();
        assert!(sql.contains("FROM traces"));
        assert_bare_timestamp_bounds(&sql);
        assert!(sql.contains(&attribute_map_varchar("attributes", "sp.user.id")));
        assert!(sql.contains("'u''1'"));
    }
}
