//! OTLP telemetry explorer SQL recipes (search / details / field_values).

/// Sessions-scoped search aggregate over `traces`.
pub fn search_sessions_sql(where_sql: &str, order_sql: &str, limit: usize) -> String {
    format!(
        "SELECT session_id AS id, 'session' AS kind, session_id, MIN(timestamp) AS start_time, \
         MAX(COALESCE(end_timestamp, timestamp)) AS end_time, COUNT(DISTINCT trace_id) AS trace_count, \
         COUNT(*) AS span_count, \
         SUM(CASE WHEN status_code = 'ERROR' OR http_response_status_code >= 500 THEN 1 ELSE 0 END) AS error_count, \
         date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp))) AS duration_ms, \
         string_agg(DISTINCT app_id, ',') AS services, any_value(http_request_path) AS entry_path, \
         any_value(status_message) AS last_error \
         FROM traces {where_sql} GROUP BY session_id {order_sql} LIMIT {limit}"
    )
}

/// Traces-scoped search aggregate over `traces`.
pub fn search_traces_sql(where_sql: &str, order_sql: &str, limit: usize) -> String {
    format!(
        "SELECT trace_id AS id, 'trace' AS kind, session_id, trace_id, MIN(timestamp) AS start_time, \
         MAX(COALESCE(end_timestamp, timestamp)) AS end_time, COUNT(*) AS span_count, \
         SUM(CASE WHEN status_code = 'ERROR' OR http_response_status_code >= 500 THEN 1 ELSE 0 END) AS error_count, \
         date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp))) AS duration_ms, \
         string_agg(DISTINCT app_id, ',') AS services, any_value(message_type) AS name, \
         any_value(http_request_path) AS entry_path, any_value(status_message) AS last_error \
         FROM traces {where_sql} GROUP BY trace_id, session_id {order_sql} LIMIT {limit}"
    )
}

/// Span detail rows for a bound identity + time window.
pub fn details_spans_sql(span_cols: &str, where_sql: &str, limit: usize) -> String {
    format!("SELECT {span_cols} FROM traces WHERE {where_sql} ORDER BY timestamp ASC LIMIT {limit}")
}

/// Log detail rows for a bound identity + time window.
pub fn details_logs_sql(log_cols: &str, where_sql: &str, limit: usize) -> String {
    format!("SELECT {log_cols} FROM logs WHERE {where_sql} ORDER BY timestamp ASC LIMIT {limit}")
}

/// Generic detail SELECT over a telemetry relation.
pub fn detail_sql(
    table: &str,
    columns: &str,
    id_filter: &str,
    time_filter: Option<&str>,
    limit: usize,
) -> String {
    let mut parts: Vec<String> = vec![id_filter.to_string()];
    if let Some(time) = time_filter {
        parts.push(time.to_string());
    }
    let where_sql = parts.join(" AND ");
    format!("SELECT {columns} FROM {table} WHERE {where_sql} ORDER BY timestamp ASC LIMIT {limit}")
}

/// DISTINCT field values over `traces` for a bound window.
pub fn field_values_sql(field_sql: &str, where_sql: &str, limit: usize) -> String {
    format!(
        "SELECT DISTINCT {field_sql} AS value FROM traces WHERE {where_sql} ORDER BY value ASC LIMIT {limit}"
    )
}
