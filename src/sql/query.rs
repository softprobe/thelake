//! Query recipes used by the tenant-bound query engine.

pub fn count_logs_sql(predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT AS count FROM logs WHERE {predicates}")
}

pub fn count_traces_sql(predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT AS count FROM traces WHERE {predicates}")
}

pub fn find_http_span_sql(predicates: &str) -> String {
    format!(
        "SELECT http_request_method, http_request_path, http_request_headers, http_request_body, http_response_status_code, http_response_headers, http_response_body FROM traces WHERE {predicates} LIMIT 1"
    )
}

pub fn count_trace_days_sql(predicates: &str) -> String {
    format!(
        "SELECT COUNT(*)::BIGINT FROM (SELECT strftime(timestamp, '%Y-%m-%d') FROM traces WHERE {predicates} GROUP BY 1) days"
    )
}

pub fn trace_attributes_sql(predicates: &str) -> String {
    format!("SELECT CAST(attributes AS JSON) FROM traces WHERE {predicates} LIMIT 1")
}

pub fn count_rows_sql(table: &str, predicates: &str) -> String {
    format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE {predicates}")
}
