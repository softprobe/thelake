//! Typed telemetry explorer SQL compilation (search / details / field_values).

use crate::sql::literal::{sql_string_literal, timestamp_ns_literal_from_str};
use crate::sql::{push_otlp_time_predicates, QueryWindow};
use crate::storage::schema::attribute_map::attribute_map_as_json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TelemetrySearchScope {
    Sessions,
    Traces,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TelemetrySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryTimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFilter {
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TelemetryFilterExpr {
    And { and: Vec<TelemetryFilterExpr> },
    Or { or: Vec<TelemetryFilterExpr> },
    Predicate(TelemetryFilter),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySort {
    pub field: String,
    pub direction: TelemetrySortDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetrySearchRequest {
    pub version: u32,
    pub scope: TelemetrySearchScope,
    #[serde(default)]
    pub time_range: Option<TelemetryTimeRange>,
    #[serde(default)]
    pub filter: Option<TelemetryFilterExpr>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub sort: Vec<TelemetrySort>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryDetailsTarget {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct CompiledDetailsSql {
    pub spans: String,
    pub logs: String,
}

#[derive(Clone, Copy)]
pub struct FieldSpec {
    pub key: &'static str,
    pub sql: &'static str,
    pub value_type: &'static str,
    pub entity: &'static str,
    pub filterable: bool,
    pub sortable: bool,
    pub projectable: bool,
    pub ops: &'static [&'static str],
}

const STRING_OPS: &[&str] = &["eq", "neq", "in", "not_in", "prefix", "contains", "exists"];
const ID_OPS: &[&str] = &["eq", "neq", "in", "not_in", "exists"];
const NUMBER_OPS: &[&str] = &[
    "eq", "neq", "in", "not_in", "lt", "lte", "gt", "gte", "exists",
];
const TIME_OPS: &[&str] = &["eq", "neq", "lt", "lte", "gt", "gte", "exists"];

pub const SEARCH_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        key: "session_id",
        sql: "session_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "trace_id",
        sql: "trace_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "span_id",
        sql: "span_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "service.name",
        sql: "app_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "timestamp",
        sql: "timestamp",
        value_type: "timestamp",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: TIME_OPS,
    },
    FieldSpec {
        key: "http_request_method",
        sql: "http_request_method",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "http_request_path",
        sql: "http_request_path",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "http_response_status_code",
        sql: "http_response_status_code",
        value_type: "int",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: NUMBER_OPS,
    },
    FieldSpec {
        key: "status_code",
        sql: "status_code",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
];

/// Compile the typed telemetry search request into an allowlisted DuckDB query.
pub fn compile_search_sql(request: &TelemetrySearchRequest) -> Result<String, String> {
    if request.version != 1 {
        return Err("unsupported telemetry search version".to_string());
    }
    if request.cursor.is_some() {
        return Err("cursor pagination is not implemented for this endpoint".to_string());
    }
    let time_range = request
        .time_range
        .as_ref()
        .ok_or_else(|| "timeRange is required".to_string())?;
    let window = query_window_from_time_range(time_range)?;

    let filter_pred = match &request.filter {
        Some(filter) => Some(compile_filter_expr(filter)?),
        None => None,
    };
    let mut conditions = Vec::new();
    push_otlp_time_predicates(&mut conditions, &window, filter_pred);
    let where_sql = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let limit = request.limit.unwrap_or(100).clamp(1, 1000);
    let order_sql = compile_order(&request.sort)?;

    let sql = match request.scope {
        TelemetrySearchScope::Sessions => super::search_sessions_sql(&where_sql, &order_sql, limit),
        TelemetrySearchScope::Traces => super::search_traces_sql(&where_sql, &order_sql, limit),
    };

    Ok(sql)
}

fn approve(sql: String) -> Result<crate::sql::trusted::TrustedSql, String> {
    crate::sql::trusted::approved_query(sql).map_err(|error| error.to_string())
}

/// Compile and approve the telemetry search recipe for trusted execution.
pub(crate) fn search(
    request: &TelemetrySearchRequest,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_search_sql(request)?)
}

/// Compile and approve the telemetry details spans recipe for trusted execution.
pub(crate) fn details_spans(
    target: &TelemetryDetailsTarget,
    time_range: &TelemetryTimeRange,
    limit: usize,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_details_sql(target, time_range, limit)?.spans)
}

/// Compile and approve the telemetry details logs recipe for trusted execution.
pub(crate) fn details_logs(
    target: &TelemetryDetailsTarget,
    time_range: &TelemetryTimeRange,
    limit: usize,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_details_sql(target, time_range, limit)?.logs)
}

/// Compile and approve field_values DISTINCT SQL for trusted execution.
pub(crate) fn field_values(
    field_sql: &str,
    window: &QueryWindow,
    limit: usize,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_field_values_sql(field_sql, window, limit))
}

/// Compile detail queries for all correlated telemetry signals.
pub fn compile_details_sql(
    target: &TelemetryDetailsTarget,
    time_range: &TelemetryTimeRange,
    limit: usize,
) -> Result<CompiledDetailsSql, String> {
    let limit = limit.clamp(1, 5000);
    let escaped_id = sql_string_literal(&target.id);
    let (span_filter, log_filter) = match target.kind.as_str() {
        "session" => (
            format!("session_id = {escaped_id}"),
            format!("session_id = {escaped_id}"),
        ),
        "trace" => (
            format!("trace_id = {escaped_id}"),
            format!("trace_id = {escaped_id}"),
        ),
        _ => return Err("target.kind must be session or trace".to_string()),
    };

    let window = query_window_from_time_range(time_range)?;
    let mut span_conds = Vec::new();
    push_otlp_time_predicates(&mut span_conds, &window, [span_filter]);
    let mut log_conds = Vec::new();
    push_otlp_time_predicates(&mut log_conds, &window, [log_filter]);
    let span_cols = format!(
        "session_id, trace_id, span_id, parent_span_id, app_id, message_type, span_kind, timestamp, end_timestamp, status_code, status_message, http_request_method, http_request_path, http_request_headers, http_request_body, http_response_status_code, http_response_headers, http_response_body, agent_id, agent_name, {}",
        attribute_map_as_json("attributes")
    );
    let log_cols = format!(
        "session_id, timestamp, severity_number, severity_text, body, trace_id, span_id, agent_id, agent_name, {}, {}",
        attribute_map_as_json("attributes"),
        attribute_map_as_json("resource_attributes")
    );

    Ok(CompiledDetailsSql {
        spans: super::details_spans_sql(&span_cols, &span_conds.join(" AND "), limit),
        logs: super::details_logs_sql(&log_cols, &log_conds.join(" AND "), limit),
    })
}

pub fn query_window_from_time_range(
    time_range: &TelemetryTimeRange,
) -> Result<QueryWindow, String> {
    let from = chrono::DateTime::parse_from_rfc3339(&time_range.from)
        .map_err(|e| format!("invalid timeRange.from: {e}"))?
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339(&time_range.to)
        .map_err(|e| format!("invalid timeRange.to: {e}"))?
        .with_timezone(&chrono::Utc);
    QueryWindow::try_new(from, to)
}

/// Build field_values DISTINCT SQL (identity → timestamp).
pub fn compile_field_values_sql(field_sql: &str, window: &QueryWindow, limit: usize) -> String {
    let mut conditions = Vec::new();
    push_otlp_time_predicates(
        &mut conditions,
        window,
        [format!("{field_sql} IS NOT NULL")],
    );
    super::field_values_sql(field_sql, &conditions.join(" AND "), limit)
}

pub fn compile_order(sort: &[TelemetrySort]) -> Result<String, String> {
    if sort.is_empty() {
        return Ok("ORDER BY end_time DESC".to_string());
    }
    let mut parts = Vec::new();
    for sort_item in sort {
        let sql = match sort_item.field.as_str() {
            "timestamp" => "end_time",
            "duration_ms" => "duration_ms",
            "error_count" => "error_count",
            "span_count" => "span_count",
            "trace_count" => "trace_count",
            other => field_spec(other)
                .filter(|field| field.sortable)
                .map(|field| field.sql)
                .ok_or_else(|| format!("unsupported sort field: {other}"))?,
        };
        let dir = match sort_item.direction {
            TelemetrySortDirection::Asc => "ASC",
            TelemetrySortDirection::Desc => "DESC",
        };
        parts.push(format!("{sql} {dir}"));
    }
    Ok(format!("ORDER BY {}", parts.join(", ")))
}

pub fn compile_filter_expr(expr: &TelemetryFilterExpr) -> Result<String, String> {
    match expr {
        TelemetryFilterExpr::And { and } => compile_compound("AND", and),
        TelemetryFilterExpr::Or { or } => compile_compound("OR", or),
        TelemetryFilterExpr::Predicate(filter) => compile_filter(filter),
    }
}

pub fn compile_compound(joiner: &str, exprs: &[TelemetryFilterExpr]) -> Result<String, String> {
    if exprs.is_empty() {
        return Err("compound filter cannot be empty".to_string());
    }
    let parts = exprs
        .iter()
        .map(compile_filter_expr)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", parts.join(&format!(" {joiner} "))))
}

pub fn compile_filter(filter: &TelemetryFilter) -> Result<String, String> {
    let spec = field_spec(&filter.field)
        .filter(|field| field.filterable)
        .ok_or_else(|| format!("unknown filter field: {}", filter.field))?;
    if !spec.ops.contains(&filter.op.as_str()) {
        return Err(format!(
            "operator {} is not allowed for field {}",
            filter.op, filter.field
        ));
    }
    let sql = spec.sql;
    let literal = |value: &Value| -> Result<String, String> {
        if spec.value_type == "timestamp" {
            Ok(timestamp_ns_literal_from_str(&string_value(value)?))
        } else {
            Ok(scalar_literal(value))
        }
    };
    match filter.op.as_str() {
        "exists" => Ok(format!("{sql} IS NOT NULL")),
        "eq" => Ok(format!("{sql} = {}", literal(required_value(filter)?)?)),
        "neq" => Ok(format!("{sql} <> {}", literal(required_value(filter)?)?)),
        "lt" => Ok(format!("{sql} < {}", literal(required_value(filter)?)?)),
        "lte" => Ok(format!("{sql} <= {}", literal(required_value(filter)?)?)),
        "gt" => Ok(format!("{sql} > {}", literal(required_value(filter)?)?)),
        "gte" => Ok(format!("{sql} >= {}", literal(required_value(filter)?)?)),
        "prefix" => Ok(format!(
            "{sql} LIKE {}",
            sql_string_literal(&format!("{}%", string_value(required_value(filter)?)?))
        )),
        "contains" => Ok(format!(
            "{sql} LIKE {}",
            sql_string_literal(&format!("%{}%", string_value(required_value(filter)?)?))
        )),
        "in" | "not_in" => {
            let values = required_value(filter)?
                .as_array()
                .ok_or_else(|| "in/not_in requires an array value".to_string())?;
            if values.is_empty() {
                return Err("in/not_in requires at least one value".to_string());
            }
            let literals = values
                .iter()
                .map(literal)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            let op = if filter.op == "in" { "IN" } else { "NOT IN" };
            Ok(format!("{sql} {op} ({literals})"))
        }
        _ => Err("unsupported operator".to_string()),
    }
}

pub fn field_spec(key: &str) -> Option<FieldSpec> {
    SEARCH_FIELDS.iter().copied().find(|field| field.key == key)
}

fn required_value(filter: &TelemetryFilter) -> Result<&Value, String> {
    filter
        .value
        .as_ref()
        .ok_or_else(|| format!("operator {} requires value", filter.op))
}

fn string_value(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| "operator requires a string value".to_string())
}

fn scalar_literal(value: &Value) -> String {
    match value {
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => sql_string_literal(s),
        _ => sql_string_literal(&value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::assert_sql_has_otlp_time_predicates;
    use serde_json::json;

    #[test]
    fn session_details_prefer_first_class_session_id_on_spans_and_logs() {
        let range = TelemetryTimeRange {
            from: "2023-11-14T22:13:20.000000001Z".into(),
            to: "2023-11-14T22:13:20.000000002Z".into(),
        };
        let compiled = compile_details_sql(
            &TelemetryDetailsTarget {
                kind: "session".into(),
                id: "sess-1".into(),
            },
            &range,
            50,
        )
        .unwrap();
        assert!(compiled.spans.contains("session_id = 'sess-1'"));
        assert!(compiled.logs.contains("session_id = 'sess-1'"));
        assert!(compiled.spans.contains("timestamp"));
        assert!(!compiled.spans.contains("record_date"));
        assert!(compiled.logs.contains("timestamp"));
        assert!(!compiled.logs.contains("record_date"));
    }

    #[test]
    fn bounded_log_details_use_timestamp_ns_without_changing_other_tables() {
        let target = TelemetryDetailsTarget {
            kind: "session".into(),
            id: "session-1".into(),
        };
        let range = TelemetryTimeRange {
            from: "2023-11-14T22:13:20.000000001Z".into(),
            to: "2023-11-14T22:13:20.000000002Z".into(),
        };
        let compiled = compile_details_sql(&target, &range, 100).unwrap();

        assert!(compiled.spans.contains("timestamp"));
        assert!(compiled.logs.contains("timestamp"));
        assert!(compiled.spans.contains("timestamp"));
        assert!(!compiled.spans.contains("record_date"));
        assert!(compiled.logs.contains("timestamp"));
        assert!(!compiled.logs.contains("record_date"));
        assert!(!compiled.logs.contains("TIMESTAMPTZ"));
    }

    #[test]
    fn search_without_time_range_rejected() {
        let request = TelemetrySearchRequest {
            version: 1,
            scope: TelemetrySearchScope::Traces,
            time_range: None,
            filter: None,
            columns: Vec::new(),
            sort: Vec::new(),
            limit: Some(10),
            cursor: None,
        };
        let err = compile_search_sql(&request).unwrap_err();
        assert!(err.contains("timeRange is required"));
    }

    #[test]
    fn field_values_window_emits_otlp_day_and_timestamp() {
        let window = query_window_from_time_range(&TelemetryTimeRange {
            from: "2023-11-14T22:13:20.000000001Z".into(),
            to: "2023-11-14T22:13:20.000000002Z".into(),
        })
        .unwrap();
        let sql = compile_field_values_sql("app_id", &window, 100);
        assert_sql_has_otlp_time_predicates(&sql);
        assert!(sql.contains("app_id IS NOT NULL"));
        let id = sql.find("app_id IS NOT NULL").unwrap();
        let ts = sql.find("timestamp >=").unwrap();
        assert!(id < ts, "identity before timestamp: {sql}");
    }

    #[test]
    fn telemetry_otlp_compilers_inventory_emit_day_and_timestamp() {
        let range = TelemetryTimeRange {
            from: "2023-11-14T22:13:20.000000001Z".into(),
            to: "2023-11-14T22:13:20.000000002Z".into(),
        };
        for scope in [TelemetrySearchScope::Traces, TelemetrySearchScope::Sessions] {
            let search = compile_search_sql(&TelemetrySearchRequest {
                version: 1,
                scope,
                time_range: Some(range.clone()),
                filter: None,
                columns: Vec::new(),
                sort: Vec::new(),
                limit: Some(10),
                cursor: None,
            })
            .unwrap();
            assert_sql_has_otlp_time_predicates(&search);
        }

        let filtered = compile_search_sql(&TelemetrySearchRequest {
            version: 1,
            scope: TelemetrySearchScope::Traces,
            time_range: Some(range.clone()),
            filter: Some(TelemetryFilterExpr::Predicate(TelemetryFilter {
                field: "session_id".into(),
                op: "eq".into(),
                value: Some(json!("sess-1")),
            })),
            columns: Vec::new(),
            sort: Vec::new(),
            limit: Some(10),
            cursor: None,
        })
        .unwrap();
        assert_sql_has_otlp_time_predicates(&filtered);
        let id = filtered.find("session_id = 'sess-1'").unwrap();
        let ts = filtered.find("timestamp >=").unwrap();
        assert!(id < ts, "search filter before timestamp: {filtered}");

        let details = compile_details_sql(
            &TelemetryDetailsTarget {
                kind: "session".into(),
                id: "sess-1".into(),
            },
            &range,
            50,
        )
        .unwrap();
        assert_sql_has_otlp_time_predicates(&details.spans);
        assert_sql_has_otlp_time_predicates(&details.logs);
        let id = details.spans.find("session_id = ").unwrap();
        let ts = details.spans.find("timestamp >=").unwrap();
        assert!(
            id < ts,
            "details identity before timestamp: {}",
            details.spans
        );
    }

    #[test]
    fn timestamp_filter_uses_timestamp_ns_for_trace_search() {
        let request = TelemetrySearchRequest {
            version: 1,
            scope: TelemetrySearchScope::Traces,
            time_range: Some(TelemetryTimeRange {
                from: "2023-11-14T22:13:20.000000001Z".into(),
                to: "2023-11-14T22:13:20.000000002Z".into(),
            }),
            filter: Some(TelemetryFilterExpr::Predicate(TelemetryFilter {
                field: "timestamp".into(),
                op: "gte".into(),
                value: Some(json!("2023-11-14T22:13:20.000000003Z")),
            })),
            columns: Vec::new(),
            sort: Vec::new(),
            limit: Some(10),
            cursor: None,
        };

        let sql = compile_search_sql(&request).unwrap();

        assert!(sql.contains("timestamp >= '2023-11-14T22:13:20.000000003Z'::TIMESTAMP_NS"));
        assert!(!sql.contains("'2023-11-14T22:13:20.000000003Z'::TIMESTAMPTZ"));
    }

    #[test]
    fn details_spans_trusted_wrapper_matches_compile() {
        let target = TelemetryDetailsTarget {
            kind: "trace".into(),
            id: "trace-1".into(),
        };
        let range = TelemetryTimeRange {
            from: "2023-11-14T22:13:20.000000001Z".into(),
            to: "2023-11-14T22:13:20.000000002Z".into(),
        };
        let compiled = compile_details_sql(&target, &range, 50).unwrap();
        let trusted = details_spans(&target, &range, 50).expect("approve");
        assert_eq!(trusted.as_str(), compiled.spans);
    }
}
