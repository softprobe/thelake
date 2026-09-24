//! Tempo / TraceQL lake scan SQL recipes (one clock: `timestamp` only).

use crate::compat::tempo::traceql::{is_duration_field, parse_duration_ns, TraceSelector};
use crate::sql::literal::sql_string_literal;
use crate::sql::query_window_from_exclusive_ns;
use crate::storage::schema::variant::prefer_attr_varchar;
use std::collections::BTreeMap;

/// Status-code name → numeric mappings embedded in Tempo predicate SQL.
pub(crate) const PERSISTED_OTLP_STATUS_CODES: [(&str, i64); 12] = [
    ("STATUS_CODE_UNSET", 0),
    ("STATUS_CODE_OK", 1),
    ("STATUS_CODE_ERROR", 2),
    ("UNSET", 0),
    ("OK", 1),
    ("ERROR", 2),
    ("unset", 0),
    ("ok", 1),
    ("error", 2),
    ("status_code_unset", 0),
    ("status_code_ok", 1),
    ("status_code_error", 2),
];

/// Default lookback when Tempo clients omit one or both of start/end.
/// Long enough to cover Phase 3 fixture timestamps (~2023) under CI "now".
/// Still a finite [`QueryWindow`] — never an unbounded lake scan.
const TEMPO_DEFAULT_LOOKBACK_NS: i64 = 10 * 365 * 24 * 60 * 60 * 1_000_000_000;

/// Inputs for a Tempo lake scan (protocol adapters map request types here).
#[derive(Debug, Clone, Copy)]
pub struct TraceScanParams<'a> {
    pub tags: &'a BTreeMap<String, String>,
    pub selector: Option<&'a TraceSelector>,
    pub min_duration_ns: Option<i64>,
    pub max_duration_ns: Option<i64>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: usize,
}

/// Resolve exclusive Tempo `[start, end)` for lake scans.
///
/// - Both set and `start < end` → use them
/// - Both omitted → lookback ending at now
/// - Only `end` → `[end - lookback, end)`
/// - Only `start` → `[start, start + lookback)`
/// - Zero-width / inverted → error (callers short-circuit zero-width)
fn resolve_tempo_scan_window(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
) -> Result<(i64, i64), String> {
    match (start_ns, end_ns) {
        (Some(start), Some(end)) if start < end => Ok((start, end)),
        (Some(start), Some(end)) if start == end => Err("empty_tempo_window".to_string()),
        (Some(_), Some(_)) => Err("`start` must be < `end`".to_string()),
        (None, None) => {
            let end = chrono::Utc::now()
                .timestamp_nanos_opt()
                .ok_or_else(|| "current time out of range".to_string())?;
            let start = end.saturating_sub(TEMPO_DEFAULT_LOOKBACK_NS);
            Ok((start, end))
        }
        (None, Some(end)) => {
            let start = end.saturating_sub(TEMPO_DEFAULT_LOOKBACK_NS);
            if start >= end {
                return Err("`start` must be < `end`".to_string());
            }
            Ok((start, end))
        }
        (Some(start), None) => {
            let end = start.saturating_add(TEMPO_DEFAULT_LOOKBACK_NS);
            if start >= end {
                return Err("`start` must be < `end`".to_string());
            }
            Ok((start, end))
        }
    }
}

/// Build the bounded raw trace scan. Protocol adapters never construct SQL.
///
/// Emits `make_timestamp_ns(epoch_ns(timestamp))` lower/upper via [`QueryWindow`]
/// (exclusive end → inclusive). Omitted bounds get a finite default lookback
/// so every lake scan still has a QueryWindow; never an open-ended scan.
pub fn trace_scan_sql(
    params: TraceScanParams<'_>,
    trace_id: Option<&str>,
) -> Result<String, String> {
    let (start_ns, end_ns) = resolve_tempo_scan_window(params.start_ns, params.end_ns)?;
    let window = query_window_from_exclusive_ns(start_ns, end_ns)?;

    let tag_predicates = params
        .tags
        .iter()
        .map(|(key, value)| tag_predicate_sql(key, value))
        .collect::<Vec<_>>();
    let selector_predicate = params
        .selector
        .map(selector_sql)
        .unwrap_or_else(|| "TRUE".into());
    let row_predicate = tag_predicates
        .into_iter()
        .chain([selector_predicate])
        .map(|predicate| format!("({predicate})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut duration_predicates = Vec::new();
    if let Some(min) = params.min_duration_ns {
        duration_predicates.push(format!(
            "MAX(COALESCE(end_time_unix_nano, start_time_unix_nano)) - MIN(start_time_unix_nano) >= {min}"
        ));
    }
    if let Some(max) = params.max_duration_ns {
        duration_predicates.push(format!(
            "MAX(COALESCE(end_time_unix_nano, start_time_unix_nano)) - MIN(start_time_unix_nano) <= {max}"
        ));
    }
    let duration_predicate = if duration_predicates.is_empty() {
        "TRUE".to_string()
    } else {
        duration_predicates.join(" AND ")
    };
    let cap = trace_scan_cap(params.limit);
    let trace_id = trace_id.map(str::to_owned);

    Ok(window
        .bind_scan("", |bound| {
            let where_base = match trace_id.as_deref() {
                Some(id) => format!("trace_id = {} AND {bound}", sql_string_literal(id)),
                None => bound.to_string(),
            };
            format!(
                "WITH base AS (SELECT trace_id, span_id, parent_span_id, message_type, span_kind, app_id, \
                 CAST(epoch_ns(timestamp) AS BIGINT) AS start_time_unix_nano, \
                 CAST(epoch_ns(end_timestamp) AS BIGINT) AS end_time_unix_nano, \
                 CAST(attributes AS JSON) AS attributes, CAST(resource_attributes AS JSON) AS resource_attributes, \
                 CAST(instrumentation_scope AS JSON) AS instrumentation_scope, CAST(links AS JSON) AS links, \
                 status_code, status_message, \
                 CAST(events AS JSON) AS events, \
                 observation_type, model_name, model_provider, user_id, session_attr_id, service_name \
                 FROM traces WHERE {where_base}), \
                 matching_traces AS (SELECT DISTINCT trace_id FROM base WHERE {row_predicate}), \
                 qualified_traces AS (SELECT trace_id FROM base GROUP BY trace_id HAVING {duration_predicate} ) \
                 SELECT base.* FROM base \
                 INNER JOIN matching_traces USING (trace_id) \
                 INNER JOIN qualified_traces USING (trace_id) \
                 ORDER BY base.trace_id ASC, base.start_time_unix_nano ASC, base.span_id ASC LIMIT {cap}"
            )
        })
        .into_sql())
}

pub fn trace_scan_cap(limit: usize) -> usize {
    limit.saturating_mul(100).clamp(10_000, 100_000)
}

/// A scan reaching the cap is incomplete/ambiguous because the bounded query
/// cannot distinguish exactly-cap rows from rows that continue beyond it.
pub fn scan_reached_cap(row_count: usize, cap: usize) -> bool {
    row_count >= cap
}

fn json_path(key: &str) -> String {
    format!("$.\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""))
}

fn json_string(column: &str, key: &str) -> String {
    format!(
        "json_extract_string(CAST({column} AS JSON), {})",
        sql_string_literal(&json_path(key))
    )
}

fn span_attribute_value(key: &str) -> String {
    if let Some(expr) = promoted_attr_expr(key) {
        return expr;
    }
    format!(
        "COALESCE({}, {})",
        json_string("attributes", key),
        json_string("resource_attributes", key)
    )
}

/// Prefer product-hot / first-class columns from traces-query-hot-attrs.yaml.
fn promoted_attr_expr(key: &str) -> Option<String> {
    let promo = crate::sql::llm::llm_promo();
    match key {
        "sp.observation.type" => Some(prefer_attr_varchar(
            Some(promo.observation_type),
            "attributes",
            "sp.observation.type",
        )),
        "gen_ai.request.model" => Some(prefer_attr_varchar(
            Some(promo.model_name),
            "attributes",
            "gen_ai.request.model",
        )),
        "gen_ai.provider.name" => Some(prefer_attr_varchar(
            Some(promo.model_provider),
            "attributes",
            "gen_ai.provider.name",
        )),
        "sp.user.id" => Some(prefer_attr_varchar(
            Some(promo.user_id),
            "attributes",
            "sp.user.id",
        )),
        "sp.session.id" => Some(prefer_attr_varchar(
            Some("session_attr_id"),
            "attributes",
            "sp.session.id",
        )),
        "service.name" => Some(format!(
            "COALESCE(service_name, {}, {}, app_id)",
            json_string("attributes", key),
            json_string("resource_attributes", key)
        )),
        _ => None,
    }
}

fn span_status_code_sql() -> String {
    format!(
        "COALESCE({}, status_code)",
        json_string("attributes", "status_code")
    )
}

fn persisted_status_code_numeric_sql(value: &str) -> String {
    let mappings = PERSISTED_OTLP_STATUS_CODES
        .iter()
        .map(|(name, code)| format!("WHEN {} THEN {code}", sql_string_literal(name)))
        .collect::<Vec<_>>()
        .join(" ");
    format!("CASE {value} {mappings} ELSE TRY_CAST({value} AS BIGINT) END")
}

fn is_persisted_status_code_field(field: &crate::compat::tempo::traceql::TraceField) -> bool {
    crate::compat::tempo::traceql::is_status_field(field)
}

fn tag_value_sql(key: &str) -> String {
    match key {
        "name" => "message_type".to_string(),
        "kind" => "span_kind".to_string(),
        "status" => "status_code".to_string(),
        "service.name" => promoted_attr_expr("service.name").expect("service.name mapping"),
        _ => span_attribute_value(key),
    }
}

fn tag_predicate_sql(key: &str, expected: &str) -> String {
    let actual = tag_value_sql(key);
    format!(
        "strpos(lower(COALESCE(CAST(({actual}) AS VARCHAR), '')), lower({})) > 0",
        sql_string_literal(expected)
    )
}

fn selector_sql(selector: &TraceSelector) -> String {
    match selector {
        TraceSelector::Predicate(predicate) => predicate_sql(predicate),
        TraceSelector::And(left, right) => {
            format!("({} AND {})", selector_sql(left), selector_sql(right))
        }
        TraceSelector::Or(left, right) => {
            format!("({} OR {})", selector_sql(left), selector_sql(right))
        }
    }
}

fn predicate_sql(predicate: &crate::compat::tempo::traceql::TracePredicate) -> String {
    use crate::compat::tempo::traceql::{is_numeric_field, TraceField, TracePredicate};
    let (field, operator, expected) = match predicate {
        TracePredicate::Eq(field, expected) => (field, "=", expected),
        TracePredicate::NotEq(field, expected) => (field, "!=", expected),
        TracePredicate::Regex(field, expected) => (field, "regex", expected),
        TracePredicate::NotRegex(field, expected) => (field, "not_regex", expected),
        TracePredicate::Greater(field, expected) => (field, ">", expected),
        TracePredicate::GreaterOrEqual(field, expected) => (field, ">=", expected),
        TracePredicate::Less(field, expected) => (field, "<", expected),
        TracePredicate::LessOrEqual(field, expected) => (field, "<=", expected),
    };
    let actual = match field {
        TraceField::Span(key) if key == "status_code" || key == "status" => span_status_code_sql(),
        TraceField::Span(key) => span_attribute_value(key),
        TraceField::Resource(key) => {
            if key == "service.name" {
                promoted_attr_expr("service.name").expect("service.name mapping")
            } else {
                json_string("resource_attributes", key)
            }
        }
        TraceField::Instrumentation(key) => json_string("instrumentation_scope", key),
        TraceField::Intrinsic(key) => match key.as_str() {
            "name" | "span:name" | "span.name" => "message_type".into(),
            "kind" | "span:kind" | "span.kind" => "span_kind".into(),
            "status" | "span:status" | "span.status" => span_status_code_sql(),
            "status_code" | "span:status_code" | "span.status_code" => span_status_code_sql(),
            "statusMessage" | "span:statusMessage" | "span.statusMessage" => {
                "status_message".into()
            }
            "duration" | "span:duration" | "span.duration" | "traceDuration" | "trace:duration"
            | "trace.duration" => {
                "COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano".into()
            }
            _ => "NULL".into(),
        },
    };
    let is_status = is_persisted_status_code_field(field);
    let numeric = is_numeric_field(field);
    let actual_numeric = if is_status || numeric {
        persisted_status_code_numeric_sql(&actual)
    } else {
        actual.clone()
    };
    let missing_guard = format!("{actual} IS NOT NULL");
    match operator {
        "regex" => {
            if is_status {
                format!(
                    "{missing_guard} AND (regexp_matches(CAST({actual} AS VARCHAR), {}) OR regexp_matches(CASE {actual_numeric} WHEN 0 THEN 'unset' WHEN 1 THEN 'ok' WHEN 2 THEN 'error' ELSE '' END, {}))",
                    sql_string_literal(expected.as_str()),
                    sql_string_literal(expected.as_str())
                )
            } else {
                format!(
                    "{missing_guard} AND regexp_matches(CAST({actual} AS VARCHAR), {})",
                    sql_string_literal(expected.as_str())
                )
            }
        }
        "not_regex" => {
            if is_status {
                format!(
                    "NOT ({missing_guard} AND (regexp_matches(CAST({actual} AS VARCHAR), {}) OR regexp_matches(CASE {actual_numeric} WHEN 0 THEN 'unset' WHEN 1 THEN 'ok' WHEN 2 THEN 'error' ELSE '' END, {})))",
                    sql_string_literal(expected.as_str()),
                    sql_string_literal(expected.as_str())
                )
            } else {
                format!(
                    "NOT regexp_matches(COALESCE(CAST({actual} AS VARCHAR), ''), {})",
                    sql_string_literal(expected.as_str())
                )
            }
        }
        comparison => {
            if is_duration_field(field) {
                let right = parse_duration_ns(expected.as_str())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".into());
                format!("{missing_guard} AND CAST(({actual}) AS BIGINT) {comparison} {right}")
            } else if is_status {
                let right = crate::compat::tempo::traceql::canonical_status_code(expected.as_str())
                    .map(|c| c.to_string())
                    .or_else(|| expected.as_str().parse::<i64>().ok().map(|c| c.to_string()))
                    .unwrap_or_else(|| "NULL".into());
                format!("{missing_guard} AND {actual_numeric} {comparison} {right}")
            } else if numeric {
                let right = expected
                    .as_str()
                    .parse::<i64>()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| "NULL".into());
                format!("{missing_guard} AND {actual_numeric} {comparison} {right}")
            } else {
                let right = sql_string_literal(expected.as_str());
                format!("{missing_guard} AND CAST({actual} AS VARCHAR) {comparison} {right}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::tempo::params::parse_tempo_search_params;

    const START_NS: i64 = 1_700_000_000_000_000_000;
    const END_NS: i64 = 1_700_000_100_000_000_000;

    fn windowed<'a>(
        tags: &'a BTreeMap<String, String>,
        selector: Option<&'a TraceSelector>,
        min_duration_ns: Option<i64>,
        max_duration_ns: Option<i64>,
        limit: usize,
    ) -> TraceScanParams<'a> {
        TraceScanParams {
            tags,
            selector,
            min_duration_ns,
            max_duration_ns,
            start_ns: Some(START_NS),
            end_ns: Some(END_NS),
            limit,
        }
    }

    #[test]
    fn tag_predicates_prefer_product_hot_promoted_columns() {
        let tags = BTreeMap::from([
            (
                String::from("sp.observation.type"),
                String::from("generation"),
            ),
            (String::from("service.name"), String::from("api")),
        ]);
        let sql = trace_scan_sql(windowed(&tags, None, None, None, 5), None).expect("sql");
        assert!(sql.contains("COALESCE(observation_type,"));
        let obs = sql.find("observation_type").expect("observation_type");
        let bag = sql
            .find("attributes['sp.observation.type']")
            .expect("bag fallback");
        assert!(obs < bag, "promoted observation_type must lead bag access");
        assert!(sql.contains("COALESCE(service_name,"));
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        assert_sql_has_otlp_time_predicates(&sql);
    }

    #[test]
    fn trace_scan_defaults_lookback_when_bounds_omitted_or_partial() {
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        let empty = BTreeMap::new();
        let omitted = trace_scan_sql(
            TraceScanParams {
                tags: &empty,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: None,
                end_ns: None,
                limit: 5,
            },
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .expect("default lookback");
        assert_sql_has_otlp_time_predicates(&omitted);

        let end_only = trace_scan_sql(
            TraceScanParams {
                tags: &empty,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: None,
                end_ns: Some(END_NS),
                limit: 5,
            },
            None,
        )
        .expect("end-only lookback");
        assert_sql_has_otlp_time_predicates(&end_only);

        let start_only = trace_scan_sql(
            TraceScanParams {
                tags: &empty,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: Some(START_NS),
                end_ns: None,
                limit: 5,
            },
            None,
        )
        .expect("start-only lookback");
        assert_sql_has_otlp_time_predicates(&start_only);
    }

    #[test]
    fn tempo_trace_scan_inventory_emits_timestamp_bound() {
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        let empty = BTreeMap::new();
        let sql = trace_scan_sql(windowed(&empty, None, None, None, 5), Some("abc")).expect("sql");
        assert_sql_has_otlp_time_predicates(&sql);
        assert!(!sql.contains("record_date"));
        let id = sql.find("trace_id = 'abc'").unwrap();
        let ts = sql.find("make_timestamp_ns(epoch_ns(timestamp))").unwrap();
        assert!(id < ts, "identity before timestamp: {sql}");
    }

    #[test]
    fn trace_scan_is_tenant_neutral_and_bounded() {
        let params = parse_tempo_search_params(
            &[("limit".into(), "5".into())],
            &crate::compat::tenant::QueryLimits::default(),
        )
        .unwrap();
        let sql = trace_scan_sql(
            TraceScanParams {
                tags: &params.tags,
                selector: params.selector.as_ref(),
                min_duration_ns: params.min_duration_ns,
                max_duration_ns: params.max_duration_ns,
                start_ns: Some(START_NS),
                end_ns: Some(END_NS),
                limit: params.limit,
            },
            Some("trace-1"),
        )
        .expect("sql");
        assert!(sql.contains("FROM traces"));
        assert!(sql.contains("trace_id = 'trace-1'"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(!sql.contains("tenant_id ="));
    }

    #[test]
    fn trace_scan_qualifies_supported_filters_before_result_limit() {
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.name = "otel-rust" && resource.service.name = "api" }"#,
        )
        .unwrap();
        let tags = BTreeMap::from([(String::from("deployment.environment"), String::from("prod"))]);
        let sql = trace_scan_sql(
            windowed(&tags, Some(&selector), Some(1_000_000), Some(2_000_000), 5),
            None,
        )
        .expect("sql");
        assert!(sql.contains("matching_traces AS"));
        assert!(sql.contains("qualified_traces AS"));
        assert!(sql.contains("instrumentation_scope"));
        assert!(sql.contains("deployment.environment"));
        assert!(sql.contains("HAVING"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(sql.find("matching_traces AS").unwrap() < sql.find("LIMIT 10000").unwrap());
    }

    #[test]
    fn resolve_tempo_scan_window_rejects_empty_and_inverted() {
        let empty = BTreeMap::new();
        let err = trace_scan_sql(
            TraceScanParams {
                tags: &empty,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: Some(START_NS),
                end_ns: Some(START_NS),
                limit: 5,
            },
            None,
        )
        .expect_err("empty window");
        assert_eq!(err, "empty_tempo_window");

        let inverted = trace_scan_sql(
            TraceScanParams {
                tags: &empty,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: Some(END_NS),
                end_ns: Some(START_NS),
                limit: 5,
            },
            None,
        )
        .expect_err("inverted window");
        assert!(inverted.contains("`start` must be < `end`"), "{inverted}");
    }

    #[test]
    fn a_scan_reaching_the_cap_is_explicitly_incomplete() {
        assert!(scan_reached_cap(10_000, trace_scan_cap(5)));
        assert!(scan_reached_cap(10_001, trace_scan_cap(5)));
        assert!(!scan_reached_cap(9_999, trace_scan_cap(5)));
    }

    #[test]
    fn duration_predicates_compare_numbers_without_varchar_coercion() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration >= 1ms }"#).unwrap();
        let empty = BTreeMap::new();
        let sql =
            trace_scan_sql(windowed(&empty, Some(&selector), None, None, 1), None).expect("sql");
        assert!(sql.contains("CAST((COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano) AS BIGINT) >= 1000000"));
        assert!(!sql.contains("CAST(COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano AS VARCHAR)"));
    }

    #[test]
    fn numeric_span_predicates_compare_numbers_without_varchar_coercion() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.http.status_code >= 500 }"#)
                .unwrap();
        let empty = BTreeMap::new();
        let sql =
            trace_scan_sql(windowed(&empty, Some(&selector), None, None, 1), None).expect("sql");
        assert!(
            sql.contains("TRY_CAST(COALESCE(json_extract_string")
                || sql.contains("TRY_CAST(json_extract_string"),
            "numeric span predicates must TRY_CAST extracted JSON strings, got: {sql}"
        );
        assert!(sql.contains(">= 500"));
        assert!(!sql.contains("AS VARCHAR) >= 500"));
    }

    #[test]
    fn persisted_span_status_code_predicates_use_the_post_filter_source() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.status_code >= 2 }"#).unwrap();
        let empty = BTreeMap::new();
        let sql =
            trace_scan_sql(windowed(&empty, Some(&selector), None, None, 1), None).expect("sql");

        assert!(sql.contains("WHEN 'STATUS_CODE_ERROR' THEN 2"));
        assert!(sql.contains("WHEN 'error' THEN 2"));
        assert!(sql.contains(">= 2"));
    }
}
