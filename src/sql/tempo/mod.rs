//! Tempo / TraceQL lake scan SQL recipes (one clock: `timestamp` only).

use crate::compat::tempo::traceql::{
    is_duration_field, parse_duration_ns, TraceField, TracePredicate, TraceSelector,
};
use crate::sql::literal::sql_string_literal;
use crate::sql::query_window_from_exclusive_ns;
use crate::sql::trusted::{approved_query, TrustedSql};
use crate::storage::schema::attribute_map::{attribute_map_varchar, prefer_attr_varchar};
use std::collections::{BTreeMap, HashSet};

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
/// Long enough to cover Tempo query fixture timestamps (~2023) under CI "now".
/// Still a finite [`QueryWindow`] — never an unbounded lake scan.
const TEMPO_DEFAULT_LOOKBACK_NS: i64 = 10 * 365 * 24 * 60 * 60 * 1_000_000_000;
/// Forward clock-skew margin when both `start` and `end` are omitted so newly ingested spans
/// from slightly ahead browser or backend clocks remain visible on `/api/v2/traces/{id}` and tag discovery.
const TEMPO_CLOCK_SKEW_NS: i64 = 5 * 60 * 1_000_000_000;
const TRACE_SCAN_SQL: &str = include_str!("trace_scan.sql");
const TRACE_SEARCH_SUMMARY_SCAN_SQL: &str = include_str!("trace_search_summary_scan.sql");
const TRACE_UNFILTERED_SUMMARY_SCAN_SQL: &str = include_str!("trace_unfiltered_summary_scan.sql");
const TRACE_LOOKUP_SCAN_SQL: &str = include_str!("trace_lookup_scan.sql");
const TRACE_ID_FILTER_SQL: &str = include_str!("trace_id_filter.sql");
const EXCLUDE_RECORDING_FILTER_SQL: &str = include_str!("exclude_recording_filter.sql");
const TAG_NAMES_MAP_KEYS_SQL: &str = include_str!("tag_names_map_keys.sql");
const TAG_EVENT_LINK_ROWS_SQL: &str = include_str!("tag_event_link_rows.sql");
const TAG_VALUES_SQL: &str = include_str!("tag_values.sql");

const FILTER_BASE_CANDIDATE_COLUMNS: &[&str] = &[
    "message_type",
    "span_kind",
    "app_id",
    "agent_id",
    "agent_name",
    "session_id",
    "status_code",
    "status_message",
    "observation_type",
    "model_name",
    "model_provider",
    "user_id",
    "session_attr_id",
    "service_name",
    "attributes",
    "resource_attributes",
    "instrumentation_scope",
];

const TRACE_START_NS_EXPR: &str = "CAST(epoch_ns(timestamp) AS BIGINT)";
const TRACE_END_NS_EXPR: &str =
    "COALESCE(CAST(epoch_ns(end_timestamp) AS BIGINT), CAST(epoch_ns(timestamp) AS BIGINT))";

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
/// - Both omitted → `[now - lookback, now + clock_skew)`
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
            let now = chrono::Utc::now()
                .timestamp_nanos_opt()
                .ok_or_else(|| "current time out of range".to_string())?;
            let end = now.saturating_add(TEMPO_CLOCK_SKEW_NS);
            let start = now.saturating_sub(TEMPO_DEFAULT_LOOKBACK_NS);
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

/// Build and approve the bounded raw trace scan. Protocol adapters never construct SQL.
///
/// Emits `timestamp` lower/upper via [`crate::sql::QueryWindow`]
/// (exclusive end → inclusive). Omitted bounds get a finite default lookback
/// so every lake scan still has a QueryWindow; never an open-ended scan.
pub(crate) fn trace_scan_sql(
    params: TraceScanParams<'_>,
    trace_id: Option<&str>,
) -> Result<TrustedSql, String> {
    let sql = compile_trace_scan_sql(params, trace_id)?;
    approved_query(sql).map_err(|error| error.to_string())
}

pub(crate) fn tag_names_map_keys_sql(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    limit: usize,
) -> Result<TrustedSql, String> {
    let (start_ns, end_ns) = resolve_tempo_scan_window(start_ns, end_ns)?;
    let window = query_window_from_exclusive_ns(start_ns, end_ns)?;
    let limit = limit.max(1);
    let sql = window
        .scan_with_timestamp_filter("", |bound| {
            TAG_NAMES_MAP_KEYS_SQL
                .replace("{{timestamp_filter}}", bound)
                .replace("{{limit}}", &limit.to_string())
        })
        .into_sql();
    approved_query(sql).map_err(|error| error.to_string())
}

pub(crate) fn tag_event_link_rows_sql(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    limit: usize,
) -> Result<TrustedSql, String> {
    let (start_ns, end_ns) = resolve_tempo_scan_window(start_ns, end_ns)?;
    let window = query_window_from_exclusive_ns(start_ns, end_ns)?;
    let limit = limit.max(1);
    let sql = window
        .scan_with_timestamp_filter("", |bound| {
            TAG_EVENT_LINK_ROWS_SQL
                .replace("{{timestamp_filter}}", bound)
                .replace("{{limit}}", &limit.to_string())
        })
        .into_sql();
    approved_query(sql).map_err(|error| error.to_string())
}

pub(crate) fn tag_values_sql(
    tag: &str,
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    limit: usize,
) -> Result<TrustedSql, String> {
    let (start_ns, end_ns) = resolve_tempo_scan_window(start_ns, end_ns)?;
    let window = query_window_from_exclusive_ns(start_ns, end_ns)?;
    let limit = limit.max(1);
    let tag_expr = tag_value_sql(tag);
    let sql = window
        .scan_with_timestamp_filter("", |bound| {
            TAG_VALUES_SQL
                .replace("{{timestamp_filter}}", bound)
                .replace("{{tag_expr}}", &tag_expr)
                .replace("{{limit}}", &limit.to_string())
        })
        .into_sql();
    approved_query(sql).map_err(|error| error.to_string())
}

fn compile_trace_scan_sql(
    params: TraceScanParams<'_>,
    trace_id: Option<&str>,
) -> Result<String, String> {
    let (start_ns, end_ns) = resolve_tempo_scan_window(params.start_ns, params.end_ns)?;
    let window = query_window_from_exclusive_ns(start_ns, end_ns)?;

    let is_plain_trace_lookup = trace_id.is_some()
        && params.tags.is_empty()
        && params.selector.is_none()
        && params.min_duration_ns.is_none()
        && params.max_duration_ns.is_none();

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
    let filter_extra_columns = filter_extra_columns_sql(&row_predicate);

    let mut duration_predicates = Vec::new();
    if let Some(min) = params.min_duration_ns {
        duration_predicates.push(format!(
            "MAX({TRACE_END_NS_EXPR}) - MIN({TRACE_START_NS_EXPR}) >= {min}"
        ));
    }
    if let Some(max) = params.max_duration_ns {
        duration_predicates.push(format!(
            "MAX({TRACE_END_NS_EXPR}) - MIN({TRACE_START_NS_EXPR}) <= {max}"
        ));
    }
    let duration_predicate = if duration_predicates.is_empty() {
        "TRUE".to_string()
    } else {
        duration_predicates.join(" AND ")
    };
    let cap = trace_scan_cap(params.limit);
    let trace_limit = if trace_id.is_some() {
        cap
    } else {
        params.limit.max(1)
    };
    let has_row_filter =
        !params.tags.is_empty() || !matches!(params.selector, None | Some(TraceSelector::All));
    let can_use_summary_projection = trace_id.is_none()
        && params.tags.is_empty()
        && !params.selector.is_some_and(selector_uses_instrumentation);
    let template = if is_plain_trace_lookup {
        TRACE_LOOKUP_SCAN_SQL
    } else if !has_row_filter && can_use_summary_projection {
        TRACE_UNFILTERED_SUMMARY_SCAN_SQL
    } else if can_use_summary_projection {
        TRACE_SEARCH_SUMMARY_SCAN_SQL
    } else {
        TRACE_SCAN_SQL
    };
    let trace_id = trace_id.map(str::to_owned);
    let default_observation_filter = if trace_id.is_none() && !query_mentions_recording(&params) {
        EXCLUDE_RECORDING_FILTER_SQL
    } else {
        ""
    };

    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            let trace_id_prefix = match trace_id.as_deref() {
                Some(id) => TRACE_ID_FILTER_SQL.replace("{{trace_id}}", &sql_string_literal(id)),
                None => String::new(),
            };
            template
                .replace("{{trace_id_prefix}}", &trace_id_prefix)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{default_observation_filter}}", default_observation_filter)
                .replace("{{filter_extra_columns}}", &filter_extra_columns)
                .replace("{{row_predicate}}", &row_predicate)
                .replace("{{duration_predicate}}", &duration_predicate)
                .replace("{{trace_limit}}", &trace_limit.to_string())
                .replace("{{cap}}", &cap.to_string())
        })
        .into_sql())
}

fn selector_uses_instrumentation(selector: &TraceSelector) -> bool {
    match selector {
        TraceSelector::All => false,
        TraceSelector::And(left, right) | TraceSelector::Or(left, right) => {
            selector_uses_instrumentation(left) || selector_uses_instrumentation(right)
        }
        TraceSelector::Predicate(predicate) => {
            matches!(predicate_field(predicate), TraceField::Instrumentation(_))
        }
    }
}

fn predicate_field(predicate: &TracePredicate) -> &TraceField {
    match predicate {
        TracePredicate::Eq(f, _)
        | TracePredicate::NotEq(f, _)
        | TracePredicate::Regex(f, _)
        | TracePredicate::NotRegex(f, _)
        | TracePredicate::Greater(f, _)
        | TracePredicate::GreaterOrEqual(f, _)
        | TracePredicate::Less(f, _)
        | TracePredicate::LessOrEqual(f, _) => f,
    }
}

fn filter_extra_columns_sql(row_predicate: &str) -> String {
    let unquoted = strip_single_quoted_literals(row_predicate);
    let referenced: HashSet<&str> = unquoted
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|tok| !tok.is_empty())
        .collect();
    let mut out = String::new();
    for &col in FILTER_BASE_CANDIDATE_COLUMNS {
        if referenced.contains(col) {
            out.push_str(",\n        ");
            out.push_str(col);
        }
    }
    out
}

fn strip_single_quoted_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let mut in_quote = false;
    while i < bytes.len() {
        if in_quote {
            if bytes[i] == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            in_quote = true;
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8(out).expect("valid UTF-8 SQL fragment")
}

fn query_mentions_recording(params: &TraceScanParams<'_>) -> bool {
    if params
        .tags
        .iter()
        .any(|(k, v)| k.contains("recording") || v.contains("recording"))
    {
        return true;
    }
    params.selector.is_some_and(selector_mentions_recording)
}

fn selector_mentions_recording(selector: &TraceSelector) -> bool {
    use crate::compat::tempo::traceql::TracePredicate;
    match selector {
        TraceSelector::All => false,
        TraceSelector::And(left, right) | TraceSelector::Or(left, right) => {
            selector_mentions_recording(left) || selector_mentions_recording(right)
        }
        TraceSelector::Predicate(predicate) => {
            let value = match predicate {
                TracePredicate::Eq(_, v)
                | TracePredicate::NotEq(_, v)
                | TracePredicate::Regex(_, v)
                | TracePredicate::NotRegex(_, v)
                | TracePredicate::Greater(_, v)
                | TracePredicate::GreaterOrEqual(_, v)
                | TracePredicate::Less(_, v)
                | TracePredicate::LessOrEqual(_, v) => v.as_str(),
            };
            value.contains("recording")
        }
    }
}

pub fn trace_scan_cap(limit: usize) -> usize {
    limit.saturating_mul(100).clamp(10_000, 100_000)
}

/// A scan reaching the cap is incomplete/ambiguous because the bounded query
/// cannot distinguish exactly-cap rows from rows that continue beyond it.
pub fn scan_reached_cap(row_count: usize, cap: usize) -> bool {
    row_count >= cap
}

fn span_only_attribute_value(key: &str) -> String {
    if let Some(expr) = span_promoted_attr_expr(key) {
        return expr;
    }
    attribute_map_varchar("attributes", key)
}

fn span_attribute_value(key: &str) -> String {
    if let Some(expr) = promoted_attr_expr(key) {
        return expr;
    }
    format!(
        "COALESCE({}, {})",
        attribute_map_varchar("attributes", key),
        attribute_map_varchar("resource_attributes", key)
    )
}

fn span_promoted_attr_expr(key: &str) -> Option<String> {
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
        "sp.session.id" | "session.id" => Some(format!(
            "COALESCE(NULLIF(session_id, ''), session_attr_id, {}, {})",
            prefer_attr_varchar(None, "attributes", "sp.session.id"),
            prefer_attr_varchar(None, "attributes", "session.id")
        )),
        _ => None,
    }
}

/// Prefer product-hot / first-class columns from traces-query-hot-attrs.yaml.
fn promoted_attr_expr(key: &str) -> Option<String> {
    let promo = crate::sql::llm::llm_promo();
    match key {
        "sp.observation.type" => Some(format!(
            "COALESCE({}, {}, {})",
            promo.observation_type,
            attribute_map_varchar("attributes", "sp.observation.type"),
            attribute_map_varchar("resource_attributes", "sp.observation.type")
        )),
        "gen_ai.request.model" => Some(format!(
            "COALESCE({}, {}, {})",
            promo.model_name,
            attribute_map_varchar("attributes", "gen_ai.request.model"),
            attribute_map_varchar("resource_attributes", "gen_ai.request.model")
        )),
        "gen_ai.provider.name" => Some(format!(
            "COALESCE({}, {}, {})",
            promo.model_provider,
            attribute_map_varchar("attributes", "gen_ai.provider.name"),
            attribute_map_varchar("resource_attributes", "gen_ai.provider.name")
        )),
        "sp.user.id" => Some(format!(
            "COALESCE({}, {}, {})",
            promo.user_id,
            attribute_map_varchar("attributes", "sp.user.id"),
            attribute_map_varchar("resource_attributes", "sp.user.id")
        )),
        "sp.session.id" | "session.id" => Some(format!(
            "COALESCE(NULLIF(session_id, ''), session_attr_id, {}, {}, {})",
            prefer_attr_varchar(None, "attributes", "sp.session.id"),
            prefer_attr_varchar(None, "attributes", "session.id"),
            attribute_map_varchar("resource_attributes", key)
        )),
        "sp.agent.id" => Some(format!(
            "COALESCE(NULLIF(agent_id, ''), {}, {})",
            attribute_map_varchar("attributes", "sp.agent.id"),
            attribute_map_varchar("resource_attributes", "sp.agent.id")
        )),
        "sp.agent.name" => Some(format!(
            "COALESCE(NULLIF(agent_name, ''), {}, {})",
            attribute_map_varchar("attributes", "sp.agent.name"),
            attribute_map_varchar("resource_attributes", "sp.agent.name")
        )),
        "service.name" => Some(format!(
            "COALESCE(service_name, {}, {}, app_id)",
            attribute_map_varchar("attributes", key),
            attribute_map_varchar("resource_attributes", key)
        )),
        _ => None,
    }
}

fn resource_attribute_value(key: &str) -> String {
    match key {
        "service.name" => format!(
            "COALESCE(service_name, {}, app_id)",
            attribute_map_varchar("resource_attributes", "service.name")
        ),
        "sp.agent.id" => format!(
            "COALESCE(NULLIF(agent_id, ''), {})",
            attribute_map_varchar("resource_attributes", "sp.agent.id")
        ),
        "sp.agent.name" => format!(
            "COALESCE(NULLIF(agent_name, ''), {})",
            attribute_map_varchar("resource_attributes", "sp.agent.name")
        ),
        _ => attribute_map_varchar("resource_attributes", key),
    }
}

fn span_status_code_sql() -> String {
    format!(
        "COALESCE({}, {}, NULLIF(status_code, ''))",
        attribute_map_varchar("attributes", "status_code"),
        attribute_map_varchar("attributes", "status")
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
        TraceSelector::All => "TRUE".to_string(),
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
        TraceField::Span(key) | TraceField::Unscoped(key)
            if key == "status_code" || key == "status" =>
        {
            span_status_code_sql()
        }
        TraceField::Span(key) => span_only_attribute_value(key),
        TraceField::Unscoped(key) => span_attribute_value(key),
        TraceField::Resource(key) => resource_attribute_value(key),
        TraceField::Instrumentation(key) => attribute_map_varchar("instrumentation_scope", key),
        TraceField::Intrinsic(key) => match key.as_str() {
            "name" | "span:name" | "span.name" => "message_type".into(),
            "kind" | "span:kind" | "span.kind" => "span_kind".into(),
            "status" | "span:status" | "span.status" => "NULLIF(status_code, '')".into(),
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
    let actual_numeric = if is_status {
        persisted_status_code_numeric_sql(&actual)
    } else if numeric {
        format!("TRY_CAST({actual} AS BIGINT)")
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
        let sql = sql.as_str();
        assert!(sql.contains("COALESCE(observation_type,"));
        let obs = sql.find("observation_type").expect("observation_type");
        let bag = sql
            .find("attributes['sp.observation.type']")
            .expect("bag fallback");
        assert!(obs < bag, "promoted observation_type must lead bag access");
        assert!(sql.contains("COALESCE(service_name,"));
        use crate::sql::assert_sql_has_otlp_time_predicates;
        assert_sql_has_otlp_time_predicates(sql);
    }

    #[test]
    fn trace_scan_defaults_lookback_when_bounds_omitted_or_partial() {
        use crate::sql::assert_sql_has_otlp_time_predicates;
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
        assert_sql_has_otlp_time_predicates(omitted.as_str());

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
        assert_sql_has_otlp_time_predicates(end_only.as_str());

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
        assert_sql_has_otlp_time_predicates(start_only.as_str());
    }

    #[test]
    fn tempo_trace_scan_inventory_emits_timestamp_bound() {
        use crate::sql::assert_sql_has_otlp_time_predicates;
        let empty = BTreeMap::new();
        let sql = trace_scan_sql(windowed(&empty, None, None, None, 5), Some("abc")).expect("sql");
        let sql = sql.as_str();
        assert_sql_has_otlp_time_predicates(sql);
        assert!(!sql.contains("record_date"));
        let id = sql.find("trace_id = 'abc'").unwrap();
        let ts = sql.find("timestamp >=").unwrap();
        assert!(id < ts, "identity before timestamp: {sql}");
    }

    #[test]
    fn trace_scan_is_tenant_neutral_and_bounded() {
        let params = parse_tempo_search_params(
            &[("limit".into(), "5".into())],
            &crate::compat::workspace::QueryLimits::default(),
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
        let sql = sql.as_str();
        assert!(sql.contains("FROM traces"));
        assert!(sql.contains("trace_id = 'trace-1'"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(!sql.contains("workspace_id ="));
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
        let sql = sql.as_str();
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
        let sql = sql.as_str();
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
        let sql = sql.as_str();
        assert!(
            !sql.contains("json_extract_string"),
            "Tempo SQL must use native MAP indexing rather than JSON serialization: {sql}"
        );
        assert!(
            sql.contains("attributes['http.status_code']"),
            "numeric span predicates must index MAP attributes directly, got: {sql}"
        );
        assert!(sql.contains("TRY_CAST("));
        assert!(sql.contains(">= 500"));
        assert!(!sql.contains("AS VARCHAR) >= 500"));
    }

    #[test]
    fn pure_scalar_intrinsics_prune_all_map_columns_from_qualification_and_summary_omits_heavy_fields(
    ) {
        let empty = BTreeMap::new();
        for query in [
            r#"{}"#,
            r#"{ status = error }"#,
            r#"{ span:status = ok }"#,
            r#"{ duration >= 100ms }"#,
            r#"{ name = "GET /checkout" }"#,
            r#"{ kind = "SPAN_KIND_SERVER" }"#,
            r#"{ statusMessage = "timeout" }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            let sql = trace_scan_sql(
                windowed(&empty, Some(&selector), Some(100_000_000), None, 20),
                None,
            )
            .expect("sql");
            let sql = sql.as_str();
            assert!(
                !sql.contains("json_extract_string"),
                "no JSON cast allowed for {query}: {sql}"
            );
            let outer_pos = sql
                .find("\nSELECT\n")
                .expect("outer SELECT boundary after WITH block");
            let qualification_sql = &sql[..outer_pos];
            let outer_select = &sql[outer_pos..];
            assert!(
                !qualification_sql.contains("attributes"),
                "qualification CTE must not project or read attributes/resource_attributes for scalar query {query}: {qualification_sql}"
            );
            assert!(
                !qualification_sql.contains("instrumentation_scope"),
                "qualification CTE must not project instrumentation_scope for scalar query {query}: {qualification_sql}"
            );
            assert!(
                !outer_select.contains("events"),
                "summary search must omit events for {query}: {outer_select}"
            );
            assert!(
                !outer_select.contains("links"),
                "summary search must omit links for {query}: {outer_select}"
            );
            assert!(
                !outer_select.contains("instrumentation_scope"),
                "summary search must omit instrumentation_scope for {query}: {outer_select}"
            );
        }
    }

    #[test]
    fn scoped_selectors_project_only_referenced_map_columns_and_lookup_preserves_full_payload() {
        let empty = BTreeMap::new();

        let span_sel =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.http.method = "POST" }"#)
                .unwrap();
        let span_sql =
            trace_scan_sql(windowed(&empty, Some(&span_sel), None, None, 10), None).expect("sql");
        let span_qual =
            &span_sql.as_str()[..span_sql.as_str().find("qualified_traces AS").unwrap()];
        assert!(span_qual.contains("attributes"));
        assert!(!span_qual.contains("resource_attributes"));
        assert!(!span_qual.contains("instrumentation_scope"));

        let res_sel = crate::compat::tempo::traceql::parse_traceql(
            r#"{ resource.deployment.environment = "staging" }"#,
        )
        .unwrap();
        let res_sql =
            trace_scan_sql(windowed(&empty, Some(&res_sel), None, None, 10), None).expect("sql");
        let res_qual = &res_sql.as_str()[..res_sql.as_str().find("qualified_traces AS").unwrap()];
        assert!(res_qual.contains("resource_attributes"));
        assert!(
            !res_qual.contains("\n        attributes,"),
            "resource selector must not project span attributes in filter_base: {res_qual}"
        );
        assert!(!res_qual.contains("instrumentation_scope"));

        let inst_sel = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.name = "otel-rust" }"#,
        )
        .unwrap();
        let inst_sql =
            trace_scan_sql(windowed(&empty, Some(&inst_sel), None, None, 10), None).expect("sql");
        assert!(inst_sql
            .as_str()
            .contains("CAST(instrumentation_scope AS JSON)"));

        let tags = BTreeMap::from([(String::from("exception.type"), String::from("Timeout"))]);
        let tag_sql = trace_scan_sql(windowed(&tags, None, None, None, 10), None).expect("sql");
        assert!(tag_sql.as_str().contains("events"));
        assert!(tag_sql.as_str().contains("CAST(links AS JSON)"));

        let lookup_sql = trace_scan_sql(
            windowed(&empty, None, None, None, 10),
            Some("0123456789abcdef0123456789abcdef"),
        )
        .expect("lookup sql");
        let lookup_sql = lookup_sql.as_str();
        assert!(!lookup_sql.contains("qualified_traces"));
        assert!(lookup_sql.contains("events"));
        assert!(lookup_sql.contains("CAST(links AS JSON)"));
        assert!(lookup_sql.contains("CAST(instrumentation_scope AS JSON)"));
    }

    #[test]
    fn tag_discovery_sql_applies_explicit_bounds_and_defaults_when_omitted() {
        use crate::sql::assert_sql_has_otlp_time_predicates;

        let bounded_names =
            tag_names_map_keys_sql(Some(START_NS), Some(END_NS), 50).expect("bounded tag names");
        assert_sql_has_otlp_time_predicates(bounded_names.as_str());
        assert!(bounded_names.as_str().contains("2023-11-14"));

        let bounded_events = tag_event_link_rows_sql(Some(START_NS), Some(END_NS), 50)
            .expect("bounded event/link tags");
        assert_sql_has_otlp_time_predicates(bounded_events.as_str());
        assert!(bounded_events.as_str().contains("2023-11-14"));

        let bounded_vals = tag_values_sql("http.method", Some(START_NS), Some(END_NS), 50)
            .expect("bounded tag values");
        assert_sql_has_otlp_time_predicates(bounded_vals.as_str());
        assert!(!bounded_vals.as_str().contains("json_extract_string"));
        assert!(bounded_vals.as_str().contains("attributes['http.method']"));

        let default_names = tag_names_map_keys_sql(None, None, 50).expect("default tag names");
        assert_sql_has_otlp_time_predicates(default_names.as_str());

        let inverted = tag_names_map_keys_sql(Some(END_NS), Some(START_NS), 50).unwrap_err();
        assert!(inverted.contains("`start` must be < `end`"));
    }

    #[test]
    fn persisted_span_status_code_predicates_use_the_post_filter_source() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.status_code >= 2 }"#).unwrap();
        let empty = BTreeMap::new();
        let sql =
            trace_scan_sql(windowed(&empty, Some(&selector), None, None, 1), None).expect("sql");
        let sql = sql.as_str();

        assert!(sql.contains("WHEN 'STATUS_CODE_ERROR' THEN 2"));
        assert!(sql.contains("WHEN 'error' THEN 2"));
        assert!(sql.contains(">= 2"));
    }

    #[test]
    fn promoted_attributes_escaped_quotes_and_instrumentation_comparators_prune_cleanly() {
        let empty = BTreeMap::new();

        let quoted_sel = crate::compat::tempo::traceql::parse_traceql(
            r#"{ name = "say 'resource_attributes' and 'events' now" }"#,
        )
        .unwrap();
        let quoted_sql = trace_scan_sql(windowed(&empty, Some(&quoted_sel), None, None, 5), None)
            .expect("quoted sql");
        let filter_head = quoted_sql
            .as_str()
            .split("matching_traces AS")
            .next()
            .expect("filter_base section");
        assert!(!filter_head.contains("resource_attributes"));
        assert!(!filter_head.contains("events"));

        let promo_sel = crate::compat::tempo::traceql::parse_traceql(
            r#"{ span.sp.observation.type = "llm" && span.gen_ai.request.model = "gpt-4o" && span.gen_ai.provider.name = "openai" && span.sp.user.id = "u1" && span.sp.session.id = "s1" && .gen_ai.request.model = "gpt-4o" && .gen_ai.provider.name = "openai" && .sp.user.id = "u1" && .sp.agent.id = "a1" && .sp.agent.name = "bot" && resource.sp.agent.id = "a1" && resource.sp.agent.name = "bot" }"#,
        )
        .unwrap();
        let promo_sql = trace_scan_sql(windowed(&empty, Some(&promo_sel), None, None, 5), None)
            .expect("promo sql");
        assert!(promo_sql.as_str().contains("model_name"));
        assert!(promo_sql.as_str().contains("model_provider"));
        assert!(promo_sql.as_str().contains("agent_id"));
        assert!(promo_sql.as_str().contains("agent_name"));
        assert!(!promo_sql.as_str().contains("json_extract_string"));

        let inst_cmp = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.version != "0" && instrumentation.version > "1" && instrumentation.version < "9" }"#,
        )
        .unwrap();
        let inst_sql = trace_scan_sql(windowed(&empty, Some(&inst_cmp), None, None, 5), None)
            .expect("inst sql");
        let inst_head = inst_sql
            .as_str()
            .split("matching_traces AS")
            .next()
            .expect("filter_base section");
        assert!(inst_head.contains("instrumentation_scope"));
    }
}
