//! Loki log scan SQL recipes (one clock: `timestamp` only).

use crate::compat::backends::label_match::{LabelMatcher, MatcherOp};
use crate::sql::literal::sql_string_literal;
use crate::sql::trusted::{approved_query, TrustedSql};
use crate::storage::schema::variant::prefer_attr_varchar;

/// Default lookback when Loki clients omit one or both of start/end.
/// Matches Tempo: long enough for Phase 3 fixture timestamps (~2023) under CI
/// "now", while keeping every lake scan inside a finite [`crate::sql::QueryWindow`].
pub const LOKI_DEFAULT_LOOKBACK_NS: i64 = 10 * 365 * 24 * 60 * 60 * 1_000_000_000;

/// Product log hot columns from `docs/promotion/logs-query-hot-attrs.yaml`.
/// `(stream_or_matcher_label, sql_column, bag_column, otel_key)`.
pub const LOG_HOT_PROMOTIONS: &[(&str, &str, &str, &str)] = &[
    (
        "service_name",
        "service_name",
        "resource_attributes",
        "service.name",
    ),
    (
        "deployment_environment",
        "deployment_environment",
        "resource_attributes",
        "deployment.environment",
    ),
    ("logger_name", "logger_name", "attributes", "logger_name"),
    (
        "session_attr_id",
        "session_attr_id",
        "attributes",
        "sp.session.id",
    ),
    ("user_id", "user_id", "attributes", "sp.user.id"),
];

/// Resolve exclusive Loki `[start, end)` for lake scans.
///
/// Grafana label/values discovery often omits bounds; default a finite lookback
/// so AC2 still holds. Explicit zero-width windows stay empty at `scan`.
pub fn resolve_loki_scan_window(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
) -> Result<(i64, i64), String> {
    match (start_ns, end_ns) {
        (Some(start), Some(end)) => Ok((start, end)),
        (None, None) => {
            let end = chrono::Utc::now()
                .timestamp_nanos_opt()
                .ok_or_else(|| "current time out of range".to_string())?;
            let start = end.saturating_sub(LOKI_DEFAULT_LOOKBACK_NS);
            Ok((start, end))
        }
        (None, Some(end)) => {
            let start = end.saturating_sub(LOKI_DEFAULT_LOOKBACK_NS);
            Ok((start, end))
        }
        (Some(start), None) => {
            let end = start.saturating_add(LOKI_DEFAULT_LOOKBACK_NS);
            Ok((start, end))
        }
    }
}

/// Equality matchers that map to product-hot promotions → column-prefer predicates.
pub fn matcher_pushdown_clauses(matchers: &[LabelMatcher]) -> Vec<String> {
    let mut parts = Vec::new();
    for m in matchers {
        if m.op != MatcherOp::Eq {
            continue;
        }
        let Some(&(label, col, bag, key)) = LOG_HOT_PROMOTIONS
            .iter()
            .find(|(matcher, _, _, _)| *matcher == m.name)
        else {
            continue;
        };
        let _ = label;
        parts.push(format!(
            "({}) = {}",
            prefer_attr_varchar(Some(col), bag, key),
            sql_string_literal(&m.value)
        ));
    }
    parts
}

pub fn promoted_select_sql() -> String {
    LOG_HOT_PROMOTIONS
        .iter()
        .map(|(_, col, _, _)| (*col).to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build the `AND …` window fragment (identity predicates + OTLP ns bounds).
pub fn sql_window(
    start_ns: i64,
    end_ns: i64,
    identity: impl IntoIterator<Item = String>,
) -> Result<String, String> {
    let mut clauses = Vec::new();
    crate::sql::push_otlp_ns_window_predicates(&mut clauses, start_ns, end_ns, identity)?;
    Ok(format!(" AND {}", clauses.join(" AND ")))
}

pub fn scan_sql(window: &str, promoted: &str, cap: usize) -> String {
    format!(
        "SELECT CAST(epoch_ns(timestamp) AS BIGINT) AS timestamp_ns, body, \
         CAST(attributes AS JSON) AS attributes, \
         CAST(resource_attributes AS JSON) AS resource_attributes, \
         {promoted} \
         FROM logs WHERE 1=1{window} ORDER BY timestamp ASC LIMIT {}",
        cap.saturating_add(1)
    )
}

/// Compile and approve a bounded Loki log scan for trusted execution.
pub(crate) fn scan(
    start_ns: i64,
    end_ns: i64,
    matchers: &[LabelMatcher],
    cap: usize,
) -> Result<TrustedSql, String> {
    let window_sql = sql_window(start_ns, end_ns, matcher_pushdown_clauses(matchers))?;
    let sql = scan_sql(&window_sql, &promoted_select_sql(), cap);
    approved_query(sql).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loki_scan_mints_trusted_sql_with_matcher_and_window() {
        let start = 1_700_000_000_000_000_000i64;
        let end = start + 60_000_000_000;
        let matchers = [LabelMatcher {
            name: "service_name".into(),
            op: MatcherOp::Eq,
            value: "O'Brien".into(),
        }];
        let sql = scan(start, end, &matchers, 100)
            .expect("trusted scan")
            .as_str()
            .to_string();
        assert!(sql.contains("FROM logs"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));
        assert!(sql.contains("ORDER BY timestamp ASC"));
        assert!(sql.contains("'O''Brien'"));
        assert!(sql.contains("service_name") || sql.contains("service.name"));
        assert!(!sql.contains("make_timestamp_ns(epoch_ns(timestamp))"));
    }
}
