//! `metric_collapse_job_1h` planner (§6.6 / §7.2 / §9.1).
//!
//! SQL recipes live in [`crate::sql::compaction`].

use crate::compat::backends::grain::RAW_RANGE_MS;
use promql_parser::parser::token::T_SUM;
use promql_parser::parser::{AggregateExpr, Expr, LabelModifier};

pub use crate::sql::compaction::{
    collapse_job_1h_for_day_sql, collapse_job_1h_from_raw_for_day_sql,
    collapse_job_1h_from_raw_pending_days_sql, collapse_job_1h_from_raw_sql,
    collapse_job_1h_pending_days_sql, collapse_job_1h_sql,
    collapse_scan_sql_optional as collapse_scan_sql, COLLAPSE_FROM_RAW_LAG,
};

/// Minimum query window to prefer collapse over wide series fetch (§9.1 step 5).
pub const COLLAPSE_MIN_RANGE_MS: i64 = RAW_RANGE_MS; // 2h

fn unwrap_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unwrap_parens(&p.expr),
        other => other,
    }
}

fn is_rate_family(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "rate" | "irate" | "increase"
    )
}

/// True when AST is `sum by (job) (rate|irate|increase (selector))` (possibly parenthesized).
pub fn is_sum_by_job_rate_shape(expr: &Expr) -> bool {
    match unwrap_parens(expr) {
        Expr::Aggregate(a) => aggregate_is_sum_by_job_rate(a),
        _ => false,
    }
}

fn aggregate_is_sum_by_job_rate(a: &AggregateExpr) -> bool {
    if a.op.id() != T_SUM {
        return false;
    }
    // Exactly `by (job)` — single grouping label (not `without`).
    let Some(LabelModifier::Include(ls)) = a.modifier.as_ref() else {
        return false;
    };
    if ls.labels.len() != 1 || ls.labels[0] != "job" {
        return false;
    }
    match unwrap_parens(a.expr.as_ref()) {
        Expr::Call(c) => {
            if !is_rate_family(c.func.name) || c.args.args.len() != 1 {
                return false;
            }
            matches!(
                unwrap_parens(c.args.args[0].as_ref()),
                Expr::MatrixSelector(_)
            )
        }
        _ => false,
    }
}

/// §9.1 step 5: use collapse table when shape matches and window ≥ 2h.
pub fn should_use_collapse(expr: &Expr, range_ms: Option<i64>) -> bool {
    let range = range_ms.unwrap_or(0);
    range >= COLLAPSE_MIN_RANGE_MS && is_sum_by_job_rate_shape(expr)
}

/// Extract metric `__name__` equality from a sum-by-job-rate AST (best effort).
pub fn collapse_metric_name(expr: &Expr) -> Option<String> {
    let Expr::Aggregate(a) = unwrap_parens(expr) else {
        return None;
    };
    let Expr::Call(c) = unwrap_parens(a.expr.as_ref()) else {
        return None;
    };
    let Expr::MatrixSelector(ms) = unwrap_parens(c.args.args[0].as_ref()) else {
        return None;
    };
    for m in ms.vs.matchers.matchers.iter() {
        if m.name == "__name__" || m.name == promql_parser::label::METRIC_NAME {
            return Some(m.value.clone());
        }
    }
    ms.vs.name.clone()
}

/// True when SQL is the collapse Prom path (AC-Q5 / AC-W3 EXPLAIN shape).
pub fn sql_is_collapse_prom_path(sql: &str) -> bool {
    sql.contains("metric_collapse_job_1h")
        && sql.contains("metric_name")
        && !sql.contains("to_timestamp(")
        && !sql.contains("FROM union_metrics")
        && !sql.contains("metric_samples sm")
}

/// Row fetch budget for collapse scans (hourly grain × series).
///
/// Raw/5m sample paths use `max_series * 10` as a scan_cap. Collapse stores one
/// row per `(job, hour)`, so a honest 90d × J=50 window is ~108k rows — above
/// that cap — and must not fail AC-W3 with an empty/error result. Cap by
/// `max_series` on **parsed series count**, not hourly row count.
pub fn collapse_fetch_limit(
    max_series: usize,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> usize {
    const HOUR_MS: u64 = 3_600_000;
    let range_ms = match (start_ms, end_ms) {
        (Some(s), Some(e)) => (e - s).unsigned_abs(),
        _ => HOUR_MS,
    };
    // +2 hours of slack for step/lookback alignment on the closed-hour grid.
    let hours = (range_ms / HOUR_MS).saturating_add(2).max(1) as usize;
    max_series
        .saturating_add(1)
        .saturating_mul(hours)
        .max(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::promql::parse_promql;
    use chrono::NaiveDate;

    #[test]
    fn collapse_sql_groups_by_job_and_is_incremental() {
        let sql = collapse_job_1h_sql("softprobe");
        assert!(sql.contains("INSERT INTO softprobe.metric_collapse_job_1h"));
        assert!(sql.contains("metric_samples_1h"));
        assert!(sql.contains("label_name = 'job'"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
    }

    #[test]
    fn collapse_for_day_scopes_timestamp_window() {
        let day = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let sql = collapse_job_1h_for_day_sql("softprobe", Some(day));
        assert!(sql.contains("2026-08-14"));
        assert!(!sql.contains("record_date"));
        let pending = collapse_job_1h_pending_days_sql("softprobe", 4);
        assert!(pending.contains("LIMIT 4"));
        assert!(pending.contains("strftime(h.timestamp AT TIME ZONE 'UTC', '%Y-%m-%d')"));
    }

    #[test]
    fn collapse_from_raw_uses_timestamp_lag() {
        let sql = collapse_job_1h_from_raw_sql("softprobe");
        assert!(sql.contains("sm.timestamp < now() - INTERVAL '24 hours'"));
        assert!(!sql.contains("record_date"));
    }

    #[test]
    fn planner_picks_collapse_for_sum_by_job_rate() {
        let expr = parse_promql(r#"sum by (job) (rate(layout_http[5m]))"#).unwrap();
        assert!(is_sum_by_job_rate_shape(&expr));
        assert!(should_use_collapse(&expr, Some(30 * 24 * 3_600_000)));
        let sql = collapse_scan_sql(
            "softprobe",
            "layout_http",
            Some(1_700_000_000_000),
            Some(1_700_000_000_000 + 30 * 24 * 3_600_000),
            10_000,
        );
        assert!(sql.contains("metric_collapse_job_1h"));
        assert!(sql.contains("layout_http"));
        assert!(sql.contains("timestamp"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
        assert!(sql_is_collapse_prom_path(&sql));
    }

    #[test]
    fn planner_rejects_non_job_or_non_rate_shapes() {
        let other = parse_promql(r#"sum by (instance) (rate(layout_http[5m]))"#).unwrap();
        assert!(!is_sum_by_job_rate_shape(&other));
    }

    #[test]
    fn collapse_metric_name_from_selector() {
        let expr = parse_promql(r#"sum by (job) (rate(layout_http[5m]))"#).unwrap();
        assert_eq!(collapse_metric_name(&expr).as_deref(), Some("layout_http"));
    }

    #[test]
    fn collapse_fetch_limit_covers_90d_job_series() {
        let end = 1_700_000_000_000i64;
        let start = end - 90 * 86_400_000;
        let lim = collapse_fetch_limit(50, Some(start), Some(end));
        assert!(lim > 90 * 24 * 50);
    }
}
