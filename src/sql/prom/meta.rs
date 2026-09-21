//! Series metadata SQL (identity + labels once per series_id).

use crate::sql::literal::sql_string_literal;
use crate::sql::prom::day_range::PostingsDayRange;
use crate::sql::prom::resolve::sql_series_id_list;
use crate::sql::schema::qualified_table_name;

/// How far `series_meta_sql` may look when filling the series-id cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesMetaDayScope {
    /// Open / near-open days only — partition-prunes for live Grafana series.
    Recent,
    /// Miss path: still bound to the Prom query's timestamp window (not
    /// full retention) so churned ids cannot re-open the whole lake.
    QueryWindow,
}

/// Series identity + labels once per `series_id` (Greptime series metadata,
/// not VARIANT extracts on every sample row).
///
/// Prefer:
/// 1. `Recent` + optional `metric_name` (sort key) for the hot path
/// 2. `QueryWindow` only for ids still missing after (1)
pub fn series_meta_sql(
    catalog: &str,
    series_ids: &[u64],
    scope: SeriesMetaDayScope,
    metric_name: Option<&str>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> String {
    let series = qualified_table_name(
        catalog,
        crate::sql::schema::table_spec("metric_series").unwrap(),
    );
    let ids = sql_series_id_list(series_ids);
    let mut preds = vec![format!("s.series_id IN ({ids})")];
    if let Some(name) = metric_name {
        preds.push(format!("s.metric_name = {}", sql_string_literal(name)));
    }
    let hint = match scope {
        SeriesMetaDayScope::Recent => {
            // Two calendar days covers open-day + lag without multi-week partition fanout.
            preds.push("s.timestamp >= (CURRENT_TIMESTAMP - INTERVAL '2' DAY)".to_string());
            "thelake_series_meta_recent"
        }
        SeriesMetaDayScope::QueryWindow => {
            let day_pred = PostingsDayRange::from_ms(start_ms, end_ms).sql_predicate("s.");
            if !day_pred.is_empty() {
                preds.push(day_pred);
            }
            "thelake_series_meta_all"
        }
    };
    let where_sql = preds.join(" AND ");
    format!(
        "SELECT /* {hint} */ s.series_id, \
         s.metric_name, \
         s.description, \
         s.unit, \
         s.metric_type, \
         CAST(s.labels AS JSON) AS labels_json \
         FROM {series} s \
         WHERE {where_sql} \
         QUALIFY row_number() OVER (PARTITION BY s.series_id ORDER BY s.timestamp DESC) = 1"
    )
}

pub fn metrics_metadata_scan_sql(catalog: &str, time: &str) -> String {
    let series = qualified_table_name(
        catalog,
        crate::sql::schema::table_spec("metric_series").unwrap(),
    );
    let samples = qualified_table_name(
        catalog,
        crate::sql::schema::table_spec("metric_samples").unwrap(),
    );
    let hist = qualified_table_name(
        catalog,
        crate::sql::schema::table_spec("metric_hist_samples").unwrap(),
    );
    let sample_time = time.replace("timestamp", "sm.timestamp");
    let hist_time = time.replace("timestamp", "hs.timestamp");
    format!(
        "SELECT metric_name, \
         any_value(description) AS description, \
         any_value(unit) AS unit, \
         any_value(metric_type) AS metric_type, \
         any_value(aggregation_temporality) AS aggregation_temporality, \
         any_value(is_monotonic) AS is_monotonic \
         FROM {series} \
         WHERE EXISTS (SELECT 1 FROM {samples} sm WHERE sm.series_id = {series}.series_id{sample_time}) \
            OR EXISTS (SELECT 1 FROM {hist} hs WHERE hs.series_id = {series}.series_id{hist_time}) \
         GROUP BY metric_name \
         ORDER BY metric_name"
    )
}

pub fn active_telemetry_promotions_sql(alias: &str) -> String {
    format!(
        "SELECT spec_id, manifest_json FROM {alias}.promotion_specs \
         WHERE status = 'active' AND target_kind = 'telemetry_columns'"
    )
}

pub fn variant_identity_keys_sql(alias: &str) -> String {
    format!(
        "SELECT DISTINCT vs.variant_path \
         FROM __ducklake_metadata_{alias}.ducklake_file_variant_stats vs \
         WHERE vs.variant_path IS NOT NULL \
         LIMIT 2048"
    )
}

pub const METRICS_PROBE_SQL: &str = "SELECT 1 FROM metrics LIMIT 1";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_scope_binds_timestamp_not_date_cast() {
        let sql = series_meta_sql(
            "softprobe",
            &[1, 2],
            SeriesMetaDayScope::Recent,
            Some("layout_wide"),
            None,
            None,
        );
        assert!(sql.contains("CAST(s.labels AS JSON)"), "{sql}");
        assert!(sql.contains("s.timestamp >="), "{sql}");
        assert!(!sql.contains("CAST(s.timestamp AS DATE)"), "{sql}");
        assert!(!sql.contains("record_date"), "{sql}");
    }

    #[test]
    fn query_window_scope_uses_day_timestamp_bounds() {
        let sql = series_meta_sql(
            "softprobe",
            &[42],
            SeriesMetaDayScope::QueryWindow,
            None,
            Some(1_700_000_000_000),
            Some(1_700_086_400_000),
        );
        assert!(sql.contains("thelake_series_meta_all"), "{sql}");
        assert!(sql.contains("s.timestamp >="), "{sql}");
        assert!(!sql.contains("record_date"), "{sql}");
    }
}
