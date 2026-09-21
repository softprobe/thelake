//! Sample / histogram scan SQL after postings resolve.

use crate::compat::backends::grain::{
    grain_table_sql, select_sample_grain, SampleGrain, FIVE_MIN_LAG_MS, ONE_HOUR_LAG_MS,
};
use crate::sql::prom::day_range::timestamptz_literal_ms;
use crate::sql::prom::resolve::sql_series_id_list;
use crate::sql::schema::{qualified_table_name, table_spec};

fn metric_table(catalog: &str, name: &str) -> String {
    qualified_table_name(catalog, table_spec(name).expect("registered metric table"))
}
use chrono::Utc;

/// Same default as [`crate::sql::prom::day_range`] discovery lookback.
const PROM_DISCOVERY_DEFAULT_LOOKBACK_MS: i64 = 10 * 365 * 86_400_000;

pub fn samples_time_predicates(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    time_column: &str,
) -> String {
    samples_time_predicates_bounded(start_ms, end_ms, time_column, true)
}

/// Like [`samples_time_predicates`], but `end_inclusive=false` emits `col < end`
/// for half-open stitch windows (downsample `[start, stitch)`, raw `[stitch, end]`).
pub fn samples_time_predicates_bounded(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    time_column: &str,
    end_inclusive: bool,
) -> String {
    time_predicates_bounded(start_ms, end_ms, "sm.", time_column, end_inclusive)
}

/// Render the same metric timestamp bounds for a caller that already owns the
/// table/column qualifier. This keeps recipes composable without textual SQL
/// replacement (for example, metadata scans use bare `timestamp`).
pub fn samples_time_predicates_for_column(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    time_column: &str,
) -> String {
    time_predicates_bounded(start_ms, end_ms, "", time_column, true)
}

fn time_predicates_bounded(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    qualifier: &str,
    time_column: &str,
    end_inclusive: bool,
) -> String {
    let (start_ms, end_ms) = match (start_ms, end_ms) {
        (None, None) => {
            let end = Utc::now().timestamp_millis();
            (
                Some(end.saturating_sub(PROM_DISCOVERY_DEFAULT_LOOKBACK_MS)),
                Some(end),
            )
        }
        other => other,
    };
    let mut parts = Vec::new();
    if let Some(start) = start_ms {
        parts.push(format!(
            "{qualifier}{time_column} >= {}",
            timestamptz_literal_ms(start)
        ));
    }
    if let Some(end) = end_ms {
        let op = if end_inclusive { "<=" } else { "<" };
        parts.push(format!(
            "{qualifier}{time_column} {op} {}",
            timestamptz_literal_ms(end)
        ));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" AND {}", parts.join(" AND "))
    }
}

/// Grafana floor is 15s. Bucket raw/hist scans to `step` so a 1h panel does not
/// materialize 1s scrape rows into PromQL eval (not a query-result cache).
const STEP_BUCKET_MIN_MS: i64 = 15_000;

fn step_bucket_interval_sql(step_ms: Option<i64>) -> Option<String> {
    let step = step_ms.filter(|s| *s >= STEP_BUCKET_MIN_MS)?;
    let secs = (step / 1000).max(1);
    Some(format!("INTERVAL '{secs} seconds'"))
}

/// Skinny sample scan after resolve (AC-Q7). No full compatibility-relation scan.
///
/// `grain` selects raw / 5m / 1h / hist (§9.1). Downsample empty tables yield empty
/// results until maintenance builds them — planner still emits the correct FROM.
#[allow(clippy::too_many_arguments)]
pub fn samples_scan_sql(
    catalog: &str,
    series_ids: &[u64],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    _label_proj: &str,
    include_fidelity: bool,
    fetch_limit: usize,
    grain: SampleGrain,
    step_ms: Option<i64>,
    hist_arrays: bool,
) -> String {
    let time = samples_time_predicates(start_ms, end_ms, grain.time_column());
    let ids = sql_series_id_list(series_ids);
    let bucket = step_bucket_interval_sql(step_ms);

    if grain.is_hist() || (include_fidelity && grain == SampleGrain::Raw) {
        return hist_or_union_scan_sql(
            catalog,
            &ids,
            &time,
            include_fidelity,
            fetch_limit,
            grain,
            start_ms,
            end_ms,
            bucket.as_deref(),
            hist_arrays,
        );
    }

    if grain.is_downsample() && !grain.is_hist() {
        return gauge_downsample_with_raw_tail(
            catalog,
            &ids,
            start_ms,
            end_ms,
            fetch_limit,
            grain,
            step_ms,
        );
    }

    let samples = grain_table_sql(catalog, grain);
    let value = grain.value_expr();
    let ts_col = grain.time_column();
    if let Some(iv) = bucket {
        if grain == SampleGrain::Raw {
            return format!(
                "SELECT sm.series_id, \
                 CAST((epoch(time_bucket({iv}, sm.{ts_col})) * 1000) AS BIGINT) AS timestamp_ms, \
                 arg_max({value}, sm.{ts_col}) AS value, \
                 NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
                 NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
                 FROM {samples} sm \
                 WHERE sm.series_id IN ({ids}){time} \
                 GROUP BY sm.series_id, time_bucket({iv}, sm.{ts_col}) \
                 LIMIT {fetch_limit}"
            );
        }
    }
    format!(
        "SELECT sm.series_id, \
         CAST((epoch(sm.{ts_col}) * 1000) AS BIGINT) AS timestamp_ms, \
         {value} AS value, \
         NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
         NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
         FROM {samples} sm \
         WHERE sm.series_id IN ({ids}){time} \
         LIMIT {fetch_limit}"
    )
}

/// Half-open stitch: downsample covers through `align_floor(now - lag, bucket)`;
/// raw starts there. Using `now - lag` for both sides left a gap of up to one
/// bucket (closed-bucket materialization ends at the floor, not at `now - lag`).
pub(crate) fn stitch_raw_start_ms(cutoff_ms: i64, bucket_ms: i64) -> i64 {
    if bucket_ms <= 0 {
        return cutoff_ms;
    }
    cutoff_ms.div_euclid(bucket_ms) * bucket_ms
}

/// Gauge FiveMin/OneHour grains: downsample for closed history + raw lag tail.
///
/// Mirrors `hist_or_union_scan_sql` (HistFiveMin / HistOneHour). Live Grafana
/// panels use `end ≈ now`, so a raw-only live path scanned the full multi-day
/// raw window and blew CPU / 100ms SLO even when `metric_samples_5m` /
/// `metric_samples_1h` were populated. Archive queries (`end` older than lag)
/// read downsample only (AC-Q2).
fn gauge_downsample_with_raw_tail(
    catalog: &str,
    ids: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    fetch_limit: usize,
    grain: SampleGrain,
    step_ms: Option<i64>,
) -> String {
    let (lag_ms, bucket_ms) = match grain {
        SampleGrain::FiveMin => (FIVE_MIN_LAG_MS, FIVE_MIN_LAG_MS),
        SampleGrain::OneHour => (ONE_HOUR_LAG_MS, ONE_HOUR_LAG_MS),
        _ => (ONE_HOUR_LAG_MS, ONE_HOUR_LAG_MS),
    };
    let bucket = step_bucket_interval_sql(step_ms);
    let raw_table = grain_table_sql(catalog, SampleGrain::Raw);
    let ds_table = grain_table_sql(catalog, grain);
    let ds_time_col = grain.time_column();
    let ds_value = grain.value_expr();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let end = end_ms.unwrap_or(now_ms);
    let start = start_ms.unwrap_or(i64::MIN);
    let cutoff = now_ms.saturating_sub(lag_ms);
    let stitch = stitch_raw_start_ms(cutoff, bucket_ms);

    let ds_select = |from_ms: i64, to_ms: i64, end_inclusive: bool| -> String {
        let ds_time =
            samples_time_predicates_bounded(Some(from_ms), Some(to_ms), ds_time_col, end_inclusive);
        format!(
            "SELECT sm.series_id, \
             CAST((epoch(sm.{ds_time_col}) * 1000) AS BIGINT) AS timestamp_ms, \
             {ds_value} AS value, \
             NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
             NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
             FROM {ds_table} sm \
             WHERE sm.series_id IN ({ids}){ds_time}"
        )
    };

    let raw_select = |from_ms: i64, to_ms: i64| -> String {
        let raw_time = samples_time_predicates(Some(from_ms), Some(to_ms), "timestamp");
        if let Some(ref iv) = bucket {
            format!(
                "SELECT sm.series_id, \
                 CAST((epoch(time_bucket({iv}, sm.timestamp)) * 1000) AS BIGINT) AS timestamp_ms, \
                 arg_max(sm.value, sm.timestamp) AS value, \
                 NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
                 NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
                 FROM {raw_table} sm \
                 WHERE sm.series_id IN ({ids}){raw_time} \
                 GROUP BY sm.series_id, time_bucket({iv}, sm.timestamp)"
            )
        } else {
            format!(
                "SELECT sm.series_id, \
                 CAST((epoch(sm.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
                 sm.value AS value, \
                 NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
                 NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
                 FROM {raw_table} sm \
                 WHERE sm.series_id IN ({ids}){raw_time}"
            )
        }
    };

    // Fully closed window → downsample only.
    if end <= cutoff {
        return format!("{} LIMIT {fetch_limit}", ds_select(start, end, true));
    }

    // Live window: historical downsample + recent raw (half-open at stitch).
    let mut parts = Vec::new();
    let raw_start = start.max(stitch);
    parts.push(raw_select(raw_start, end));
    if start < stitch {
        // Downsample is [start, stitch); raw is [stitch, end].
        parts.push(ds_select(start, stitch, false));
    }
    match parts.len() {
        1 => format!("{} LIMIT {fetch_limit}", parts[0]),
        _ => format!(
            "({}) UNION ALL ({}) LIMIT {fetch_limit}",
            parts[0], parts[1]
        ),
    }
}

fn hist_row_select_sql(
    catalog: &str,
    table: &str,
    ts_col: &str,
    ids: &str,
    time: &str,
    hist_arrays: bool,
    bucket_iv: Option<&str>,
) -> String {
    let hist = metric_table(catalog, table);
    let (count_expr, sum_expr, buckets_expr, bounds_expr) = if hist_arrays {
        (
            "sm.count",
            "sm.sum",
            "sm.bucket_counts",
            "sm.explicit_bounds",
        )
    } else {
        ("sm.count", "sm.sum", "NULL::UBIGINT[]", "NULL::DOUBLE[]")
    };
    if let Some(iv) = bucket_iv {
        if hist_arrays {
            format!(
                "SELECT sm.series_id, \
                 CAST((epoch(time_bucket({iv}, sm.{ts_col})) * 1000) AS BIGINT) AS timestamp_ms, \
                 arg_max(COALESCE(sm.sum, 0.0), sm.{ts_col}) AS value, \
                 arg_max(sm.count, sm.{ts_col}) AS count, arg_max(sm.sum, sm.{ts_col}) AS sum, \
                 arg_max(sm.bucket_counts, sm.{ts_col}) AS bucket_counts, \
                 arg_max(sm.explicit_bounds, sm.{ts_col}) AS explicit_bounds, NULL AS quantiles \
                 FROM {hist} sm \
                 WHERE sm.series_id IN ({ids}){time} \
                 GROUP BY sm.series_id, time_bucket({iv}, sm.{ts_col})"
            )
        } else {
            format!(
                "SELECT sm.series_id, \
                 CAST((epoch(time_bucket({iv}, sm.{ts_col})) * 1000) AS BIGINT) AS timestamp_ms, \
                 arg_max(COALESCE(sm.sum, 0.0), sm.{ts_col}) AS value, \
                 arg_max(sm.count, sm.{ts_col}) AS count, arg_max(sm.sum, sm.{ts_col}) AS sum, \
                 NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
                 FROM {hist} sm \
                 WHERE sm.series_id IN ({ids}){time} \
                 GROUP BY sm.series_id, time_bucket({iv}, sm.{ts_col})"
            )
        }
    } else {
        format!(
            "SELECT sm.series_id, \
             CAST((epoch(sm.{ts_col}) * 1000) AS BIGINT) AS timestamp_ms, \
             COALESCE(sm.sum, 0.0) AS value, \
             {count_expr}, {sum_expr}, {buckets_expr}, {bounds_expr}, NULL AS quantiles \
             FROM {hist} sm \
             WHERE sm.series_id IN ({ids}){time}"
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn hist_or_union_scan_sql(
    catalog: &str,
    ids: &str,
    time: &str,
    include_fidelity: bool,
    fetch_limit: usize,
    grain: SampleGrain,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    bucket_iv: Option<&str>,
    hist_arrays: bool,
) -> String {
    if grain.is_hist() {
        let body = match grain {
            SampleGrain::Hist => hist_row_select_sql(
                catalog,
                "metric_hist_samples",
                "timestamp",
                ids,
                time,
                hist_arrays,
                bucket_iv,
            ),
            SampleGrain::HistFiveMin => {
                // 5m hist for older data (guaranteed complete), raw for recent window.
                let now_ms = chrono::Utc::now().timestamp_millis();
                let end = end_ms.unwrap_or(now_ms);
                let start = start_ms.unwrap_or(i64::MIN);
                let cutoff = now_ms.saturating_sub(FIVE_MIN_LAG_MS);
                let stitch = stitch_raw_start_ms(cutoff, FIVE_MIN_LAG_MS);

                if end <= cutoff {
                    let ds_time = samples_time_predicates(Some(start), Some(end), "timestamp");
                    hist_row_select_sql(
                        catalog,
                        "metric_hist_samples_5m",
                        "timestamp",
                        ids,
                        &ds_time,
                        hist_arrays,
                        bucket_iv,
                    )
                } else {
                    let mut parts = Vec::new();
                    let raw_start = start.max(stitch);
                    let raw_time = samples_time_predicates(Some(raw_start), Some(end), "timestamp");
                    parts.push(hist_row_select_sql(
                        catalog,
                        "metric_hist_samples",
                        "timestamp",
                        ids,
                        &raw_time,
                        hist_arrays,
                        bucket_iv,
                    ));
                    if start < stitch {
                        let ds_time = samples_time_predicates_bounded(
                            Some(start),
                            Some(stitch),
                            "timestamp",
                            false,
                        );
                        parts.push(hist_row_select_sql(
                            catalog,
                            "metric_hist_samples_5m",
                            "timestamp",
                            ids,
                            &ds_time,
                            hist_arrays,
                            bucket_iv,
                        ));
                    }
                    match parts.len() {
                        1 => parts.into_iter().next().unwrap(),
                        _ => format!("({}) UNION ALL ({})", parts[0], parts[1]),
                    }
                }
            }
            SampleGrain::HistOneHour => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let end = end_ms.unwrap_or(now_ms);
                let start = start_ms.unwrap_or(i64::MIN);
                let cutoff = now_ms.saturating_sub(ONE_HOUR_LAG_MS);
                let stitch = stitch_raw_start_ms(cutoff, ONE_HOUR_LAG_MS);

                if end <= cutoff {
                    hist_row_select_sql(
                        catalog,
                        "metric_hist_samples_1h",
                        "timestamp",
                        ids,
                        time,
                        hist_arrays,
                        bucket_iv,
                    )
                } else {
                    let mut parts = Vec::new();
                    let raw_start = start.max(stitch);
                    let raw_time = samples_time_predicates(Some(raw_start), Some(end), "timestamp");
                    parts.push(hist_row_select_sql(
                        catalog,
                        "metric_hist_samples",
                        "timestamp",
                        ids,
                        &raw_time,
                        hist_arrays,
                        bucket_iv,
                    ));
                    if start < stitch {
                        let ds_time = samples_time_predicates_bounded(
                            Some(start),
                            Some(stitch),
                            "timestamp",
                            false,
                        );
                        parts.push(hist_row_select_sql(
                            catalog,
                            "metric_hist_samples_1h",
                            "timestamp",
                            ids,
                            &ds_time,
                            hist_arrays,
                            bucket_iv,
                        ));
                    }
                    match parts.len() {
                        1 => parts.into_iter().next().unwrap(),
                        _ => format!("({}) UNION ALL ({})", parts[0], parts[1]),
                    }
                }
            }
            _ => unreachable!("is_hist()"),
        };
        return format!("{body} LIMIT {fetch_limit}");
    }

    let samples = grain_table_sql(catalog, SampleGrain::Raw);
    let raw_time = samples_time_predicates(start_ms, end_ms, SampleGrain::Raw.time_column());
    let gauge_sql = format!(
        "SELECT sm.series_id, \
         CAST((epoch(sm.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
         sm.value, \
         NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
         NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
         FROM {samples} sm \
         WHERE sm.series_id IN ({ids}){raw_time}"
    );
    if !include_fidelity {
        return format!("{gauge_sql} LIMIT {fetch_limit}");
    }
    let hist_sql = hist_row_select_sql(
        catalog,
        "metric_hist_samples",
        "timestamp",
        ids,
        &raw_time,
        hist_arrays,
        bucket_iv,
    );
    format!("({gauge_sql}) UNION ALL ({hist_sql}) LIMIT {fetch_limit}")
}

/// Build samples SQL using §9.1 grain selection.
#[allow(clippy::too_many_arguments)]
pub fn samples_scan_sql_for_window(
    catalog: &str,
    series_ids: &[u64],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    step_ms: Option<i64>,
    label_proj: &str,
    include_fidelity: bool,
    is_histogram: bool,
    hist_arrays: bool,
    fetch_limit: usize,
) -> String {
    let grain = select_sample_grain(start_ms, end_ms, step_ms, is_histogram);
    samples_scan_sql(
        catalog,
        series_ids,
        start_ms,
        end_ms,
        label_proj,
        include_fidelity,
        fetch_limit,
        grain,
        step_ms,
        hist_arrays,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_predicate_is_timestamptz_only() {
        let pred = samples_time_predicates(Some(1_000), Some(2_000), "timestamp");
        assert!(pred.contains("TIMESTAMPTZ "), "{pred}");
        assert!(!pred.contains("to_timestamp("), "{pred}");
        assert!(pred.contains("sm.timestamp >="), "{pred}");
        assert!(!pred.contains("record_date"), "{pred}");
        // One-clock: precise ms bounds only — no redundant day-covering dual predicate.
        assert_eq!(
            pred.matches("sm.timestamp").count(),
            2,
            "expected start+end only: {pred}"
        );
    }

    #[test]
    fn stitch_downsample_end_is_exclusive() {
        let inclusive = samples_time_predicates(Some(1_000), Some(2_000), "timestamp");
        let exclusive =
            samples_time_predicates_bounded(Some(1_000), Some(2_000), "timestamp", false);
        assert!(inclusive.contains("timestamp <="), "{inclusive}");
        assert!(
            exclusive.contains("timestamp < ") && !exclusive.contains("timestamp <="),
            "{exclusive}"
        );
    }

    #[test]
    fn short_window_uses_raw_samples() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            true,
            100,
        );
        assert!(sql.contains("metric_samples"), "{sql}");
        assert!(!sql.contains("metric_samples_1h"), "{sql}");
    }

    #[test]
    fn stitch_raw_start_aligns_to_closed_bucket_end() {
        let bucket = 3_600_000i64;
        let cutoff = 1_700_005_220_000i64;
        let stitch = stitch_raw_start_ms(cutoff, bucket);
        assert_eq!(stitch, cutoff.div_euclid(bucket) * bucket);
        assert!(stitch <= cutoff);
        assert!(cutoff - stitch < bucket);
        assert_eq!(stitch_raw_start_ms(stitch, bucket), stitch);
    }
}
