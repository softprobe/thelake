//! Downsample ladder SQL (one clock: `timestamp` only).
//!
//! Per-day maintenance uses timestamp half-open windows so DuckLake can prune
//! `PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp))`.

use crate::sql::schema::{qualified_table_name, table_spec};
use chrono::NaiveDate;

fn qualified_metrics_layout_table(catalog: &str, name: &str) -> String {
    qualified_table_name(catalog, table_spec(name).expect("registered metric table"))
}

/// Raw samples must be older than this before entering 5m (closed buckets).
/// Kept short so Grafana long windows hit filled ladders within minutes of
/// demo ingest (empty-UNION was blowing the 100ms SLO).
pub const DOWNSAMPLE_5M_LAG: &str = "INTERVAL '5 minutes'";
/// 5m / raw must be older than this before entering 1h.
pub const DOWNSAMPLE_1H_LAG: &str = "INTERVAL '1 hour'";
/// Max closed days processed per maintenance pass (AC-Q9 / G2).
pub const HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS: usize = 4;
/// Alias used by scalar + collapse ladder steps (same bound as hist).
pub const METRICS_LADDER_MAX_DAYS_PER_PASS: usize = HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS;

/// Closed-bucket predicate: samples older than `lag`, and the whole
/// `[T, T+width)` window ended before that cutoff. `time_bucket <= now()-lag`
/// alone freezes a partial bucket via NOT EXISTS (e.g. 12:07 rolls 12:00
/// from samples before 12:02 and never accepts 12:02–12:05).
fn closed_bucket_filter(ts_expr: &str, bucket_interval: &str, lag: &str) -> String {
    format!(
        "{ts_expr} < now() - {lag}\n\
           AND time_bucket({bucket_interval}, {ts_expr}) + {bucket_interval} <= now() - {lag}"
    )
}

/// Partition prune from a timestamp lag: `ts < now() - lag` ⇒ day ≤ that cutoff.
fn day_le_lag(column: &str, lag: &str) -> String {
    // The closed-bucket predicate already supplies the strict event-time
    // bound. Keep this redundant timestamp predicate for partition pruning;
    // never cast the event clock to a DATE.
    format!("{column} < now() - {lag}")
}

fn day_eq_filter(column: &str, day: Option<NaiveDate>) -> String {
    match day {
        Some(d) => {
            let from = d.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
            let to = (d + chrono::Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .expect("next midnight")
                .and_utc();
            format!(
                "AND {column} >= {} AND {column} < {}",
                crate::sql::timestamptz_literal(&from),
                crate::sql::timestamptz_literal(&to)
            )
        }
        None => String::new(),
    }
}

/// INSERT … SELECT building `metric_samples_5m` from raw (incremental).
pub fn downsample_5m_sql(catalog_alias: &str) -> String {
    downsample_5m_for_day_sql(catalog_alias, None)
}

/// Days with raw samples whose 5m buckets are not materialized (bounded per pass).
pub fn downsample_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '5 minutes'", DOWNSAMPLE_5M_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_5M_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '5 minutes', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

/// One calendar-day slice of scalar 5m downsample (partition-scoped).
pub fn downsample_5m_for_day_sql(catalog_alias: &str, day: Option<NaiveDate>) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '5 minutes'", DOWNSAMPLE_5M_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_5M_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '5 minutes', timestamp) AS timestamp,\n\
           count(*)::UBIGINT AS count,\n\
           sum(value) AS sum,\n\
           min(value) AS min,\n\
           max(value) AS max,\n\
           arg_max(value, timestamp) AS last,\n\
           max(timestamp) AS last_ts\n\
         FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           {day_filter}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '5 minutes', raw.timestamp)\n\
           )\n\
         GROUP BY raw.series_id, time_bucket(INTERVAL '5 minutes', raw.timestamp);"
    )
}

/// INSERT … SELECT building `metric_samples_1h` from 5m (incremental).
pub fn downsample_1h_from_5m_sql(catalog_alias: &str) -> String {
    downsample_1h_from_5m_for_day_sql(catalog_alias, None)
}

pub fn downsample_1h_from_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE raw.timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND time_bucket(INTERVAL '1 hour', raw.timestamp) <= now() - {DOWNSAMPLE_1H_LAG}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn downsample_1h_from_5m_for_day_sql(catalog_alias: &str, day: Option<NaiveDate>) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '1 hour', timestamp) AS timestamp,\n\
           sum(count)::UBIGINT AS count,\n\
           sum(sum) AS sum,\n\
           min(min) AS min,\n\
           max(max) AS max,\n\
           arg_max(last, last_ts) AS last,\n\
           max(last_ts) AS last_ts\n\
         FROM {src} raw\n\
         WHERE raw.timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND time_bucket(INTERVAL '1 hour', raw.timestamp) <= now() - {DOWNSAMPLE_1H_LAG}\n\
           AND {day_bound}\n\
           {day_filter}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         GROUP BY raw.series_id, time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
         HAVING max(raw.timestamp) >= time_bucket(INTERVAL '1 hour', raw.timestamp) + INTERVAL '55 minutes';"
    )
}

/// Fallback: build 1h directly from raw when 5m is empty / lagging.
pub fn downsample_1h_from_raw_sql(catalog_alias: &str) -> String {
    downsample_1h_from_raw_for_day_sql(catalog_alias, None)
}

pub fn downsample_1h_from_raw_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '1 hour'", DOWNSAMPLE_1H_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn downsample_1h_from_raw_for_day_sql(catalog_alias: &str, day: Option<NaiveDate>) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '1 hour'", DOWNSAMPLE_1H_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '1 hour', timestamp) AS timestamp,\n\
           count(*)::UBIGINT AS count,\n\
           sum(value) AS sum,\n\
           min(value) AS min,\n\
           max(value) AS max,\n\
           arg_max(value, timestamp) AS last,\n\
           max(timestamp) AS last_ts\n\
         FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           {day_filter}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         GROUP BY raw.series_id, time_bucket(INTERVAL '1 hour', raw.timestamp);"
    )
}

/// Count SQL for AC-S2 / AC-M2 assertions.
pub fn count_sql(catalog_alias: &str, table: &str) -> String {
    let q = qualified_metrics_layout_table(catalog_alias, table);
    format!("SELECT count(*)::BIGINT FROM {q}")
}

/// INSERT … SELECT building `metric_hist_samples_5m` from raw hist (incremental).
///
/// Merges `bucket_counts` element-wise (Thanos compact analog). Rows without
/// bucket arrays still land count/sum aggregates.
pub fn hist_downsample_5m_sql(catalog_alias: &str) -> String {
    hist_downsample_5m_for_day_sql(catalog_alias, None)
}

/// Days with raw hist rows whose 5m buckets are not materialized (bounded per pass).
pub fn hist_downsample_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '5 minutes'", DOWNSAMPLE_5M_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_5M_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '5 minutes', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

/// One calendar-day slice of hist 5m downsample (partition-scoped for memory).
pub fn hist_downsample_5m_for_day_sql(
    catalog_alias: &str,
    day: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '5 minutes'", DOWNSAMPLE_5M_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_5M_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT raw.* FROM {src} raw\n\
           WHERE {closed}\n\
             AND {day_bound}\n\
             AND NOT EXISTS (\n\
               SELECT 1 FROM {dest} existing\n\
               WHERE existing.series_id = raw.series_id\n\
                 AND existing.timestamp = time_bucket(INTERVAL '5 minutes', raw.timestamp)\n\
             )\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '5 minutes', timestamp) AS timestamp,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, timestamp) AS explicit_bounds,\n\
             max(timestamp) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '5 minutes', s.timestamp) AS timestamp,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, timestamp,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.timestamp, sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, timestamp);"
    )
}

/// INSERT … SELECT building `metric_hist_samples_1h` from 5m hist (incremental).
pub fn hist_downsample_1h_from_5m_sql(catalog_alias: &str) -> String {
    hist_downsample_1h_from_5m_for_day_sql(catalog_alias, None)
}

pub fn hist_downsample_1h_from_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE raw.timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn hist_downsample_1h_from_5m_for_day_sql(
    catalog_alias: &str,
    day: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT raw.* FROM {src} raw\n\
           WHERE raw.timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
             AND time_bucket(INTERVAL '1 hour', raw.timestamp) <= now() - {DOWNSAMPLE_1H_LAG}\n\
             AND {day_bound}\n\
             AND NOT EXISTS (\n\
               SELECT 1 FROM {dest} existing\n\
               WHERE existing.series_id = raw.series_id\n\
                 AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
             )\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '1 hour', timestamp) AS timestamp,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, last_ts) AS explicit_bounds,\n\
             max(last_ts) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
           HAVING max(timestamp) >= time_bucket(INTERVAL '1 hour', timestamp) + INTERVAL '55 minutes'\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '1 hour', s.timestamp) AS timestamp,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, timestamp,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.timestamp, sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, timestamp);"
    )
}

/// Fallback: build hist 1h directly from raw when 5m hist is empty / lagging.
pub fn hist_downsample_1h_from_raw_sql(catalog_alias: &str) -> String {
    hist_downsample_1h_from_raw_for_day_sql(catalog_alias, None)
}

pub fn hist_downsample_1h_from_raw_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '1 hour'", DOWNSAMPLE_1H_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    format!(
        "SELECT DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC') AS day FROM {src} raw\n\
         WHERE {closed}\n\
           AND {day_bound}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.series_id = raw.series_id\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn hist_downsample_1h_from_raw_for_day_sql(
    catalog_alias: &str,
    day: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let closed = closed_bucket_filter("raw.timestamp", "INTERVAL '1 hour'", DOWNSAMPLE_1H_LAG);
    let day_bound = day_le_lag("raw.timestamp", DOWNSAMPLE_1H_LAG);
    let day_filter = day_eq_filter("raw.timestamp", day);
    format!(
        "INSERT INTO {dest} (series_id, timestamp, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT raw.* FROM {src} raw\n\
           WHERE {closed}\n\
             AND {day_bound}\n\
             AND NOT EXISTS (\n\
               SELECT 1 FROM {dest} existing\n\
               WHERE existing.series_id = raw.series_id\n\
                 AND existing.timestamp = time_bucket(INTERVAL '1 hour', raw.timestamp)\n\
             )\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '1 hour', timestamp) AS timestamp,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, timestamp) AS explicit_bounds,\n\
             max(timestamp) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '1 hour', s.timestamp) AS timestamp,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, timestamp,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.timestamp, sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, timestamp);"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downsample_5m_sql_is_key_scoped_and_additive() {
        let sql = downsample_5m_sql("softprobe");
        assert!(sql.starts_with("INSERT INTO softprobe.metric_samples_5m"));
        assert!(sql.contains("FROM softprobe.metric_samples"));
        assert!(sql.contains("INTERVAL '5 minutes'"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(
            sql.contains("time_bucket(INTERVAL '5 minutes', raw.timestamp) + INTERVAL '5 minutes'"),
            "5m must wait for the bucket to close: {sql}"
        );
        assert!(!sql.to_lowercase().contains("delete"));
        assert!(!sql.to_lowercase().contains("truncate"));
    }

    #[test]
    fn downsample_1h_sql_uses_1h_lag_and_key_guard() {
        let from_5m = downsample_1h_from_5m_sql("softprobe");
        assert!(from_5m.contains("metric_samples_5m"));
        assert!(from_5m.contains("INTERVAL '1 hour'"));
        assert!(from_5m.contains("INSERT INTO softprobe.metric_samples_1h"));
        assert!(from_5m.contains("NOT EXISTS"));
        assert!(from_5m.contains("max(raw.timestamp)"));
        assert!(
            from_5m.contains(
                "time_bucket(INTERVAL '1 hour', raw.timestamp) <= now() - INTERVAL '1 hour'"
            ),
            "1h from 5m must wait for closed hours"
        );
        assert!(
            from_5m.contains("INTERVAL '55 minutes'"),
            "1h from 5m must wait for last 5m slot in hour"
        );

        let from_raw = downsample_1h_from_raw_sql("softprobe");
        assert!(from_raw.contains("FROM softprobe.metric_samples raw"));
        assert!(from_raw.contains("INTERVAL '1 hour'"));
        assert!(from_raw.contains("NOT EXISTS"));
        assert!(
            from_raw.contains(
                "time_bucket(INTERVAL '1 hour', raw.timestamp) + INTERVAL '1 hour' <= now() - INTERVAL '1 hour'"
            ),
            "1h from raw must wait for closed hours"
        );
    }

    /// AC-M2 shape: the destination guard is scoped to the series/day/window key.
    #[test]
    fn downsample_guard_is_key_scoped() {
        let sql = downsample_5m_sql("softprobe");
        assert!(sql.contains("existing.series_id = raw.series_id"));
        assert!(!sql.contains("record_date"));
        assert!(sql.contains("existing.timestamp"));
    }

    #[test]
    fn downsample_5m_for_day_scopes_partition_day() {
        let day = chrono::NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let sql = downsample_5m_for_day_sql("softprobe", Some(day));
        assert!(sql.contains("raw.timestamp >= TIMESTAMPTZ '2026-08-14 00:00:00.000000+00'"));
        assert!(sql.contains("raw.timestamp < now() - INTERVAL '5 minutes'"));
    }

    #[test]
    fn downsample_pending_days_sql_is_bounded() {
        let sql = downsample_5m_pending_days_sql("softprobe", 4);
        assert!(sql.contains("LIMIT 4"));
        assert!(sql.contains("DISTINCT date_trunc('day', raw.timestamp AT TIME ZONE 'UTC')"));
        assert!(sql.contains("raw.timestamp < now() - INTERVAL '5 minutes'"));
    }

    #[test]
    fn hist_downsample_5m_sql_is_incremental() {
        let sql = hist_downsample_5m_sql("softprobe");
        assert!(sql.contains("INSERT INTO softprobe.metric_hist_samples_5m"));
        assert!(sql.contains("FROM softprobe.metric_hist_samples"));
        assert!(sql.contains("unnest(s.bucket_counts)"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(!sql.to_lowercase().contains("delete"));
    }

    #[test]
    fn hist_downsample_5m_for_day_scopes_partition_day() {
        let day = chrono::NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let sql = hist_downsample_5m_for_day_sql("softprobe", Some(day));
        assert!(sql.contains("raw.timestamp >= TIMESTAMPTZ '2026-08-14 00:00:00.000000+00'"));
        assert!(sql.contains("INSERT INTO softprobe.metric_hist_samples_5m"));
    }

    #[test]
    fn hist_downsample_pending_days_sql_is_bounded() {
        let sql = hist_downsample_5m_pending_days_sql("softprobe", 4);
        assert!(sql.contains("LIMIT 4"));
        assert!(sql.contains("metric_hist_samples"));
    }

    #[test]
    fn hist_downsample_1h_sql_uses_1h_lag() {
        let from_5m = hist_downsample_1h_from_5m_sql("softprobe");
        assert!(from_5m.contains("metric_hist_samples_5m"));
        assert!(from_5m.contains("INTERVAL '1 hour'"));
        let from_raw = hist_downsample_1h_from_raw_sql("softprobe");
        assert!(from_raw.contains("metric_hist_samples raw"));
    }
}
