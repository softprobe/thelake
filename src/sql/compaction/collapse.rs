//! Compaction / downsample / collapse SQL (one clock: `timestamp` only).

use crate::sql::literal::sql_string_literal;
use crate::sql::schema::{qualified_table_name, table_spec};
use crate::sql::{BoundLakeSql, QueryWindow};
use chrono::{DateTime, NaiveDate, Utc};

/// Lag before collapsing from raw samples.
pub const COLLAPSE_FROM_RAW_LAG: &str = "INTERVAL '24 hours'";

fn day_start_end(day: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = day.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
    let end = (day + chrono::Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .expect("next midnight")
        .and_utc()
        - chrono::Duration::nanoseconds(1);
    (start, end)
}

fn day_timestamp_filter(alias: &str, day: Option<NaiveDate>) -> String {
    match day {
        Some(d) => {
            let (from, to) = day_start_end(d);
            let w = QueryWindow { from, to };
            format!("AND {}", w.timestamp_bound_sql(alias))
        }
        None => String::new(),
    }
}

fn lag_timestamp_filter(alias: &str, lag: &str) -> String {
    format!("{alias}timestamp < now() - {lag}")
}

fn metric_table(catalog: &str, name: &str) -> String {
    qualified_table_name(catalog, table_spec(name).expect("registered metric table"))
}

/// Incremental INSERT for `metric_collapse_job_1h` from 1h samples + job postings.
pub fn collapse_job_1h_sql(catalog_alias: &str) -> String {
    collapse_job_1h_for_day_sql(catalog_alias, None)
}

pub fn collapse_job_1h_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let dest = metric_table(catalog_alias, "metric_collapse_job_1h");
    let samples_1h = metric_table(catalog_alias, "metric_samples_1h");
    let series = metric_table(catalog_alias, "metric_series");
    let postings = metric_table(catalog_alias, "metric_postings");
    let sample_series_day = crate::sql::same_utc_calendar_day("h.timestamp", "s.timestamp");
    let sample_posting_day = crate::sql::same_utc_calendar_day("p.timestamp", "h.timestamp");
    format!(
        "SELECT DISTINCT strftime(h.timestamp AT TIME ZONE 'UTC', '%Y-%m-%d') AS day\n\
         FROM {samples_1h} h\n\
         JOIN {series} s ON h.series_id = s.series_id AND {sample_series_day}\n\
         JOIN {postings} p ON p.series_id = h.series_id AND p.label_name = 'job' AND {sample_posting_day}\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {dest} existing\n\
           WHERE existing.metric_name = s.metric_name\n\
             AND existing.job = p.label_value\n\
             AND existing.timestamp = h.timestamp\n\
         )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn collapse_job_1h_for_day_sql(catalog_alias: &str, day: Option<NaiveDate>) -> String {
    let dest = metric_table(catalog_alias, "metric_collapse_job_1h");
    let samples_1h = metric_table(catalog_alias, "metric_samples_1h");
    let series = metric_table(catalog_alias, "metric_series");
    let postings = metric_table(catalog_alias, "metric_postings");
    let sample_series_day = crate::sql::same_utc_calendar_day("h.timestamp", "s.timestamp");
    let sample_posting_day = crate::sql::same_utc_calendar_day("p.timestamp", "h.timestamp");
    let day_filter = day_timestamp_filter("h.", day);
    format!(
        "INSERT INTO {dest} (metric_name, job, timestamp, count, sum, min, max, last)\n\
         SELECT\n\
           s.metric_name,\n\
           p.label_value AS job,\n\
           h.timestamp,\n\
           sum(h.count)::UBIGINT AS count,\n\
           sum(h.sum) AS sum,\n\
           min(h.min) AS min,\n\
           max(h.max) AS max,\n\
           sum(h.last) AS last\n\
         FROM {samples_1h} h\n\
         JOIN {series} s ON h.series_id = s.series_id AND {sample_series_day}\n\
         JOIN {postings} p ON p.series_id = h.series_id AND p.label_name = 'job' AND {sample_posting_day}\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {dest} existing\n\
           WHERE existing.metric_name = s.metric_name\n\
             AND existing.job = p.label_value\n\
             AND existing.timestamp = h.timestamp\n\
         )\n\
         {day_filter}\n\
         GROUP BY s.metric_name, p.label_value, h.timestamp;"
    )
}

pub fn collapse_job_1h_from_raw_sql(catalog_alias: &str) -> String {
    collapse_job_1h_from_raw_for_day_sql(catalog_alias, None)
}

pub fn collapse_job_1h_from_raw_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let dest = metric_table(catalog_alias, "metric_collapse_job_1h");
    let samples = metric_table(catalog_alias, "metric_samples");
    let series = metric_table(catalog_alias, "metric_series");
    let postings = metric_table(catalog_alias, "metric_postings");
    let sample_series_day = crate::sql::same_utc_calendar_day("sm.timestamp", "s.timestamp");
    let sample_posting_day = crate::sql::same_utc_calendar_day("p.timestamp", "sm.timestamp");
    let lag = lag_timestamp_filter("sm.", COLLAPSE_FROM_RAW_LAG);
    format!(
        "SELECT DISTINCT strftime(sm.timestamp AT TIME ZONE 'UTC', '%Y-%m-%d') AS day\n\
         FROM {samples} sm\n\
         JOIN {series} s ON sm.series_id = s.series_id AND {sample_series_day}\n\
         JOIN {postings} p ON p.series_id = sm.series_id AND p.label_name = 'job' AND {sample_posting_day}\n\
         WHERE {lag}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.metric_name = s.metric_name\n\
               AND existing.job = p.label_value\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', sm.timestamp)\n\
           )\n\
         ORDER BY day\n\
         LIMIT {limit};"
    )
}

pub fn collapse_job_1h_from_raw_for_day_sql(catalog_alias: &str, day: Option<NaiveDate>) -> String {
    let dest = metric_table(catalog_alias, "metric_collapse_job_1h");
    let samples = metric_table(catalog_alias, "metric_samples");
    let series = metric_table(catalog_alias, "metric_series");
    let postings = metric_table(catalog_alias, "metric_postings");
    let sample_series_day = crate::sql::same_utc_calendar_day("sm.timestamp", "s.timestamp");
    let sample_posting_day = crate::sql::same_utc_calendar_day("p.timestamp", "sm.timestamp");
    let lag = lag_timestamp_filter("sm.", COLLAPSE_FROM_RAW_LAG);
    let day_filter = day_timestamp_filter("sm.", day);
    format!(
        "INSERT INTO {dest} (metric_name, job, timestamp, count, sum, min, max, last)\n\
         SELECT\n\
           s.metric_name,\n\
           p.label_value AS job,\n\
           time_bucket(INTERVAL '1 hour', sm.timestamp) AS timestamp,\n\
           count(*)::UBIGINT AS count,\n\
           sum(sm.value) AS sum,\n\
           min(sm.value) AS min,\n\
           max(sm.value) AS max,\n\
           arg_max(sm.value, sm.timestamp) AS last\n\
         FROM {samples} sm\n\
         JOIN {series} s ON sm.series_id = s.series_id AND {sample_series_day}\n\
         JOIN {postings} p ON p.series_id = sm.series_id AND p.label_name = 'job' AND {sample_posting_day}\n\
         WHERE {lag}\n\
           {day_filter}\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.metric_name = s.metric_name\n\
               AND existing.job = p.label_value\n\
               AND existing.timestamp = time_bucket(INTERVAL '1 hour', sm.timestamp)\n\
           )\n\
         GROUP BY s.metric_name, p.label_value, time_bucket(INTERVAL '1 hour', sm.timestamp);"
    )
}

/// Scan SQL for collapse Prom path — requires a finite window.
pub fn collapse_scan_sql(
    catalog_alias: &str,
    metric_name: &str,
    start_ms: i64,
    end_ms: i64,
    fetch_limit: usize,
) -> BoundLakeSql {
    let from =
        DateTime::<Utc>::from_timestamp_millis(start_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let to = DateTime::<Utc>::from_timestamp_millis(end_ms).unwrap_or(from);
    let window = QueryWindow::try_new(from, to).unwrap_or(QueryWindow { from, to: from });
    let table = metric_table(catalog_alias, "metric_collapse_job_1h");
    let name = sql_string_literal(metric_name);
    window.bind_scan("c.", |bound| {
        format!(
            "SELECT c.metric_name, c.job, \
             CAST((epoch(c.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
             c.last AS value, c.count, c.sum, c.min, c.max\n\
             FROM {table} c\n\
             WHERE c.metric_name = {name} AND {bound}\n\
             ORDER BY c.job, c.timestamp\n\
             LIMIT {fetch_limit}"
        )
    })
}

/// Legacy Option-window wrapper used by existing Prom edge (converts at edge).
pub fn collapse_scan_sql_optional(
    catalog_alias: &str,
    metric_name: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    fetch_limit: usize,
) -> String {
    let start = start_ms.unwrap_or(0);
    let end = end_ms.unwrap_or(start);
    collapse_scan_sql(catalog_alias, metric_name, start, end, fetch_limit).into_sql()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_scan_is_timestamp_bound_only() {
        let sql = collapse_scan_sql(
            "softprobe",
            "http_requests",
            1_700_000_000_000,
            1_700_003_600_000,
            100,
        )
        .into_sql();
        assert!(sql.contains("CAST(c.timestamp AS TIMESTAMP_NS)"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
    }

    #[test]
    fn collapse_insert_has_no_forbidden_columns() {
        let sql = collapse_job_1h_sql("softprobe");
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
        assert!(sql.contains("timestamp"));
    }
}
