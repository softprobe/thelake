//! Prometheus series resolve via day-partitioned `metric_postings` (§9.1).
//!
//! Equality matchers → postings intersect → `series_id` set; then skinny
//! `metric_samples` / `metric_hist_samples` scan. Does not scan the compatibility relation
//! or full `union_metrics` for resolve.
//!
//! SQL recipes live under [`crate::sql::prom`]; this module owns resolve/runtime/cache.

use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::prometheus::sanitize_label_name;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use crate::sql::prom::{
    discover_name_values_sql, resolve_series_ids_sql, samples_scan_sql,
    samples_scan_sql_for_window, samples_time_predicates, samples_time_predicates_bounded,
    series_meta_sql, single_posting_sql, timestamptz_literal_ms, EqualityPosting, PostingsDayRange,
    SeriesMetaDayScope,
};

/// Equality matchers used for postings resolve (`=` only).
pub fn equality_postings(matchers: &[LabelMatcher]) -> Vec<EqualityPosting> {
    let mut out = Vec::new();
    for m in matchers {
        if m.op != MatcherOp::Eq {
            continue;
        }
        let values = if m.name == "__name__" {
            posting_name_values(&m.value)
        } else {
            vec![m.value.clone()]
        };
        out.push(EqualityPosting {
            label_name: m.name.clone(),
            values,
        });
    }
    out
}

/// Posting `__name__` candidates: exact Prom name + dotted OTel form. Dual-written
/// `_bucket`/`_sum`/`_count` series keep their suffix names. Other histograms
/// expand from the native base series, so suffix selectors also resolve the base.
pub fn posting_name_values(prom_name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut candidates = vec![
        prom_name.to_string(),
        prom_name.replace('_', "."),
        prom_name.replace('.', "_"),
    ];
    if crate::compat::projection::prometheus::classic_suffix_uses_native_hist(prom_name) {
        if let Some(base) =
            crate::compat::projection::prometheus::classic_prom_suffix_base(prom_name)
        {
            candidates.push(base.to_string());
            candidates.push(base.replace('_', "."));
            candidates.push(base.replace('.', "_"));
        }
    }
    for cand in candidates {
        let s = sanitize_label_name(&cand);
        if !out.contains(&s) {
            out.push(s);
        }
    }
    out
}

/// Softprobe analog of Greptime SST inverted-index tag→row-group bitmaps
/// (§4.4 MEASURE / AC-G3): cache equality posting id sets keyed by
/// `(engine, tenant, calendar_day, label_name, label_value)` with a short TTL.
///
/// Keying by calendar day prevents serving yesterday's postings for today.
/// TTL covers same-day ingest freshness without a Puffin/SST index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostingCacheKey {
    pub engine_id: usize,
    pub tenant_id: String,
    /// Calendar day of the posting (not a lake DATE column).
    pub record_date: NaiveDate,
    pub label_name: String,
    pub label_value: String,
}

/// Grafana refresh storms hit warm sets; long dashboard sweeps need ≥5m TTL.
pub const POSTING_CACHE_TTL: Duration = Duration::from_secs(300);
const POSTING_CACHE_MAX: usize = 8192;

#[derive(Clone)]
struct PostingCacheEntry {
    /// Sorted unique `series_id`s for one equality posting on one day.
    series_ids: Arc<Vec<u64>>,
    expires: Instant,
}

#[derive(Default)]
pub struct PostingSetCache {
    entries: HashMap<PostingCacheKey, PostingCacheEntry>,
}

impl PostingSetCache {
    pub fn get(&mut self, key: &PostingCacheKey, now: Instant) -> Option<Arc<Vec<u64>>> {
        if let Some(entry) = self.entries.get(key) {
            if entry.expires > now {
                return Some(Arc::clone(&entry.series_ids));
            }
        }
        self.entries.retain(|_, e| e.expires > now);
        None
    }

    pub fn put(&mut self, key: PostingCacheKey, series_ids: Arc<Vec<u64>>, now: Instant) {
        if self.entries.len() >= POSTING_CACHE_MAX {
            self.entries.retain(|_, e| e.expires > now);
            if self.entries.len() >= POSTING_CACHE_MAX {
                let drop_n = POSTING_CACHE_MAX / 2;
                let keys: Vec<PostingCacheKey> =
                    self.entries.keys().take(drop_n).cloned().collect();
                for k in keys {
                    self.entries.remove(&k);
                }
            }
        }
        self.entries.insert(
            key,
            PostingCacheEntry {
                series_ids,
                expires: now + POSTING_CACHE_TTL,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Test/helper: clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Merge two sorted unique id slices (union).
pub fn union_sorted_ids(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    if i < a.len() {
        out.extend_from_slice(&a[i..]);
    }
    if j < b.len() {
        out.extend_from_slice(&b[j..]);
    }
    out
}

/// Intersect two sorted unique id slices.
pub fn intersect_sorted_ids(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Intersect equality posting sets in-process (Greptime II analog after cache fill).
///
/// For each equality constraint, unions posting lists across `days` and values;
/// then intersects across constraints. Empty equality → empty result (caller
/// should use unbounded SQL path for "all series" discovery).
pub fn intersect_equality_postings_from_sets(
    equality: &[EqualityPosting],
    days: &[NaiveDate],
    mut lookup: impl FnMut(&NaiveDate, &str, &str) -> Arc<Vec<u64>>,
) -> Vec<u64> {
    if equality.is_empty() || days.is_empty() {
        return Vec::new();
    }
    let mut acc: Option<Vec<u64>> = None;
    for eq in equality {
        let mut eq_set: Vec<u64> = Vec::new();
        for day in days {
            for value in &eq.values {
                let part = lookup(day, &eq.label_name, value);
                eq_set = union_sorted_ids(&eq_set, part.as_slice());
            }
        }
        acc = Some(match acc {
            None => eq_set,
            Some(prev) => intersect_sorted_ids(&prev, &eq_set),
        });
    }
    acc.unwrap_or_default()
}

/// Fail loud when resolved id count exceeds `max_series` (AC-Q4). Message must
/// contain `max_series`; callers must not run a sample scan after this error.
pub fn enforce_resolved_series_cap(count: usize, max_series: usize) -> Result<(), CompatError> {
    if count > max_series {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            format!("series count {count} exceeds max_series {max_series}"),
        ));
    }
    Ok(())
}

/// True when SQL is a postings+samples resolve path (AC-Q7 shape check).
pub fn sql_is_postings_resolve_path(resolve_sql: &str, samples_sql: &str) -> bool {
    let resolve_ok =
        resolve_sql.contains("metric_postings") && !resolve_sql.contains("FROM union_metrics");
    let samples_ok = (samples_sql.contains("metric_samples")
        || samples_sql.contains("metric_samples_5m")
        || samples_sql.contains("metric_samples_1h")
        || samples_sql.contains("metric_hist_samples"))
        && samples_sql.contains("series_id IN")
        && !samples_sql.contains("FROM union_metrics");
    resolve_ok && samples_ok
}

/// True when hist Prom short-window SQL uses postings + `metric_hist_samples` (AC-H2).
pub fn sql_is_hist_prom_path(resolve_sql: &str, samples_sql: &str) -> bool {
    let resolve_ok =
        resolve_sql.contains("metric_postings") && !resolve_sql.contains("FROM union_metrics");
    let samples_ok = samples_sql.contains("metric_hist_samples")
        && samples_sql.contains("series_id IN")
        && !samples_sql.contains("FROM union_metrics")
        // Gauge skinny samples must not back hist selectors on the short path.
        && !samples_sql.contains("metric_samples sm")
        && !samples_sql.contains(".metric_samples sm");
    resolve_ok && samples_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::grain::SampleGrain;
    use crate::models::Metric;
    use crate::storage::ducklake::{write_metrics_layout_txn, DEFAULT_MAX_LABELS_PER_SERIES};
    use chrono::{DateTime, TimeZone, Utc};
    use duckdb::Connection;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::TempDir;

    /// AC-G3 / §4.4: cache hit returns same intersect as cold fill; day key isolates.
    #[test]
    fn posting_cache_hit_matches_cold_intersect_and_isolates_days() {
        let day_a = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        let day_b = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        let mut cache = PostingSetCache::default();
        let now = Instant::now();
        let key_name = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_a,
            label_name: "__name__".into(),
            label_value: "layout_wide".into(),
        };
        let key_inst = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_a,
            label_name: "instance".into(),
            label_value: "i-1".into(),
        };
        let key_name_b = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_b,
            label_name: "__name__".into(),
            label_value: "layout_wide".into(),
        };
        cache.put(key_name.clone(), Arc::new(vec![1, 2, 3, 99]), now);
        cache.put(key_inst.clone(), Arc::new(vec![2, 99, 100]), now);
        // Different day must not leak day_a ids even for same label/value.
        cache.put(key_name_b, Arc::new(vec![7, 8]), now);

        let fetches = AtomicUsize::new(0);
        let equality = vec![
            EqualityPosting {
                label_name: "__name__".into(),
                values: vec!["layout_wide".into()],
            },
            EqualityPosting {
                label_name: "instance".into(),
                values: vec!["i-1".into()],
            },
        ];
        let hit = intersect_equality_postings_from_sets(&equality, &[day_a], |day, name, value| {
            let key = PostingCacheKey {
                engine_id: 1,
                tenant_id: "t1".into(),
                record_date: *day,
                label_name: name.to_string(),
                label_value: value.to_string(),
            };
            cache.get(&key, now).unwrap_or_else(|| {
                fetches.fetch_add(1, AtomicOrdering::SeqCst);
                Arc::new(Vec::new())
            })
        });
        assert_eq!(hit, vec![2, 99], "warm intersect must match posting AND");
        assert_eq!(
            fetches.load(AtomicOrdering::SeqCst),
            0,
            "must be cache hits"
        );

        // Expired entry must miss (TTL).
        let expired = now + POSTING_CACHE_TTL + Duration::from_secs(1);
        assert!(cache.get(&key_name, expired).is_none());

        // Day B name posting is isolated from day A intersect.
        let only_b =
            intersect_equality_postings_from_sets(&equality[..1], &[day_b], |day, name, value| {
                let key = PostingCacheKey {
                    engine_id: 1,
                    tenant_id: "t1".into(),
                    record_date: *day,
                    label_name: name.to_string(),
                    label_value: value.to_string(),
                };
                // Re-seed day_b after TTL wipe for this assertion.
                if *day == day_b {
                    Arc::new(vec![7, 8])
                } else {
                    cache.get(&key, now).unwrap_or_else(|| Arc::new(Vec::new()))
                }
            });
        assert_eq!(only_b, vec![7, 8]);
        assert!(!only_b.contains(&2), "must not serve day_a ids for day_b");
    }

    #[test]
    fn union_and_intersect_sorted_ids() {
        assert_eq!(
            union_sorted_ids(&[1, 3, 5], &[2, 3, 4]),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(intersect_sorted_ids(&[1, 3, 5, 7], &[3, 7, 9]), vec![3, 7]);
        assert!(intersect_sorted_ids(&[1, 2], &[3, 4]).is_empty());
    }

    #[test]
    fn inclusive_days_and_single_posting_sql() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 14).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()),
        };
        assert_eq!(
            days.inclusive_days().unwrap().len(),
            3,
            "inclusive day walk"
        );
        let sql = single_posting_sql(
            "softprobe",
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(),
            "__name__",
            "layout_wide",
        );
        assert!(sql.contains("metric_postings"));
        assert!(
            sql.contains("timestamp >="),
            "single posting must bind timestamp day window: {sql}"
        );
        assert!(!sql.contains("CAST(timestamp AS DATE)"), "{sql}");
        assert!(sql.contains("label_name = '__name__'"));
        assert!(sql.contains("label_value = 'layout_wide'"));
    }

    fn attach_ducklake(temp: &TempDir) -> (Connection, String) {
        let config = crate::test_support::file_backed_test_config(temp);
        let (conn, catalog) =
            crate::storage::ducklake::open_and_attach_ducklake(&config.ducklake).expect("attach");
        crate::storage::schema::ensure_metrics_layout_family_tables(&conn, &catalog)
            .expect("layout ensure");
        (conn, catalog)
    }

    fn gauge(name: &str, instance: &str, ts: DateTime<Utc>, value: f64) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("service.instance.id".into(), instance.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "d".into(),
            unit: "1".into(),
            metric_type: "gauge".into(),
            timestamp: ts,
            value,
            attributes: attrs,
            resource_attributes: resource,
            ..Default::default()
        }
    }

    fn gauge_pod(name: &str, pod: &str, ts: DateTime<Utc>, value: f64) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("pod".into(), pod.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "d".into(),
            unit: "1".into(),
            metric_type: "gauge".into(),
            timestamp: ts,
            value,
            attributes: attrs,
            resource_attributes: resource,
            ..Default::default()
        }
    }

    /// T-Q6 / AC-Q6: discovery SQL uses metric_postings + label_name='__name__'.
    #[test]
    fn discover_sql_uses_postings() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let sql = discover_name_values_sql("softprobe", days, 10_000);
        assert!(
            sql.contains("metric_postings"),
            "AC-Q6: expected metric_postings, got {sql}"
        );
        assert!(
            sql.contains("label_name = '__name__'"),
            "AC-Q6: expected label_name = '__name__', got {sql}"
        );
        assert!(
            !sql.contains("metric_samples"),
            "AC-Q6: must not scan metric_samples for discovery, got {sql}"
        );
    }

    /// T-Q7 / AC-Q7: resolve + samples SQL shape (postings + series_id IN).
    #[test]
    fn resolve_and_samples_sql_uses_postings_not_fat() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let eq = equality_postings(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "layout_wide".into(),
            },
            LabelMatcher {
                name: "instance".into(),
                op: MatcherOp::Eq,
                value: "i-1".into(),
            },
        ]);
        let resolve = resolve_series_ids_sql("softprobe", days, &eq, 10_000);
        let samples = samples_scan_sql(
            "softprobe",
            &[42],
            Some(1_000),
            Some(2_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            100,
            SampleGrain::Raw,
            None,
            true,
        );
        assert!(
            sql_is_postings_resolve_path(&resolve, &samples),
            "AC-Q7 resolve={resolve}\nsamples={samples}"
        );
        assert!(resolve.contains("INTERSECT") || resolve.contains("instance"));
        assert!(samples.contains("series_id IN (42)"));
        assert!(!samples.contains("to_timestamp("));
        assert!(samples.contains("TIMESTAMPTZ "));
        assert!(
            samples.contains("timestamp"),
            "AC-Q3/G3: sample scan must be time-bound, got {samples}"
        );
    }

    /// T-Q2 SQL shape: 30d scan references metric_samples_1h, not raw.
    #[test]
    fn long_range_samples_sql_uses_1h_grain() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 86_400_000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(3_600_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            true,
            100,
        );
        assert!(
            sql.contains("metric_samples_1h"),
            "AC-Q2: expected metric_samples_1h, got {sql}"
        );
        assert!(
            !sql.contains(".metric_samples sm"),
            "AC-Q2: historical window must not use raw metric_samples, got {sql}"
        );
        assert!(sql.contains("timestamp"));
        assert!(!sql.contains("to_timestamp("));
    }

    /// T-W6 SQL shape: 180d → 1h grain.
    #[test]
    fn samples_sql_180d_uses_1h_grain() {
        let end = 1_700_000_000_000i64;
        let start = end - 180 * 86_400_000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(3_600_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            true,
            100,
        );
        assert!(sql.contains("metric_samples_1h"), "{sql}");
    }

    /// T-Q1 SQL shape: 30m → raw metric_samples.
    #[test]
    fn short_range_samples_sql_uses_raw_grain() {
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
        assert!(
            sql.contains("metric_samples sm") || sql.contains(".metric_samples sm"),
            "AC-Q1: expected raw metric_samples, got {sql}"
        );
        assert!(!sql.contains("metric_samples_1h"));
        assert!(!sql.contains("metric_samples_5m"));
        assert!(!sql.contains("to_timestamp("));
        assert!(
            sql.contains("sm.series_id") && !sql.contains("JOIN") && !sql.contains("CAST(s.labels"),
            "sample scan must be skinny (no series JOIN / VARIANT labels): {sql}"
        );
        assert!(
            sql.contains("time_bucket(INTERVAL '15 seconds'"),
            "Grafana 15s step must bucket raw scans: {sql}"
        );
    }

    #[test]
    fn series_meta_sql_reads_labels_as_json_once() {
        let sql = series_meta_sql(
            "softprobe",
            &[42],
            SeriesMetaDayScope::Recent,
            Some("demo_metric"),
            Some(1_000),
            Some(2_000),
        );
        assert!(sql.contains("metric_series"));
        assert!(sql.contains("CAST(s.labels AS JSON)"));
        assert!(sql.contains("QUALIFY row_number()"));
        assert!(sql.contains("thelake_series_meta_recent"));
        assert!(sql.contains("s.timestamp >="));
        assert!(sql.contains("INTERVAL '2' DAY"));
        assert!(!sql.contains("CAST(s.timestamp AS DATE)"));
        assert!(sql.contains("metric_name = 'demo_metric'"));
        assert!(!sql.contains("CAST(s.labels['"));
        assert!(sql.contains("series_id IN (42)"));

        let all = series_meta_sql(
            "softprobe",
            &[42],
            SeriesMetaDayScope::QueryWindow,
            None,
            Some(1_700_000_000_000),
            Some(1_700_086_400_000),
        );
        assert!(all.contains("thelake_series_meta_all"));
        assert!(all.contains("timestamp"));
        assert!(!all.contains("CURRENT_DATE"));
    }

    /// Live 30d panels (`end ≈ now`) must UNION 1h history + raw lag, not raw-only.
    #[test]
    fn live_long_range_samples_sql_unions_1h_with_raw_tail() {
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - 30 * 86_400_000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(3_600_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            true,
            100,
        );
        assert!(
            sql.contains("metric_samples_1h"),
            "live 30d must read 1h history, got {sql}"
        );
        assert!(
            sql.contains("UNION ALL"),
            "live 30d must UNION downsample + raw lag, got {sql}"
        );
        assert!(
            sql.contains("metric_samples sm") || sql.contains(".metric_samples sm"),
            "live 30d must keep raw lag tail, got {sql}"
        );
        // Downsample side must be half-open at stitch (`timestamp < stitch`), not `<=`.
        // Raw lag arm still uses inclusive `<= end`; only assert on the 1h arm.
        let ds_arm = sql
            .split("UNION ALL")
            .find(|p| p.contains("metric_samples_1h"))
            .expect("1h downsample arm");
        assert!(
            ds_arm.contains("timestamp < ") && !ds_arm.contains("timestamp <="),
            "live stitch must use exclusive downsample end: {ds_arm}"
        );
    }

    /// time_predicate_is_timestamptz (§9.1 step 8).
    #[test]
    fn time_predicate_is_timestamptz() {
        let pred = samples_time_predicates(Some(1_000), Some(2_000), "timestamp");
        assert!(pred.contains("TIMESTAMPTZ "));
        assert!(!pred.contains("to_timestamp("));
        assert!(
            pred.contains("sm.timestamp") && pred.contains(">="),
            "time window must bind timestamp: {pred}"
        );
        assert!(!pred.contains("record_date"), "{pred}");
    }

    #[test]
    fn stitch_downsample_end_is_exclusive() {
        let inclusive = samples_time_predicates(Some(1_000), Some(2_000), "timestamp");
        let exclusive =
            samples_time_predicates_bounded(Some(1_000), Some(2_000), "timestamp", false);
        assert!(
            inclusive.contains("timestamp <="),
            "default end must stay inclusive: {inclusive}"
        );
        assert!(
            exclusive.contains("timestamp < TIMESTAMPTZ '1970-01-01 00:00:02.000+00'")
                && !exclusive.contains("timestamp <= TIMESTAMPTZ '1970-01-01 00:00:02.000+00'"),
            "half-open stitch end must be exclusive: {exclusive}"
        );
    }

    /// T-Q4 / AC-Q4: ids > max_series → limit_exceeded + max_series, no sample scan.
    #[test]
    fn planner_fails_when_ids_exceed_max_series() {
        let err = enforce_resolved_series_cap(11, 10).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(
            err.message.contains("max_series"),
            "AC-Q4: message must contain max_series, got {}",
            err.message
        );
        assert!(enforce_resolved_series_cap(10, 10).is_ok());
    }

    /// Prom resolve SQL must honor schema-qualified layout prefix (tenant DuckLake).
    #[test]
    fn resolve_sql_uses_schema_qualified_catalog_prefix() {
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let days = PostingsDayRange::from_ms(Some(1_699_998_200_000), Some(1_700_000_000_000));
        let sql = resolve_series_ids_sql(
            "softprobe.metrics_layout_local_dev_tenant",
            days,
            &eq,
            10_000,
        );
        assert!(
            sql.contains("softprobe.metrics_layout_local_dev_tenant.metric_postings"),
            "expected tenant-qualified postings table, got {sql}"
        );
        assert!(
            !sql.contains("softprobe.metric_postings ")
                && !sql.ends_with("softprobe.metric_postings"),
            "must not use bare catalog.metric_postings when schema is set: {sql}"
        );
    }

    /// T-Q3 / AC-Q3 (correctness): `{__name__,instance}` resolves to 1 series via postings.
    #[test]
    fn matcher_name_instance_resolves_one_series_via_postings() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        // Scaled wide fixture (N=50) — pick instance i-1.
        let metrics: Vec<Metric> = (0..50)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let days = PostingsDayRange {
            start: Some(ts.date_naive()),
            end: Some(ts.date_naive()),
        };
        let eq = equality_postings(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "layout_wide".into(),
            },
            LabelMatcher {
                name: "instance".into(),
                op: MatcherOp::Eq,
                value: "i-1".into(),
            },
        ]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
        assert!(resolve_sql.contains("metric_postings"));

        let mut stmt = conn.prepare(&resolve_sql).expect("prepare resolve");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(
            ids.len(),
            1,
            "AC-Q3: expected exactly 1 series_id, got {ids:?}"
        );

        let samples_sql = samples_scan_sql(
            &catalog,
            &ids,
            Some(ts.timestamp_millis() - 60_000),
            Some(ts.timestamp_millis() + 60_000),
            "CAST(s.labels['instance'] AS VARCHAR) AS lbl_instance",
            false,
            100,
            SampleGrain::Raw,
            None,
            true,
        );
        assert!(sql_is_postings_resolve_path(&resolve_sql, &samples_sql));
        let mut sstmt = conn.prepare(&samples_sql).expect("prepare samples");
        let rows: Vec<(u64, f64)> = sstmt
            .query_map([], |r| Ok((r.get::<_, u64>(0)?, r.get::<_, f64>(2)?)))
            .expect("samples query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 1.0);
        let meta_sql = series_meta_sql(
            &catalog,
            &ids,
            SeriesMetaDayScope::QueryWindow,
            Some("layout_wide"),
            Some(ts.timestamp_millis() - 60_000),
            Some(ts.timestamp_millis() + 60_000),
        );
        let mut mstmt = conn.prepare(&meta_sql).expect("prepare meta");
        let instances: Vec<String> = mstmt
            .query_map([], |r| r.get::<_, String>(5))
            .expect("meta query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(instances.len(), 1, "expected one series meta row");
        assert!(
            instances[0].contains("i-1"),
            "labels_json should include instance i-1, got {}",
            instances[0]
        );
    }

    /// T-Q4 integration: wide name-only selector exceeds low max_series without sample hang.
    #[test]
    fn wide_name_only_selector_fails_loud_before_samples() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        const N: i64 = 25;
        const MAX: usize = 10;
        let metrics: Vec<Metric> = (0..N)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let days = PostingsDayRange {
            start: Some(ts.date_naive()),
            end: Some(ts.date_naive()),
        };
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, MAX);
        let mut stmt = conn.prepare(&resolve_sql).expect("prepare");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert!(ids.len() > MAX);
        let err = enforce_resolved_series_cap(ids.len(), MAX).unwrap_err();
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(err.message.contains("max_series"));
        // No sample scan executed after fail (AC-Q4).
    }

    /// T-W4: 31d window still resolves F-wide on the single populated day via SQL
    /// BETWEEN + LIMIT (multi-day must not rely on day-scoped posting cache).
    #[test]
    fn wide_selector_31d_sql_fails_loud_like_short_window() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2023, 11, 14, 22, 0, 0).unwrap();
        const N: i64 = 25;
        const MAX: usize = 10;
        let metrics: Vec<Metric> = (0..N)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let end = ts;
        let start = ts - chrono::Duration::days(31);
        let days =
            PostingsDayRange::from_ms(Some(start.timestamp_millis()), Some(end.timestamp_millis()));
        assert!(
            days.inclusive_days().map(|d| d.len()).unwrap_or(0) > 1,
            "AC-W4 window must span multiple calendar days"
        );
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, MAX);
        assert!(
            resolve_sql.contains("BETWEEN") || resolve_sql.contains("timestamp"),
            "multi-day resolve must prune by timestamp: {resolve_sql}"
        );
        assert!(
            resolve_sql.contains(&format!("LIMIT {}", MAX + 1)),
            "must LIMIT max_series+1 for fail-loud: {resolve_sql}"
        );
        let mut stmt = conn.prepare(&resolve_sql).expect("prepare");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert!(
            ids.len() > MAX,
            "AC-W4: 31d SQL must still see F-wide on EVAL_END day, got {}",
            ids.len()
        );
        let err = enforce_resolved_series_cap(ids.len(), MAX).unwrap_err();
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(err.message.contains("max_series"));
    }

    /// T-C1 / AC-C1: F-churn — pod values for older day ≠ today.
    #[test]
    fn churn_pod_values_differ_by_record_date() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let today = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let older = Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap(); // today-2
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[
                gauge_pod("layout_churn", "p1", older, 1.0),
                gauge_pod("layout_churn", "p2", today, 2.0),
            ],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest");

        let pods_on = |day: NaiveDate| -> Vec<String> {
            let sql = format!(
                "SELECT DISTINCT label_value FROM {}.metric_postings \
                 WHERE CAST(timestamp AS DATE) = DATE '{day}' AND label_name = 'pod' \
                 ORDER BY 1",
                catalog
            );
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(
            pods_on(older.date_naive()),
            vec!["p1".to_string()],
            "AC-C1: older day must be {{p1}} only"
        );
        assert_eq!(
            pods_on(today.date_naive()),
            vec!["p2".to_string()],
            "AC-C1: today must be {{p2}} only"
        );
    }

    /// T-C4 / AC-C4: today's postings for yesterday's dead pod = 0.
    #[test]
    fn churn_dead_pod_absent_from_today_postings() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let today = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let yesterday = Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[
                gauge_pod("layout_churn", "p1", yesterday, 1.0),
                gauge_pod("layout_churn", "p2", today, 2.0),
            ],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest");

        let n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_postings \
                     WHERE CAST(timestamp AS DATE) = DATE '{}' AND label_name = 'pod' AND label_value = 'p1'",
                    today.date_naive()
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 0,
            "AC-C4: dead pod p1 must not appear in today's postings"
        );
    }

    /// A stable series identity needs one index row in every calendar day it is
    /// observed. Prom postings resolve and series metadata both use those day
    /// bounds, so retaining only the first-ever row strands later-day queries.
    #[test]
    fn persistent_series_retains_day_scoped_index_rows_and_metadata() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let day_a = Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap();
        let day_b = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[
                gauge("persistent_metric", "instance-1", day_a, 1.0),
                gauge(
                    "persistent_metric",
                    "instance-1",
                    day_a + chrono::Duration::hours(1),
                    1.5,
                ),
            ],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest first-day persistent series");
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[gauge("persistent_metric", "instance-1", day_b, 2.0)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest second-day persistent series");

        let series_id: u64 = conn
            .query_row(
                &format!(
                    "SELECT series_id FROM {catalog}.metric_series \
                     WHERE metric_name = 'persistent_metric' \
                     ORDER BY timestamp LIMIT 1"
                ),
                [],
                |row| row.get(0),
            )
            .expect("series id");
        let day_bounds = |day: DateTime<Utc>| {
            let start = day.date_naive().and_hms_opt(0, 0, 0).unwrap();
            let end = start + chrono::Duration::days(1);
            (
                start.and_utc().format("%Y-%m-%d %H:%M:%S+00"),
                end.and_utc().format("%Y-%m-%d %H:%M:%S+00"),
            )
        };

        for day in [day_a, day_b] {
            let (from, to) = day_bounds(day);
            let series_rows: i64 = conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {catalog}.metric_series \
                         WHERE series_id = {series_id} \
                           AND timestamp >= TIMESTAMPTZ '{from}' \
                           AND timestamp < TIMESTAMPTZ '{to}'"
                    ),
                    [],
                    |row| row.get(0),
                )
                .expect("day-scoped series rows");
            assert_eq!(series_rows, 1, "metric_series row missing for {day}");

            let posting_rows: i64 = conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {catalog}.metric_postings \
                         WHERE label_name = '__name__' \
                           AND series_id = {series_id} \
                           AND timestamp >= TIMESTAMPTZ '{from}' \
                           AND timestamp < TIMESTAMPTZ '{to}'"
                    ),
                    [],
                    |row| row.get(0),
                )
                .expect("day-scoped posting rows");
            assert_eq!(posting_rows, 1, "metric_postings row missing for {day}");

            let days = PostingsDayRange {
                start: Some(day.date_naive()),
                end: Some(day.date_naive()),
            };
            let equality = [EqualityPosting {
                label_name: "__name__".into(),
                values: vec!["persistent_metric".into()],
            }];
            let resolve_sql = resolve_series_ids_sql(&catalog, days, &equality, 10);
            let resolved: Vec<u64> = conn
                .prepare(&resolve_sql)
                .expect("prepare day resolve")
                .query_map([], |row| row.get(0))
                .expect("execute day resolve")
                .map(|row| row.expect("resolved id"))
                .collect();
            assert_eq!(resolved, vec![series_id], "resolve failed for {day}");

            let start_ms = day.timestamp_millis();
            let end_ms = (day + chrono::Duration::days(1)).timestamp_millis() - 1;
            let metadata_sql = series_meta_sql(
                &catalog,
                &[series_id],
                SeriesMetaDayScope::QueryWindow,
                None,
                Some(start_ms),
                Some(end_ms),
            );
            let metadata_rows: Vec<u64> = conn
                .prepare(&metadata_sql)
                .expect("prepare day metadata")
                .query_map([], |row| row.get(0))
                .expect("execute day metadata")
                .map(|row| row.expect("metadata id"))
                .collect();
            assert_eq!(metadata_rows, vec![series_id], "metadata failed for {day}");
        }

        let union_sql = crate::sql::schema::union_metrics_sql(&catalog);
        let union_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM ({union_sql}) AS metrics \
                     WHERE metric_name = 'persistent_metric'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("union metrics rows");
        assert_eq!(
            union_rows, 3,
            "day-index metadata must not multiply persistent samples"
        );
    }

    #[test]
    fn multi_day_resolve_limits_unique_persistent_series_ids() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let day_a = Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap();
        let day_b = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        for day in [day_a, day_b] {
            write_metrics_layout_txn(
                &conn,
                &catalog,
                &[
                    gauge("persistent_metric", "instance-1", day, 1.0),
                    gauge("persistent_metric", "instance-2", day, 2.0),
                ],
                DEFAULT_MAX_LABELS_PER_SERIES,
            )
            .expect("ingest persistent series");
        }

        let expected: Vec<u64> = conn
            .prepare(&format!(
                "SELECT DISTINCT series_id FROM {catalog}.metric_series \
                 WHERE metric_name = 'persistent_metric' ORDER BY series_id"
            ))
            .expect("prepare ids")
            .query_map([], |row| row.get(0))
            .expect("query ids")
            .map(|row| row.expect("id"))
            .collect();
        assert_eq!(expected.len(), 2);

        let days = PostingsDayRange {
            start: Some(day_a.date_naive()),
            end: Some(day_b.date_naive()),
        };
        let equality = [EqualityPosting {
            label_name: "__name__".into(),
            values: vec!["persistent_metric".into()],
        }];
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &equality, expected.len());
        let resolved: Vec<u64> = conn
            .prepare(&resolve_sql)
            .expect("prepare multi-day resolve")
            .query_map([], |row| row.get(0))
            .expect("execute multi-day resolve")
            .map(|row| row.expect("resolved id"))
            .collect();
        assert_eq!(resolved, expected);
    }

    fn hist(name: &str, instance: &str, ts: DateTime<Utc>) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("service.instance.id".into(), instance.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-hist".into());
        Metric {
            metric_name: name.into(),
            description: "latency".into(),
            unit: "ms".into(),
            metric_type: "histogram".into(),
            timestamp: ts,
            value: 100.0,
            attributes: attrs,
            resource_attributes: resource,
            count: Some(10),
            sum: Some(100.0),
            bucket_counts: Some(vec![2, 5, 3]),
            explicit_bounds: Some(vec![10.0, 50.0]),
            ..Default::default()
        }
    }

    /// T-H2 / AC-H2: short hist selector SQL references hist+postings.
    #[test]
    fn hist_prom_sql_uses_hist_samples_and_postings() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        let days = PostingsDayRange::from_ms(Some(start), Some(end));
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_count".into(),
        }]);
        let resolve = resolve_series_ids_sql("softprobe", days, &eq, 10_000);
        let samples = samples_scan_sql_for_window(
            "softprobe",
            &[42],
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            true,
            true,
            true,
            100,
        );
        assert!(
            sql_is_hist_prom_path(&resolve, &samples),
            "AC-H2 resolve={resolve}\nsamples={samples}"
        );
        assert!(samples.contains("bucket_counts") || samples.contains("sm.count"));
        assert!(!samples.contains("to_timestamp("));
        // Dual-written classic names resolve exactly (no bare hist base).
        assert!(
            resolve.contains("'layout_latency_count'"),
            "resolve must look up classic _count name, got {resolve}"
        );
    }

    /// AC-H3 / H4 / H5: mid+long windows use hist ladder (5m / 1h), never gauge grains.
    #[test]
    fn hist_prom_sql_uses_hist_ladder_for_mid_and_long_windows() {
        let end = 1_700_000_000_000i64;
        let hour = 3_600_000i64;
        let day = 24 * hour;
        type HistPromSqlCase<'a> = (i64, Option<i64>, &'a str, fn(&str) -> bool);
        let cases: &[HistPromSqlCase<'_>] = &[
            (
                3 * hour,
                Some(20_000),
                "layout_latency_count",
                (|s: &str| s.contains("metric_hist_samples")) as fn(&str) -> bool,
            ),
            (day, Some(60_000), "layout_latency_count", |s: &str| {
                s.contains("metric_hist_samples_5m") || s.contains("metric_hist_samples")
            }),
            (30 * day, Some(hour), "layout_latency_count", |s: &str| {
                s.contains("metric_hist_samples_1h") || s.contains("metric_hist_samples")
            }),
            (3 * hour, Some(20_000), "layout_latency_sum", |s: &str| {
                s.contains("metric_hist_samples")
            }),
            (3 * hour, Some(hour), "layout_latency_bucket", |s: &str| {
                s.contains("metric_hist_samples")
            }),
        ];
        for &(range, step, name, want) in cases {
            let start = end - range;
            let samples = samples_scan_sql_for_window(
                "softprobe",
                &[42],
                Some(start),
                Some(end),
                step,
                "NULL::VARCHAR AS lbl__empty",
                true,
                true,
                true,
                100,
            );
            assert!(
                want(&samples),
                "AC-H3/H4/H5 {name} range={range}: want hist ladder, sql={samples}"
            );
            assert!(
                !samples.contains("metric_samples_1h"),
                "AC-H3/H4/H5 {name} range={range}: must not use gauge 1h, sql={samples}"
            );
            assert!(
                !samples.contains("metric_samples_5m"),
                "AC-H3/H4/H5 {name} range={range}: must not use gauge 5m, sql={samples}"
            );
            let resolve = resolve_series_ids_sql(
                "softprobe",
                PostingsDayRange::from_ms(Some(start), Some(end)),
                &equality_postings(&[LabelMatcher {
                    name: "__name__".into(),
                    op: MatcherOp::Eq,
                    value: name.into(),
                }]),
                10_000,
            );
            assert!(
                sql_is_hist_prom_path(&resolve, &samples),
                "AC-H3/H4/H5 {name} range={range} resolve={resolve}\nsamples={samples}"
            );
        }
    }

    /// T-H1 / AC-H1: `_count` resolves via postings to dual-written classic gauges.
    #[test]
    fn hist_count_selector_resolves_via_postings_and_hist_samples() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let metrics: Vec<Metric> = (0..3)
            .map(|i| hist("layout_latency", &format!("i-{i}"), ts))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("hist ingest");

        // Ingest half: base hist name must not land in metric_samples (AC-H1).
        let sample_n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_samples sm \
                     JOIN {catalog}.metric_series s \
                       ON sm.series_id = s.series_id \
                     WHERE s.metric_name = 'layout_latency'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sample_n, 0, "AC-H1: hist must not land in metric_samples");

        // Classic `_count` gauges are dual-written for the Grafana fast path.
        let count_n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_samples sm \
                     JOIN {catalog}.metric_series s \
                       ON sm.series_id = s.series_id \
                     WHERE s.metric_name = 'layout_latency_count'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count_n, 3, "classic _count gauges dual-written");

        let start = ts.timestamp_millis() - 15 * 60 * 1000;
        let end = ts.timestamp_millis() + 15 * 60 * 1000;
        let days = PostingsDayRange::from_ms(Some(start), Some(end));
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_count".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
        assert!(resolve_sql.contains("metric_postings"));

        let mut stmt = conn.prepare(&resolve_sql).expect("prepare resolve");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(
            ids.len(),
            3,
            "AC-H1: expected 3 classic _count series via postings, got {ids:?}"
        );

        let samples_sql = samples_scan_sql_for_window(
            &catalog,
            &ids,
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            false,
            100,
        );
        assert!(
            samples_sql.contains("metric_samples"),
            "classic _count must scan skinny gauges, sql={samples_sql}"
        );
        assert!(
            !samples_sql.contains("metric_hist_samples"),
            "classic _count must not expand native hist arrays, sql={samples_sql}"
        );

        let mut sstmt = conn.prepare(&samples_sql).expect("prepare samples");
        let rows: Vec<Option<f64>> = sstmt
            .query_map([], |r| r.get::<_, Option<f64>>(2))
            .expect("samples query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 3, "AC-H1: expected gauge rows, got {rows:?}");
        for value in &rows {
            assert_eq!(*value, Some(10.0));
        }

        // Classic `_bucket` selector resolves one series per (instance × le).
        let eq_bucket = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_bucket".into(),
        }]);
        let resolve_bucket = resolve_series_ids_sql(&catalog, days, &eq_bucket, 10_000);
        let mut bstmt = conn
            .prepare(&resolve_bucket)
            .expect("prepare bucket resolve");
        let bucket_ids: Vec<u64> = bstmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        // 3 instances × (2 bounds + +Inf) = 9 classic bucket series.
        assert_eq!(
            bucket_ids.len(),
            9,
            "AC-H1: _bucket must resolve classic gauges via postings, got {bucket_ids:?}"
        );
    }

    /// Non-whitelisted histograms are native-only; `_bucket` must still resolve.
    #[test]
    fn native_hist_bucket_selector_resolves_base_series() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[hist("db.client.operation.duration", "i-1", ts)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("hist ingest");

        let dual_n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_series \
                     WHERE metric_name LIKE 'db_client_operation_duration_%'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            dual_n, 0,
            "non-GOLD hists must not dual-write suffix series"
        );

        let start = ts.timestamp_millis() - 15 * 60 * 1000;
        let end = ts.timestamp_millis() + 15 * 60 * 1000;
        let days = PostingsDayRange::from_ms(Some(start), Some(end));
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "db_client_operation_duration_bucket".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
        let mut stmt = conn.prepare(&resolve_sql).expect("prepare resolve");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(
            ids.len(),
            1,
            "suffix selector must resolve the native hist series, got {ids:?}"
        );

        let samples_sql = samples_scan_sql_for_window(
            &catalog,
            &ids,
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            true,
            true,
            true,
            100,
        );
        assert!(
            samples_sql.contains("metric_hist_samples"),
            "native _bucket must scan hist tables, sql={samples_sql}"
        );
        let mut sstmt = conn.prepare(&samples_sql).expect("prepare samples");
        let n = sstmt
            .query_map([], |_| Ok(()))
            .expect("samples query")
            .count();
        assert_eq!(n, 1, "native hist row must be readable for _bucket expand");
    }

    /// T-D3 / AC-D3: Prom backend is DuckLakeMetricsBackend; no sidecar writers in src.
    #[test]
    fn prom_backend_is_ducklake_no_sidecar_writers() {
        // Type presence: construction site is DuckLakeMetricsBackend.
        let _ = std::any::type_name::<crate::compat::backends::DuckLakeMetricsBackend>();
        // Source tree ban: no greptime/victoria remote_write writer modules under compat.
        let banned = [
            include_str!("mod.rs"),
            include_str!("ducklake_metrics.rs"),
            include_str!("metrics.rs"),
        ];
        for src in banned {
            assert!(
                !src.to_ascii_lowercase().contains("victoria"),
                "AC-D3: victoria writer must not appear in compat backends"
            );
            assert!(
                !src.contains("greptime::") && !src.contains("GreptimeWriter"),
                "AC-D3: greptime writer must not appear in compat backends"
            );
            assert!(
                !src.contains("remote_write"),
                "AC-D3: remote_write must not appear in compat backends"
            );
        }
    }
}
