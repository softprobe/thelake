//! Cardinality-safe label helpers and SQL kind classification.

use dashmap::DashMap;
use once_cell::sync::Lazy;
use opentelemetry::KeyValue;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Max distinct `app` label values (OTLP service.name); overflow → `_other`.
pub const MAX_APP_CARDINALITY: usize = 64;

static APP_KEYS: Lazy<DashMap<String, ()>> = Lazy::new(DashMap::new);
static APP_OTHER: AtomicUsize = AtomicUsize::new(0);

pub fn attrs(pairs: &[(&str, &str)]) -> Vec<KeyValue> {
    pairs
        .iter()
        .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
        .collect()
}

/// Bound customer `service.name` for ops metrics.
pub fn bound_app(raw: Option<&str>) -> String {
    let name = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("_none");
    if APP_KEYS.contains_key(name) {
        return name.to_string();
    }
    if APP_KEYS.len() < MAX_APP_CARDINALITY {
        APP_KEYS.insert(name.to_string(), ());
        return name.to_string();
    }
    APP_OTHER.fetch_add(1, Ordering::Relaxed);
    "_other".to_string()
}

#[cfg(test)]
pub fn reset_app_cardinality_for_test() {
    APP_KEYS.clear();
    APP_OTHER.store(0, Ordering::Relaxed);
}

/// Fixed sql_kind enum for query instrumentation.
///
/// Prefer more specific grain / table tokens before bare `metric_samples` so
/// ops metrics can show whether long windows hit downsample or raw.
///
/// Live Grafana windows (`end ≈ now`) emit `UNION ALL` of downsample history +
/// raw lag. Those must not collapse into plain `metric_samples_*` — otherwise
/// avg latency by sql_kind cannot distinguish archive downsample from the
/// live ladder (the previous hotspot was live long-range scanning raw only).
pub fn classify_sql_kind(sql: &str) -> &'static str {
    let s = sql.to_ascii_lowercase();
    let live_union = s.contains("union all");
    // Explicit hints from series_meta_sql (before bare metric_series).
    if s.contains("thelake_series_meta_recent") {
        return "metric_series_recent";
    }
    if s.contains("thelake_series_meta_all") {
        return "metric_series_all";
    }
    if s.contains("metric_postings") {
        "metric_postings"
    } else if s.contains("metric_series") {
        "metric_series"
    } else if s.contains("metric_hist_samples_1h") {
        if live_union {
            "metric_hist_samples_1h_union"
        } else {
            "metric_hist_samples_1h"
        }
    } else if s.contains("metric_hist_samples_5m") {
        if live_union {
            "metric_hist_samples_5m_union"
        } else {
            "metric_hist_samples_5m"
        }
    } else if s.contains("metric_hist_samples") {
        "metric_hist_samples"
    } else if s.contains("metric_samples_1h") {
        if live_union {
            "metric_samples_1h_union"
        } else {
            "metric_samples_1h"
        }
    } else if s.contains("metric_samples_5m") {
        if live_union {
            "metric_samples_5m_union"
        } else {
            "metric_samples_5m"
        }
    } else if s.contains("metric_samples") {
        "metric_samples"
    } else if s.contains("metric_collapse_job_1h") || s.contains("union_metrics") {
        if s.contains("metric_collapse_job_1h") {
            "metric_collapse_job_1h"
        } else {
            "union_metrics"
        }
    } else if s.contains("promotion_specs") {
        "promotion_specs"
    } else if s.contains("variant") {
        "variant_stats"
    } else if s.contains("logs") {
        "logs"
    } else if s.contains("traces") {
        "traces"
    } else {
        "other"
    }
}

/// Physical sample-scan plan for self-monitoring (grain + mode).
///
/// `scan_mode`:
/// - `raw` — full window on raw/hist raw tables
/// - `downsample` — closed/archive window on 5m/1h only
/// - `downsample_with_raw_tail` — live window: downsample history + raw lag
pub fn classify_sample_scan(sql: &str) -> (&'static str, &'static str) {
    let s = sql.to_ascii_lowercase();
    let live_union = s.contains("union all");
    let grain = if s.contains("metric_hist_samples_1h") {
        "metric_hist_samples_1h"
    } else if s.contains("metric_hist_samples_5m") {
        "metric_hist_samples_5m"
    } else if s.contains("metric_hist_samples") {
        "metric_hist_samples"
    } else if s.contains("metric_samples_1h") {
        "metric_samples_1h"
    } else if s.contains("metric_samples_5m") {
        "metric_samples_5m"
    } else if s.contains("metric_samples") {
        "metric_samples"
    } else {
        "other"
    };
    let scan_mode = if live_union && grain != "metric_samples" && grain != "metric_hist_samples" {
        "downsample_with_raw_tail"
    } else if matches!(
        grain,
        "metric_samples_1h"
            | "metric_samples_5m"
            | "metric_hist_samples_1h"
            | "metric_hist_samples_5m"
    ) {
        "downsample"
    } else if grain == "other" {
        "other"
    } else {
        "raw"
    };
    (grain, scan_mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_overflow_to_other() {
        reset_app_cardinality_for_test();
        for i in 0..MAX_APP_CARDINALITY {
            let a = bound_app(Some(&format!("svc-{i}")));
            assert_eq!(a, format!("svc-{i}"));
        }
        assert_eq!(bound_app(Some("overflow-app")), "_other");
        // Existing key still resolves.
        assert_eq!(bound_app(Some("svc-0")), "svc-0");
    }

    #[test]
    fn sql_kind_classifier() {
        assert_eq!(
            classify_sql_kind("SELECT DISTINCT series_id FROM softprobe.t.metric_postings WHERE"),
            "metric_postings"
        );
        assert_eq!(
            classify_sql_kind("SELECT sm.series_id FROM softprobe.t.metric_samples sm WHERE"),
            "metric_samples"
        );
        assert_eq!(
            classify_sql_kind(
                "SELECT * FROM softprobe.t.metric_hist_samples WHERE record_date = today()"
            ),
            "metric_hist_samples"
        );
        assert_eq!(
            classify_sql_kind("SELECT last FROM softprobe.t.metric_samples_1h sm WHERE"),
            "metric_samples_1h"
        );
        assert_eq!(
            classify_sql_kind("SELECT last FROM softprobe.t.metric_samples_5m sm WHERE"),
            "metric_samples_5m"
        );
        assert_eq!(
            classify_sql_kind(
                "(SELECT * FROM softprobe.t.metric_samples sm) UNION ALL \
                 (SELECT * FROM softprobe.t.metric_samples_1h sm)"
            ),
            "metric_samples_1h_union"
        );
        assert_eq!(
            classify_sql_kind(
                "SELECT /* thelake_series_meta_recent */ s.series_id FROM softprobe.t.metric_series s"
            ),
            "metric_series_recent"
        );
        assert_eq!(
            classify_sql_kind(
                "SELECT /* thelake_series_meta_all */ s.series_id FROM softprobe.t.metric_series s"
            ),
            "metric_series_all"
        );
        assert_eq!(
            classify_sql_kind("SELECT 1 FROM union_metrics LIMIT 1"),
            "union_metrics"
        );
        assert_eq!(
            classify_sql_kind("SELECT * FROM softprobe.promotion_specs"),
            "promotion_specs"
        );
    }

    #[test]
    fn sample_scan_classifier_separates_live_union_from_raw() {
        assert_eq!(
            classify_sample_scan(
                "SELECT last FROM softprobe.t.metric_samples_1h sm WHERE window_ts < ..."
            ),
            ("metric_samples_1h", "downsample")
        );
        assert_eq!(
            classify_sample_scan(
                "(SELECT * FROM softprobe.t.metric_samples sm WHERE ...) UNION ALL \
                 (SELECT * FROM softprobe.t.metric_samples_1h sm WHERE ...)"
            ),
            ("metric_samples_1h", "downsample_with_raw_tail")
        );
        assert_eq!(
            classify_sample_scan(
                "SELECT value FROM softprobe.t.metric_samples sm WHERE series_id IN (1)"
            ),
            ("metric_samples", "raw")
        );
    }
}
