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
pub fn classify_sql_kind(sql: &str) -> &'static str {
    let s = sql.to_ascii_lowercase();
    if s.contains("promotion_specs") {
        "promotion_specs"
    } else if s.contains("variant") {
        "variant_stats"
    } else if s.contains("scores") {
        "scores"
    } else if s.contains("logs") {
        "logs"
    } else if s.contains("traces") {
        "traces"
    } else {
        "other"
    }
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
            classify_sql_kind("SELECT * FROM softprobe.promotion_specs"),
            "promotion_specs"
        );
        assert_eq!(
            classify_sql_kind("SELECT * FROM softprobe.t.logs WHERE"),
            "logs"
        );
        assert_eq!(
            classify_sql_kind("SELECT * FROM softprobe.t.traces WHERE"),
            "traces"
        );
        assert_eq!(
            classify_sql_kind("SELECT * FROM softprobe.t.scores WHERE"),
            "scores"
        );
        assert_eq!(
            classify_sql_kind("SELECT count(*) FROM ducklake_table_info"),
            "other"
        );
    }
}
