//! Required event-time window for OTLP DuckLake reads.
//!
//! Re-exports [`crate::sql::QueryWindow`]. Prefer `crate::sql` for new code.
//! Emit time SQL via [`QueryWindow::bind_scan`] / [`crate::sql::BoundLakeSql`].

pub use crate::sql::{query_window_from_exclusive_ns, QueryWindow};

/// Push identity predicates plus a required `timestamp` bound (one clock).
/// Prefer [`QueryWindow::bind_scan`] for new recipes.
pub fn push_otlp_time_predicates(
    conditions: &mut Vec<String>,
    window: &QueryWindow,
    identity: impl IntoIterator<Item = String>,
) {
    conditions.extend(identity);
    conditions.push(window.timestamp_bound_sql(""));
}

/// Map exclusive-end ns window → predicates via [`push_otlp_time_predicates`].
pub(crate) fn push_otlp_ns_window_predicates(
    conditions: &mut Vec<String>,
    start_ns: i64,
    end_ns_exclusive: i64,
    identity: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let window = query_window_from_exclusive_ns(start_ns, end_ns_exclusive)?;
    push_otlp_time_predicates(conditions, &window, identity);
    Ok(())
}

/// Assert SQL embeds required OTLP timestamp bounds (no day columns).
#[cfg(test)]
pub(crate) fn assert_sql_has_otlp_time_predicates(sql: &str) {
    assert!(
        sql.contains("make_timestamp_ns(epoch_ns(timestamp)) >=") || sql.contains("timestamp >="),
        "missing event-time lower bound: {sql}"
    );
    assert!(
        sql.contains("make_timestamp_ns(epoch_ns(timestamp)) <=") || sql.contains("timestamp <="),
        "missing event-time upper bound: {sql}"
    );
    for bad in ["record_date", "event_date", "window_ts"] {
        assert!(!sql.contains(bad), "forbidden time column {bad}: {sql}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::fs;
    use std::path::Path;

    fn sample() -> QueryWindow {
        QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 9, 10, 16, 5, 15).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 10, 16, 45, 48).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn rejects_inverted_range() {
        let from = Utc.with_ymd_and_hms(2026, 9, 11, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 9, 10, 0, 0, 0).unwrap();
        assert!(QueryWindow::try_new(from, to).is_err());
    }

    #[test]
    fn push_emits_timestamp_only() {
        let w = sample();
        let mut conditions = Vec::new();
        push_otlp_time_predicates(&mut conditions, &w, ["session_id = 's1'".to_string()]);
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], "session_id = 's1'");
        assert!(conditions[1].contains("timestamp"));
        assert!(!conditions.iter().any(|c| c.contains("record_date")));
        assert_sql_has_otlp_time_predicates(&conditions.join(" AND "));
    }

    #[test]
    fn optional_time_bounds_absent_from_src_tree() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let needle = format!("fn push_{}_time_bounds", "optional");
        let mut hits = Vec::new();
        fn walk(dir: &Path, needle: &str, hits: &mut Vec<String>) {
            for entry in fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, needle, hits);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    let text = fs::read_to_string(&path).unwrap();
                    if text.contains(needle) {
                        hits.push(path.display().to_string());
                    }
                }
            }
        }
        walk(&root, &needle, &mut hits);
        assert!(
            hits.is_empty(),
            "optional time-bounds fn must be deleted from src/; found in {hits:?}"
        );
    }

    #[test]
    fn ns_window_adapter_rejects_inverted_and_maps_exclusive_end() {
        let mut conditions = Vec::new();
        assert!(
            push_otlp_ns_window_predicates(&mut conditions, 10, 10, std::iter::empty()).is_err()
        );
        conditions.clear();
        push_otlp_ns_window_predicates(
            &mut conditions,
            1_700_000_000_000_000_001,
            1_700_000_000_000_000_002,
            ["trace_id = 't'".to_string()],
        )
        .unwrap();
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], "trace_id = 't'");
        assert!(conditions[1].contains("'2023-11-14T22:13:20.000000001Z'::TIMESTAMP_NS"));
        assert!(!conditions[1].contains("record_date"));
    }
}
