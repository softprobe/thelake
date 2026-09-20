//! Required event-time window for OTLP DuckLake reads.
//!
//! Design: [`design-event-time-layout.md`](../../../docs/design-event-time-layout.md).
//! Callers pass only [`QueryWindow`]; partition day is always derived — never a second clock.
//! Emit time SQL **only** via [`push_otlp_time_predicates`].

use super::sql_support::{timestamp_ns_column, timestamp_ns_literal};
use chrono::{DateTime, Utc};

/// Finite event-time range. Partition day bounds are derived from `from`/`to` only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl QueryWindow {
    /// Build a window. Rejects inverted ranges.
    /// Max-span is enforced by callers that need it (e.g. session_summary rebuild), not here.
    pub fn try_new(from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Self, String> {
        if from > to {
            return Err("`from` must be <= `to`".to_string());
        }
        Ok(Self { from, to })
    }

    fn partition_day_predicate(&self) -> String {
        let from_date = self.from.date_naive();
        let to_date = self.to.date_naive();
        format!("{OTLP_PARTITION_DAY_COLUMN} BETWEEN DATE '{from_date}' AND DATE '{to_date}'")
    }

    fn event_time_predicate(&self) -> String {
        let col = timestamp_ns_column(OTLP_EVENT_TIME_COLUMN);
        format!(
            "{col} >= {} AND {col} <= {}",
            timestamp_ns_literal(&self.from),
            timestamp_ns_literal(&self.to)
        )
    }
}

/// Default partition-day column on OTLP tables (legacy spelling of `date(timestamp)`).
pub const OTLP_PARTITION_DAY_COLUMN: &str = "record_date";

/// Default event-time column on OTLP tables.
pub const OTLP_EVENT_TIME_COLUMN: &str = "timestamp";

/// Push predicates in design order: partition day → `identity`… → event time.
/// This is the **only** allowed emitter of OTLP day + event-time bounds.
pub fn push_otlp_time_predicates(
    conditions: &mut Vec<String>,
    window: &QueryWindow,
    identity: impl IntoIterator<Item = String>,
) {
    conditions.push(window.partition_day_predicate());
    conditions.extend(identity);
    conditions.push(window.event_time_predicate());
}

/// Assert SQL embeds the required OTLP time shape (day before timestamp, both sides).
#[cfg(test)]
pub(crate) fn assert_sql_has_otlp_time_predicates(sql: &str) {
    assert!(
        sql.contains("record_date BETWEEN DATE"),
        "missing partition day bound: {sql}"
    );
    assert!(
        sql.contains("CAST(timestamp AS TIMESTAMP_NS) >="),
        "missing event-time lower bound: {sql}"
    );
    assert!(
        sql.contains("CAST(timestamp AS TIMESTAMP_NS) <="),
        "missing event-time upper bound: {sql}"
    );
    let rd = sql.find("record_date BETWEEN").expect("record_date");
    let ts = sql
        .find("CAST(timestamp AS TIMESTAMP_NS)")
        .expect("timestamp");
    assert!(
        rd < ts,
        "partition day must precede event-time predicate: {sql}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
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
    fn query_window_fields_are_only_from_to() {
        // D3: no parallel partition-day field on the window type.
        let w = sample();
        let _ = (w.from, w.to);
        let debug = format!("{w:?}");
        assert!(debug.contains("from"));
        assert!(debug.contains("to"));
        assert!(!debug.contains("record_date"));
        assert!(!debug.contains("event_date"));
    }

    #[test]
    fn partition_day_derived_from_window_only() {
        let w = sample();
        assert_eq!(
            w.partition_day_predicate(),
            "record_date BETWEEN DATE '2026-09-10' AND DATE '2026-09-10'"
        );
    }

    #[test]
    fn multi_day_window_expands_partition_day() {
        let w = QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 9, 10, 23, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 11, 1, 0, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(
            w.partition_day_predicate(),
            "record_date BETWEEN DATE '2026-09-10' AND DATE '2026-09-11'"
        );
    }

    #[test]
    fn push_order_is_day_identity_timestamp() {
        let w = sample();
        let mut conditions = Vec::new();
        push_otlp_time_predicates(&mut conditions, &w, [format!("session_id = 's1'")]);
        assert_eq!(conditions.len(), 3);
        assert_eq!(conditions[0], w.partition_day_predicate());
        assert_eq!(conditions[1], "session_id = 's1'");
        assert_eq!(conditions[2], w.event_time_predicate());
        let joined = conditions.join(" AND ");
        assert_sql_has_otlp_time_predicates(&joined);
    }

    #[test]
    fn equal_day_single_date_partition() {
        let w = sample();
        let day = w.partition_day_predicate();
        assert!(day.contains("DATE '2026-09-10' AND DATE '2026-09-10'"));
    }

    #[test]
    fn optional_time_bounds_absent_from_src_tree() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        // Construct so this test file does not contain the banned identifier as a contiguous fn decl.
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
    fn no_execute_sql_regex_guard_files() {
        let api = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api");
        for name in [
            "sql_guard.rs",
            "unbounded_sql.rs",
            "sql_allowlist.rs",
            "execute_sql_guard.rs",
        ] {
            assert!(
                !api.join(name).exists(),
                "D12: do not add execute-time SQL guard module {name}"
            );
        }
    }
}
