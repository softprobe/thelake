//! Required event-time window that injects bare predicates for partition pruning.

use chrono::{DateTime, NaiveDate, Utc};

use crate::sql::literal::timestamp_ns_literal;

/// Finite event-time range. Sole lake time window type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// SQL assembled with the required bare timestamp predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampFilteredSql {
    sql: String,
}

impl TimestampFilteredSql {
    pub fn as_str(&self) -> &str {
        &self.sql
    }

    pub fn into_sql(self) -> String {
        self.sql
    }
}

impl QueryWindow {
    pub fn try_new(from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Self, String> {
        if from > to {
            return Err("`from` must be <= `to`".to_string());
        }
        Ok(Self { from, to })
    }

    /// Bare event-time predicate for every DuckLake fact table.
    ///
    /// `alias` is a column prefix such as `"c."` or `""`.
    ///
    /// **Must stay bare `timestamp` comparisons.** Wrapping in
    /// `make_timestamp_ns(epoch_ns(...))` disables DuckLake year/month/day
    /// partition prune (greenfield EXPLAIN in `one_clock_prune`: bare → 1 file,
    /// wrap → more than one day file).
    /// See `docs/fixtures/one-clock-prune-explain.md`.
    pub fn timestamp_filter_sql(&self, alias: &str) -> String {
        let col = format!("{alias}timestamp");
        format!(
            "{col} >= {} AND {col} <= {}",
            timestamp_ns_literal(&self.from),
            timestamp_ns_literal(&self.to)
        )
    }

    /// Assemble SQL with the bare TIMESTAMP_NS predicate.
    pub fn scan_with_timestamp_filter(
        self,
        alias: &str,
        assemble: impl FnOnce(&str) -> String,
    ) -> TimestampFilteredSql {
        let filter = self.timestamp_filter_sql(alias);
        let sql = assemble(&filter);
        assert_filter_kept(&sql, &filter);
        TimestampFilteredSql { sql }
    }

    /// Narrow to one calendar day as a *timestamp* sub-window (never emit DATE columns).
    pub fn scan_with_day_filter(
        self,
        day: NaiveDate,
        alias: &str,
        assemble: impl FnOnce(&str) -> String,
    ) -> TimestampFilteredSql {
        let day_start = day.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
        let next_midnight = (day + chrono::Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .expect("next midnight")
            .and_utc();
        let day_end = next_midnight - chrono::Duration::nanoseconds(1);
        let from = self.from.max(day_start);
        let to = self.to.min(day_end);
        let window = if from <= to {
            QueryWindow { from, to }
        } else {
            // Non-overlapping: emit an empty-point predicate so the scan stays time-gated.
            QueryWindow {
                from: day_start,
                to: day_start,
            }
        };
        window.scan_with_timestamp_filter(alias, assemble)
    }
}

fn assert_filter_kept(sql: &str, filter: &str) {
    assert!(
        sql.contains(filter),
        "assemble closure dropped timestamp filter fragment"
    );
    for forbidden in ["record_date", "event_date", "window_ts"] {
        assert!(
            !sql.contains(forbidden),
            "forbidden time column `{forbidden}` in timestamp-filtered SQL"
        );
    }
}

/// Map exclusive-end ns window → [`QueryWindow`].
pub fn query_window_from_exclusive_ns(
    start_ns: i64,
    end_ns_exclusive: i64,
) -> Result<QueryWindow, String> {
    if start_ns >= end_ns_exclusive {
        return Err("`start` must be < `end`".to_string());
    }
    let from = DateTime::<Utc>::from_timestamp_nanos(start_ns);
    let to = DateTime::<Utc>::from_timestamp_nanos(end_ns_exclusive - 1);
    QueryWindow::try_new(from, to)
}

/// Push identity predicates plus the required bare `timestamp` predicate.
/// Prefer [`QueryWindow::scan_with_timestamp_filter`] for new recipes.
pub fn push_otlp_time_predicates(
    conditions: &mut Vec<String>,
    window: &QueryWindow,
    identity: impl IntoIterator<Item = String>,
) {
    conditions.extend(identity);
    conditions.push(window.timestamp_filter_sql(""));
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

/// Assert SQL embeds both required OTLP timestamp predicates (no day columns).
///
/// Product bounds must be **bare** `timestamp >=` / `<=`. The
/// `make_timestamp_ns(epoch_ns(timestamp))` wrap disables DuckLake day prune
/// (greenfield EXPLAIN in `one_clock_prune`: wrap opens more than one day file).
#[cfg(test)]
pub(crate) fn assert_sql_has_otlp_time_predicates(sql: &str) {
    assert!(
        sql.contains("timestamp >="),
        "missing event-time lower bound: {sql}"
    );
    assert!(
        sql.contains("timestamp <="),
        "missing event-time upper bound: {sql}"
    );
    assert!(
        !sql.contains("make_timestamp_ns(epoch_ns(timestamp))"),
        "wrapped timestamp predicate breaks day prune: {sql}"
    );
    for bad in ["record_date", "event_date", "window_ts"] {
        assert!(!sql.contains(bad), "forbidden time column {bad}: {sql}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample() -> QueryWindow {
        QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 9, 10, 16, 5, 15).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 10, 16, 45, 48).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn rejects_inverted() {
        let from = Utc.with_ymd_and_hms(2026, 9, 11, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 9, 10, 0, 0, 0).unwrap();
        assert!(QueryWindow::try_new(from, to).is_err());
    }

    #[test]
    fn scan_with_timestamp_filter_embeds_bare_predicate() {
        let sql = sample()
            .scan_with_timestamp_filter("c.", |filter| {
                format!("SELECT 1 FROM t c WHERE c.id = 1 AND {filter}")
            })
            .into_sql();
        assert!(sql.contains("c.timestamp >="));
        assert!(
            !sql.contains("make_timestamp_ns(epoch_ns("),
            "wrapped timestamp predicates break day prune"
        );
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
    }

    #[test]
    #[should_panic(expected = "dropped timestamp filter")]
    fn scan_with_timestamp_filter_panics_if_filter_dropped() {
        let _ = sample().scan_with_timestamp_filter("", |_bound| "SELECT 1 FROM traces".into());
    }

    #[test]
    fn scores_use_the_same_timestamp_ns_literal() {
        let sql = sample()
            .scan_with_timestamp_filter("", |filter| format!("SELECT 1 FROM scores WHERE {filter}"))
            .into_sql();
        assert!(sql.contains("::TIMESTAMP_NS"));
        assert!(!sql.contains("TIMESTAMPTZ"));
        assert!(!sql.contains("make_timestamp_ns(epoch_ns("));
    }

    #[test]
    fn scan_with_day_filter_narrows_to_calendar_day_as_timestamp() {
        let w = QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 9, 10, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap(),
        )
        .unwrap();
        let sql = w
            .scan_with_day_filter(
                NaiveDate::from_ymd_opt(2026, 9, 11).unwrap(),
                "",
                |filter| format!("SELECT * FROM t WHERE {filter}"),
            )
            .into_sql();
        assert!(sql.contains("2026-09-11"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("DATE '"));
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
        use std::fs;
        use std::path::Path;

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
