//! Required event-time window that injects bare predicates for partition pruning.

use chrono::{DateTime, NaiveDate, Utc};

use crate::sql::literal::{timestamp_ns_literal, timestamptz_literal};

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

    /// Event-time predicate for **TIMESTAMP_NS** tables (`traces` / `logs`).
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

    /// Event-time predicate for **TIMESTAMPTZ** tables (`scores`).
    ///
    /// Same bare-column rule as [`Self::timestamp_filter_sql`] — do not wrap.
    /// Scores use the microsecond/TIMESTAMPTZ family (`storage::schema::tables`).
    pub fn timestamptz_filter_sql(&self, alias: &str) -> String {
        let col = format!("{alias}timestamp");
        format!(
            "{col} >= {} AND {col} <= {}",
            timestamptz_literal(&self.from),
            timestamptz_literal(&self.to)
        )
    }

    /// Assemble SQL with the bare TIMESTAMP_NS predicate (traces/logs).
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

    /// Assemble SQL over **scores** only (TIMESTAMPTZ clock).
    pub fn scan_with_timestamptz_filter(
        self,
        alias: &str,
        assemble: impl FnOnce(&str) -> String,
    ) -> TimestampFilteredSql {
        let filter = self.timestamptz_filter_sql(alias);
        let sql = assemble(&filter);
        assert_filter_kept(&sql, &filter);
        TimestampFilteredSql { sql }
    }

    /// Assemble SQL that touches both TIMESTAMP_NS (`traces`/`logs`) and
    /// TIMESTAMPTZ (`scores`) storage — each scan gets its matching bare predicate.
    pub fn scan_with_both_timestamp_filters(
        self,
        alias: &str,
        assemble: impl FnOnce(/* timestamp_ns */ &str, /* timestamptz */ &str) -> String,
    ) -> TimestampFilteredSql {
        let ns = self.timestamp_filter_sql(alias);
        let tz = self.timestamptz_filter_sql(alias);
        let sql = assemble(&ns, &tz);
        assert_filter_kept(&sql, &ns);
        assert_filter_kept(&sql, &tz);
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
    fn scan_with_timestamptz_filter_uses_tz_literals() {
        let sql = sample()
            .scan_with_timestamptz_filter("", |filter| {
                format!("SELECT 1 FROM scores WHERE {filter}")
            })
            .into_sql();
        assert!(sql.contains("TIMESTAMPTZ '"));
        assert!(!sql.contains("::TIMESTAMP_NS"));
        assert!(!sql.contains("make_timestamp_ns(epoch_ns("));
    }

    #[test]
    fn scan_with_both_timestamp_filters_embeds_both_clocks() {
        let sql = sample()
            .scan_with_both_timestamp_filters("", |ns, tz| {
                format!(
                    "SELECT 1 FROM scores WHERE {tz} AND EXISTS (SELECT 1 FROM traces WHERE {ns})"
                )
            })
            .into_sql();
        assert!(sql.contains("::TIMESTAMP_NS"));
        assert!(sql.contains("TIMESTAMPTZ '"));
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
}
