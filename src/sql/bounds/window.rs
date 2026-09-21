//! Required event-time window + BoundLakeSql (one clock: `timestamp` only).

use chrono::{DateTime, NaiveDate, Utc};

use crate::sql::literal::{timestamp_ns_column, timestamp_ns_literal};

/// Finite event-time range. Sole lake time window type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// SQL that was assembled under a bound `timestamp` fragment (cannot omit the bound).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundLakeSql {
    sql: String,
}

impl BoundLakeSql {
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

    /// Event-time predicate fragment: `{alias}timestamp` lower/upper only.
    ///
    /// `alias` is a column prefix such as `"c."` or `""`.
    pub fn timestamp_bound_sql(&self, alias: &str) -> String {
        let col = timestamp_ns_column(&format!("{alias}timestamp"));
        format!(
            "{col} >= {} AND {col} <= {}",
            timestamp_ns_literal(&self.from),
            timestamp_ns_literal(&self.to)
        )
    }

    /// Assemble SQL that must embed the window's `timestamp` bound fragment.
    pub fn bind_scan(self, alias: &str, assemble: impl FnOnce(&str) -> String) -> BoundLakeSql {
        let bound = self.timestamp_bound_sql(alias);
        let sql = assemble(&bound);
        assert_bound_kept(&sql, &bound);
        BoundLakeSql { sql }
    }

    /// Narrow to one calendar day as a *timestamp* sub-window (never emit DATE columns).
    pub fn bind_day(
        self,
        day: NaiveDate,
        alias: &str,
        assemble: impl FnOnce(&str) -> String,
    ) -> BoundLakeSql {
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
            // Non-overlapping: emit an empty-point bound so the scan stays time-gated.
            QueryWindow {
                from: day_start,
                to: day_start,
            }
        };
        window.bind_scan(alias, assemble)
    }
}

fn assert_bound_kept(sql: &str, bound: &str) {
    assert!(
        sql.contains(bound),
        "assemble closure dropped timestamp bound fragment"
    );
    for forbidden in ["record_date", "event_date", "window_ts"] {
        assert!(
            !sql.contains(forbidden),
            "forbidden time column `{forbidden}` in bound SQL"
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
    fn bind_scan_embeds_timestamp_only() {
        let sql = sample()
            .bind_scan("c.", |bound| {
                format!("SELECT 1 FROM t c WHERE c.id = 1 AND {bound}")
            })
            .into_sql();
        assert!(sql.contains("CAST(c.timestamp AS TIMESTAMP_NS)"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("window_ts"));
    }

    #[test]
    #[should_panic(expected = "dropped timestamp bound")]
    fn bind_scan_panics_if_bound_dropped() {
        let _ = sample().bind_scan("", |_bound| "SELECT 1 FROM traces".into());
    }

    #[test]
    fn bind_day_narrows_to_calendar_day_as_timestamp() {
        let w = QueryWindow::try_new(
            Utc.with_ymd_and_hms(2026, 9, 10, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap(),
        )
        .unwrap();
        let sql = w
            .bind_day(NaiveDate::from_ymd_opt(2026, 9, 11).unwrap(), "", |bound| {
                format!("SELECT * FROM t WHERE {bound}")
            })
            .into_sql();
        assert!(sql.contains("2026-09-11"));
        assert!(!sql.contains("record_date"));
        assert!(!sql.contains("DATE '"));
    }
}
