//! Event-time helpers shared by writers and query (D2).

use chrono::{DateTime, NaiveDate, Utc};

/// Sole assignment path for calendar day of event time (hive partition keys).
///
/// DuckLake partitions by `year/month/day(timestamp)` — no `record_date` column.
pub fn partition_day_from_event_time(ts: DateTime<Utc>) -> NaiveDate {
    ts.date_naive()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_day_is_utc_date_of_timestamp() {
        let ts = DateTime::parse_from_rfc3339("2026-09-10T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            partition_day_from_event_time(ts),
            NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
        );
    }
}
