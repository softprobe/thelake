//! Event-time and epoch-nanosecond conversion utilities shared across models, storage, and queries.

use chrono::{DateTime, NaiveDate, Utc};

/// Sole assignment path for calendar day of event time (hive partition keys).
///
/// DuckLake partitions by `year/month/day(timestamp)` — no `record_date` column.
pub fn partition_day_from_event_time(ts: DateTime<Utc>) -> NaiveDate {
    ts.date_naive()
}

/// Convert a UTC instant to signed nanoseconds since Unix epoch.
pub fn to_ns(value: DateTime<Utc>) -> i64 {
    value
        .timestamp_nanos_opt()
        .expect("timestamp must fit signed nanoseconds")
}

/// Convert signed nanoseconds since Unix epoch to a UTC instant.
pub fn from_ns(value: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
    .expect("timestamp nanoseconds must be a valid UTC instant")
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

    #[test]
    fn round_trips_submicrosecond_instants() {
        let instant = DateTime::from_timestamp(1_700_000_000, 123).unwrap();
        assert_eq!(from_ns(to_ns(instant)), instant);
    }
}
