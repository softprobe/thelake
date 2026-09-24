//! Sole SQL string-literal / timestamp literal helpers for lake SQL.

use chrono::{DateTime, Utc};

/// Escape and quote a SQL string literal.
pub fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Nanosecond-precision timestamp literal (`'…'::TIMESTAMP_NS`).
pub fn timestamp_ns_literal(value: &DateTime<Utc>) -> String {
    timestamp_ns_literal_from_str(&value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

/// Nanosecond-precision literal from an RFC3339 string.
pub fn timestamp_ns_literal_from_str(value: &str) -> String {
    format!("{}::TIMESTAMP_NS", sql_string_literal(value))
}

/// Normalize a timestamp column to `TIMESTAMP_NS` for comparisons.
///
/// DuckLake on Postgres may surface event-time columns as `TIMESTAMPTZ` for
/// inlined rows; `CAST(... AS TIMESTAMP_NS)` is unimplemented for that type.
/// `epoch_ns` accepts TIMESTAMP / TIMESTAMPTZ / TIMESTAMP_NS, and
/// `make_timestamp_ns` rebuilds a timezone-free ns clock for comparisons.
pub fn timestamp_ns_column(column: &str) -> String {
    format!("make_timestamp_ns(epoch_ns({column}))")
}

/// Timezone-bearing metric literal. Metrics tables intentionally use
/// `TIMESTAMPTZ`; trace/log tables use [`timestamp_ns_literal`] instead.
pub fn timestamptz_literal(value: &DateTime<Utc>) -> String {
    format!("TIMESTAMPTZ '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f+00"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_single_quotes() {
        assert_eq!(sql_string_literal("it's"), "'it''s'");
    }
}
