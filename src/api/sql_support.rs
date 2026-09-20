//! Shared DuckDB SQL-compilation helpers for query modules (e.g. `api/llm/query.rs`).
//!
//! Extracted from `api/llm/query.rs` so literal-escaping, cursors, and time-bound clauses
//! are not copy-pasted across query recipes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Opaque keyset-pagination cursor: `(timestamp, tiebreaker id)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PageCursor {
    pub t: DateTime<Utc>,
    pub id: String,
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Render a nanosecond-precision literal for trace/span timestamp columns.
pub(crate) fn timestamp_ns_literal(value: &DateTime<Utc>) -> String {
    timestamp_ns_literal_from_str(&value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

/// Render a nanosecond-precision literal from an RFC3339 API value.
pub(crate) fn timestamp_ns_literal_from_str(value: &str) -> String {
    format!("{}::TIMESTAMP_NS", sql_string_literal(value))
}

/// Compare a timestamp column to a `TIMESTAMP_NS` literal.
///
/// Prod DuckLake tables may still be `TIMESTAMPTZ` while newer schemas use
/// `TIMESTAMP_NS`. Casting the column makes predicates bind for both.
pub(crate) fn timestamp_ns_column(column: &str) -> String {
    format!("CAST({column} AS TIMESTAMP_NS)")
}

pub(crate) fn encode_cursor(timestamp: DateTime<Utc>, id: &str) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let payload = PageCursor {
        t: timestamp,
        id: id.to_string(),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("cursor json"))
}

pub(crate) fn decode_cursor(cursor: &str) -> Result<PageCursor, String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|_| "malformed cursor".to_string())?;
    serde_json::from_slice(&bytes).map_err(|_| "malformed cursor".to_string())
}

pub(crate) fn cursor_predicate(
    cursor: &str,
    timestamp_col: &str,
    id_col: &str,
) -> Result<String, String> {
    let decoded = decode_cursor(cursor)?;
    let ts_col = timestamp_ns_column(timestamp_col);
    Ok(format!(
        "({ts_col} < {ts} OR ({ts_col} = {ts} AND {id_col} < {id}))",
        ts = timestamp_ns_literal(&decoded.t),
        id = sql_string_literal(&decoded.id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_single_quotes_in_string_literal() {
        assert_eq!(sql_string_literal("it's"), "'it''s'");
    }

    #[test]
    fn cursor_round_trips() {
        let ts = DateTime::parse_from_rfc3339("2026-07-18T23:22:00.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let encoded = encode_cursor(ts, "span-1");
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded.id, "span-1");
        assert_eq!(decoded.t, ts);
        assert!(decode_cursor("%%%not-base64%%%").is_err());
    }

    #[test]
    fn timestamp_ns_literal_preserves_nanoseconds() {
        let timestamp = DateTime::parse_from_rfc3339("2026-07-18T23:22:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            timestamp_ns_literal(&timestamp),
            "'2026-07-18T23:22:00.123456789Z'::TIMESTAMP_NS"
        );
    }
}
