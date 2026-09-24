//! Serialization-conflict retry for DuckLake maintenance CALLs.
//!
//! Backoff uses `block_in_place` + thread sleep so the Tokio worker is not
//! blocked without yielding, while DuckDB `Connection` (not `Send`) stays on
//! the same thread across retries.

use tracing::warn;

pub(crate) const COMPACTION_SERIALIZATION_ATTEMPTS: usize = 8;

pub(crate) fn is_ducklake_unsupported(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("catalog error")
        || msg.contains("function") && (msg.contains("does not exist") || msg.contains("not found"))
        || msg.contains("no function matches")
        || msg.contains("not implemented")
}

pub(crate) fn is_ducklake_serialization_conflict(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("could not serialize access due to concurrent update")
        || msg.contains("serialization failure")
}

pub(crate) fn is_ducklake_oom(err: &duckdb::Error) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("out of memory")
}

/// Fail closed when the loaded DuckLake build rejects `newer_than`.
pub(crate) fn is_newer_than_unsupported(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("newer_than")
        && (msg.contains("unknown")
            || msg.contains("named parameter")
            || msg.contains("does not exist")
            || msg.contains("no function matches")
            || msg.contains("binder error")
            || msg.contains("invalid input"))
}

fn sleep_backoff(ms: u64) {
    tokio::task::block_in_place(|| {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    });
}

pub(crate) fn execute_batch_with_serialization_retry(
    conn: &duckdb::Connection,
    sql: &str,
    max_attempts: usize,
    action: &str,
) -> std::result::Result<(), duckdb::Error> {
    if let Err(msg) = crate::sql::ensure_fact_scan_bound(sql) {
        return Err(duckdb::Error::InvalidParameterName(format!(
            "SQL gate: {msg}"
        )));
    }
    let attempts = std::cmp::max(1, max_attempts);
    let mut backoff_ms = 150u64;
    for attempt in 1..=attempts {
        match conn.execute_batch(sql) {
            Ok(()) => return Ok(()),
            Err(err) if is_ducklake_serialization_conflict(&err) && attempt < attempts => {
                warn!(
                    "Retrying {} after transient serialization conflict (attempt {}/{}): {}",
                    action, attempt, attempts, err
                );
                sleep_backoff(backoff_ms);
                backoff_ms = (backoff_ms.saturating_mul(2)).min(2_000);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub(crate) fn sleep_conflict_wave_backoff() {
    sleep_backoff(500);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_attempts_constant() {
        assert_eq!(COMPACTION_SERIALIZATION_ATTEMPTS, 8);
    }

    #[test]
    fn newer_than_unsupported_detection() {
        let err = duckdb::Error::InvalidParameterName(
            "Binder Error: No function matches ... newer_than".into(),
        );
        assert!(is_newer_than_unsupported(&err));
        let other = duckdb::Error::InvalidParameterName("out of memory".into());
        assert!(!is_newer_than_unsupported(&other));
    }
}
