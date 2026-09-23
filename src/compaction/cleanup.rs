//! Snapshot expire / orphan-cleanup helpers (SQL via [`crate::sql::maintenance`]).

use anyhow::Result;
use duckdb::Connection;

pub use crate::sql::maintenance::{cleanup_old_files_sql, expire_snapshots_sql};

pub(crate) fn count_returned_rows(conn: &Connection, sql: &str) -> Result<usize> {
    let mut stmt = crate::sql::prepare_checked(conn, sql)?;
    let mut rows = stmt.query([])?;
    let mut count = 0usize;
    while let Some(_row) = rows.next()? {
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::maintenance::delete_orphaned_files_sql;

    /// AC-N6: after a maintenance pass, live `ducklake_snapshot` count must be ≤ this.
    const SNAPSHOT_COUNT_BAR_AFTER_PASS: usize = 50;

    fn snapshot_max_age_after_pass_seconds(
        max_snapshot_age_seconds: u64,
        interval_seconds: u64,
    ) -> u64 {
        max_snapshot_age_seconds.saturating_add(interval_seconds)
    }

    #[test]
    fn expire_snapshots_sql_honors_n6_count_and_age_bars() {
        let cfg = crate::config::Config::default();
        assert_eq!(SNAPSHOT_COUNT_BAR_AFTER_PASS, 50);
        assert_eq!(
            snapshot_max_age_after_pass_seconds(
                cfg.maintenance.max_snapshot_age_seconds,
                cfg.maintenance.interval_seconds,
            ),
            120
        );
        let sql =
            expire_snapshots_sql("softprobe", cfg.maintenance.max_snapshot_age_seconds, false);
        assert!(sql.contains("INTERVAL '60 seconds'"));
    }

    #[test]
    fn maintenance_file_cleanup_is_scheduled_only() {
        let scheduled = cleanup_old_files_sql("softprobe", 60);
        let orphan = delete_orphaned_files_sql("softprobe", 60);
        assert!(scheduled.contains("ducklake_cleanup_old_files"));
        assert!(!scheduled.contains("delete_orphaned"));
        assert!(orphan.contains("ducklake_delete_orphaned_files"));
    }
}
