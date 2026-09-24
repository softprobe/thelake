//! DuckLake maintenance SQL recipes (merge, expire, cleanup, inventory probes).
//!
//! All production maintenance SQL lives here. Compaction/engine code calls these
//! builders and executes via [`crate::sql::ensure_fact_scan_bound`] /
//! [`crate::sql::prepare_checked`] / [`crate::sql::execute_batch_checked`].

use chrono::{DateTime, Utc};

use crate::runtime_engine::quote_pg_ident;
use crate::sql::literal::{sql_string_literal, timestamp_ns_column, timestamptz_literal};

fn meta_schema(catalog_alias: &str) -> String {
    format!("__ducklake_metadata_{catalog_alias}")
}

/// Scheduled merge must pass `newer_than`; admin full repair passes `None`.
pub fn ducklake_merge_adjacent_files_sql(
    catalog_alias: &str,
    table: &str,
    schema: &str,
    newer_than: Option<DateTime<Utc>>,
    max_compacted_files: Option<u64>,
    max_file_size_bytes: Option<u64>,
) -> String {
    let mut args = format!("schema => {}", sql_string_literal(schema));
    if let Some(ts) = newer_than {
        args.push_str(&format!(", newer_than => {}", timestamptz_literal(&ts)));
    }
    if let Some(max_compacted_files) = max_compacted_files {
        args.push_str(&format!(", max_compacted_files => {max_compacted_files}"));
    }
    if let Some(max_file_size_bytes) = max_file_size_bytes {
        args.push_str(&format!(", max_file_size => {max_file_size_bytes}"));
    }
    format!(
        "CALL ducklake_merge_adjacent_files({}, {}, {args});",
        sql_string_literal(catalog_alias),
        sql_string_literal(table)
    )
}

pub fn ducklake_set_target_file_size_sql(
    catalog_alias: &str,
    target_file_size_literal: &str,
    option_scope: &str,
) -> String {
    format!(
        "CALL {}.set_option('target_file_size', {}, {});",
        catalog_alias,
        sql_string_literal(target_file_size_literal),
        option_scope
    )
}

pub fn partition_live_file_stats_sql(catalog_alias: &str, table: &str) -> String {
    partition_live_file_stats_sql_inner(catalog_alias, table, None)
}

/// Post-watermark partition stats (incremental gates / drain predicate).
pub fn partition_live_file_stats_after_sql(
    catalog_alias: &str,
    table: &str,
    newer_than: DateTime<Utc>,
) -> String {
    partition_live_file_stats_sql_inner(catalog_alias, table, Some(newer_than))
}

fn partition_live_file_stats_sql_inner(
    catalog_alias: &str,
    table: &str,
    newer_than: Option<DateTime<Utc>>,
) -> String {
    let meta = meta_schema(catalog_alias);
    let newer_join = if let Some(ts) = newer_than {
        format!(
            " JOIN {meta}.ducklake_snapshot snap \
               ON snap.snapshot_id = df.begin_snapshot \
              AND snap.snapshot_time >= {} ",
            timestamptz_literal(&ts)
        )
    } else {
        String::new()
    };
    format!(
        "SELECT printf('%04d-%02d-%02d', \
                  CAST(y.partition_value AS INTEGER), \
                  CAST(m.partition_value AS INTEGER), \
                  CAST(d.partition_value AS INTEGER)) AS partition_day, \
                count(*)::BIGINT AS live_file_count, \
                coalesce(sum(df.file_size_bytes), 0)::BIGINT AS total_bytes \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         {newer_join}\
         JOIN {meta}.ducklake_file_partition_value y \
           ON y.data_file_id = df.data_file_id AND y.table_id = t.table_id \
          AND y.partition_key_index = 0 \
         JOIN {meta}.ducklake_file_partition_value m \
           ON m.data_file_id = df.data_file_id AND m.table_id = t.table_id \
          AND m.partition_key_index = 1 \
         JOIN {meta}.ducklake_file_partition_value d \
           ON d.data_file_id = df.data_file_id AND d.table_id = t.table_id \
          AND d.partition_key_index = 2 \
         WHERE t.table_name = {} \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL \
         GROUP BY 1 \
         ORDER BY 1",
        sql_string_literal(table)
    )
}

/// Logical row count probe — carries a wide timestamp bound for D12.
pub fn logical_table_row_count_sql(catalog_alias: &str, table: &str) -> String {
    let ts = timestamp_ns_column("timestamp");
    format!(
        "SELECT count(*)::BIGINT FROM {catalog_alias}.{table} \
         WHERE {ts} >= '1970-01-01'::TIMESTAMP_NS \
           AND {ts} <= '2100-01-01'::TIMESTAMP_NS"
    )
}

pub fn live_files_spanning_record_dates_sql(catalog_alias: &str, table: &str) -> String {
    let meta = meta_schema(catalog_alias);
    format!(
        "WITH file_days AS ( \
           SELECT df.data_file_id, \
                  printf('%04d-%02d-%02d', \
                    CAST(y.partition_value AS INTEGER), \
                    CAST(m.partition_value AS INTEGER), \
                    CAST(d.partition_value AS INTEGER)) AS partition_day \
           FROM {meta}.ducklake_data_file df \
           JOIN {meta}.ducklake_table t \
             ON df.table_id = t.table_id \
           JOIN {meta}.ducklake_file_partition_value y \
             ON y.data_file_id = df.data_file_id AND y.table_id = t.table_id \
            AND y.partition_key_index = 0 \
           JOIN {meta}.ducklake_file_partition_value m \
             ON m.data_file_id = df.data_file_id AND m.table_id = t.table_id \
            AND m.partition_key_index = 1 \
           JOIN {meta}.ducklake_file_partition_value d \
             ON d.data_file_id = df.data_file_id AND d.table_id = t.table_id \
            AND d.partition_key_index = 2 \
           WHERE t.table_name = {} \
             AND t.end_snapshot IS NULL \
             AND df.end_snapshot IS NULL \
         ) \
         SELECT data_file_id, count(DISTINCT partition_day) AS n_dates \
         FROM file_days \
         GROUP BY data_file_id \
         HAVING count(DISTINCT partition_day) > 1",
        sql_string_literal(table)
    )
}

pub fn live_data_file_paths_sql(catalog_alias: &str, table: &str) -> String {
    let meta = meta_schema(catalog_alias);
    format!(
        "SELECT df.path \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         WHERE t.table_name = {} \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL",
        sql_string_literal(table)
    )
}

pub fn live_file_count_sql(catalog_alias: &str, table: &str) -> String {
    let meta = meta_schema(catalog_alias);
    format!(
        "SELECT count(*)::BIGINT \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         WHERE t.table_name = {} \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL",
        sql_string_literal(table)
    )
}

pub fn live_file_sizes_sql(catalog_alias: &str, table: &str) -> String {
    let meta = meta_schema(catalog_alias);
    format!(
        "SELECT df.file_size_bytes::BIGINT AS file_size_bytes \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t ON df.table_id = t.table_id \
         WHERE t.table_name = {} \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL",
        sql_string_literal(table)
    )
}

pub fn table_exists_probe_sql(qualified_table: &str) -> String {
    let ts = timestamp_ns_column("timestamp");
    format!(
        "SELECT 1 FROM {qualified_table} \
         WHERE {ts} >= '1970-01-01'::TIMESTAMP_NS \
           AND {ts} <= '2100-01-01'::TIMESTAMP_NS \
         LIMIT 0;"
    )
}

fn ducklake_older_than_interval(age_seconds: u64) -> String {
    format!("INTERVAL '{} seconds'", age_seconds)
}

pub fn expire_snapshots_sql(
    catalog_alias: &str,
    max_snapshot_age_seconds: u64,
    dry_run: bool,
) -> String {
    let interval = ducklake_older_than_interval(max_snapshot_age_seconds);
    let alias = sql_string_literal(catalog_alias);
    if dry_run {
        format!(
            "CALL ducklake_expire_snapshots({alias}, dry_run => true, older_than => now() - {interval});"
        )
    } else {
        format!("CALL ducklake_expire_snapshots({alias}, older_than => now() - {interval});")
    }
}

pub fn cleanup_old_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
    ducklake_file_cleanup_sql(
        "ducklake_cleanup_old_files",
        catalog_alias,
        older_than_seconds,
    )
}

/// Not used by the scheduler (hive live files look untracked).
pub fn delete_orphaned_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
    ducklake_file_cleanup_sql(
        "ducklake_delete_orphaned_files",
        catalog_alias,
        older_than_seconds,
    )
}

fn ducklake_file_cleanup_sql(
    function: &str,
    catalog_alias: &str,
    older_than_seconds: u64,
) -> String {
    let alias = sql_string_literal(catalog_alias);
    if older_than_seconds == 0 {
        format!("CALL {function}({alias}, cleanup_all => true);")
    } else {
        let interval = ducklake_older_than_interval(older_than_seconds);
        format!("CALL {function}({alias}, older_than => now() - {interval});")
    }
}

// --- Postgres compaction watermark (app-owned) ---

pub fn compaction_watermark_create_table_sql(registry_schema: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {}.compaction_watermark (
  scope_key TEXT NOT NULL,
  table_name TEXT NOT NULL,
  watermark TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (scope_key, table_name)
);"#,
        quote_pg_ident(registry_schema)
    )
}

pub fn compaction_watermark_get_sql(registry_schema: &str) -> String {
    format!(
        "SELECT watermark FROM {}.compaction_watermark WHERE scope_key = $1 AND table_name = $2;",
        quote_pg_ident(registry_schema)
    )
}

pub fn compaction_watermark_insert_fence_sql(registry_schema: &str) -> String {
    format!(
        r#"INSERT INTO {}.compaction_watermark (scope_key, table_name, watermark, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (scope_key, table_name) DO NOTHING;"#,
        quote_pg_ident(registry_schema)
    )
}

pub fn compaction_watermark_advance_sql(registry_schema: &str) -> String {
    format!(
        r#"UPDATE {}.compaction_watermark
SET watermark = $3, updated_at = NOW()
WHERE scope_key = $1 AND table_name = $2;"#,
        quote_pg_ident(registry_schema)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn incremental_merge_sql_requires_newer_than() {
        let ts = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let sql = ducklake_merge_adjacent_files_sql(
            "softprobe",
            "traces",
            "main",
            Some(ts),
            Some(32),
            Some(8 * 1024 * 1024),
        );
        assert!(sql.contains("newer_than => TIMESTAMPTZ"));
        assert!(sql.contains("max_compacted_files => 32"));
        assert!(!sql.contains("partition_filter"));
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
    }

    #[test]
    fn full_merge_sql_omits_newer_than() {
        let sql = ducklake_merge_adjacent_files_sql(
            "softprobe",
            "traces",
            "main",
            None,
            Some(32),
            Some(8 * 1024 * 1024),
        );
        assert!(!sql.contains("newer_than"));
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
    }

    #[test]
    fn logical_row_count_carries_timestamp_bound() {
        let sql = logical_table_row_count_sql("softprobe", "traces");
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
    }

    #[test]
    fn partition_stats_after_filters_snapshot_time() {
        let ts = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let sql = partition_live_file_stats_after_sql("softprobe", "traces", ts);
        assert!(sql.contains("ducklake_snapshot"));
        assert!(sql.contains("snapshot_time >="));
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
    }

    #[test]
    fn expire_and_cleanup_use_seconds_intervals() {
        let sql = expire_snapshots_sql("softprobe", 60, false);
        assert!(sql.contains("INTERVAL '60 seconds'"));
        assert!(!sql.contains("days"));
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
        let cleanup = cleanup_old_files_sql("softprobe", 3600);
        assert!(cleanup.contains("INTERVAL '3600 seconds'"));
        assert!(crate::sql::ensure_fact_scan_bound(&cleanup).is_ok());
    }

    #[test]
    fn scheduled_merge_sql_never_omits_newer_than() {
        use chrono::TimeZone;
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let with = ducklake_merge_adjacent_files_sql(
            "softprobe",
            "traces",
            "main",
            Some(ts),
            Some(32),
            Some(8 * 1024 * 1024),
        );
        assert!(with.contains("newer_than => TIMESTAMPTZ"));
        // Engine scheduled path only inserts watermarked merges.
        let engine = include_str!("../../compaction/engine.rs");
        let production = engine.split("#[cfg(test)]").next().unwrap();
        assert!(
            !production.contains("MergeMode::Full"),
            "scheduled engine must not request Full merge"
        );
        assert!(
            production.contains("ensure_fence") && production.contains("compact_table_incremental"),
            "engine must fence then incremental-merge"
        );
    }

    #[test]
    fn post_watermark_stats_sql_excludes_pre_watermark_via_snapshot_predicate() {
        use chrono::TimeZone;
        let ts = Utc.with_ymd_and_hms(2026, 9, 15, 12, 0, 0).unwrap();
        let sql = partition_live_file_stats_after_sql("softprobe", "traces", ts);
        assert!(sql.contains("snap.snapshot_time >="));
        assert!(sql.contains("2026-09-15"));
        assert!(
            !sql.contains("OR snap.snapshot_time <"),
            "must not include pre-watermark files"
        );
    }

    #[test]
    fn incomplete_drain_predicate_retains_work() {
        use crate::compaction::twcs::{
            partitions_needing_merge, post_watermark_candidates_drained, PartitionFileStats,
            TwcsPolicy,
        };
        use chrono::NaiveDate;
        let today = NaiveDate::from_ymd_opt(2026, 9, 20).unwrap();
        let still_needs = [PartitionFileStats {
            record_date: NaiveDate::from_ymd_opt(2026, 9, 19).unwrap(),
            live_file_count: 4,
            total_bytes: 1_000,
        }];
        let p = TwcsPolicy::default();
        assert!(!post_watermark_candidates_drained(&still_needs, today, &p));
        assert!(!partitions_needing_merge(&still_needs, today, &p).is_empty());
        let drained = [PartitionFileStats {
            record_date: NaiveDate::from_ymd_opt(2026, 9, 19).unwrap(),
            live_file_count: 1,
            total_bytes: 1_000,
        }];
        assert!(post_watermark_candidates_drained(&drained, today, &p));
    }

    #[test]
    fn pass_ok_false_on_failed_or_unsupported() {
        use crate::compaction::{pass_compaction_ok, ActionStatus};
        assert!(!pass_compaction_ok(&[ActionStatus::Failed]));
        assert!(!pass_compaction_ok(&[ActionStatus::Unsupported]));
        assert!(pass_compaction_ok(&[
            ActionStatus::Skipped,
            ActionStatus::Completed
        ]));
    }

    #[test]
    fn watermark_sql_is_schema_qualified_and_conflict_safe() {
        let create = compaction_watermark_create_table_sql("thelake_registry");
        assert!(create.contains("\"thelake_registry\".compaction_watermark"));
        assert!(create.contains("PRIMARY KEY (scope_key, table_name)"));
        let fence = compaction_watermark_insert_fence_sql("thelake_registry");
        assert!(fence.contains("ON CONFLICT (scope_key, table_name) DO NOTHING"));
        let advance = compaction_watermark_advance_sql("thelake_registry");
        assert!(advance.contains("SET watermark = $3"));
        assert!(advance.contains("WHERE scope_key = $1 AND table_name = $2"));
    }

    #[test]
    fn table_exists_probe_is_gate_checked() {
        let sql = table_exists_probe_sql("softprobe.main.traces");
        assert!(crate::sql::ensure_fact_scan_bound(&sql).is_ok());
    }
}
