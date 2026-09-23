//! Softprobe TWCS merge policy (§7.1).
//!
//! Time window = calendar day of `timestamp` (one-clock hive keys).
//! Scheduled merges always pass DuckLake `newer_than` (see
//! [`crate::sql::maintenance`]). Softprobe gates run on **post-watermark**
//! partition stats only. Rewrite locality comes from `PARTITIONED BY` (T-F6).
//!
//! SQL recipes live in [`crate::sql::maintenance`] — not here.

use crate::config::MaintenanceConfig;
use chrono::{DateTime, NaiveDate, Utc};

/// Closed-day merge if more than this many live files (complete compact → 1 file).
pub const TWCS_TRIGGER_FILE_NUM: usize = 2;
/// Default open-day live file soft cap (AC-F4). Override via `MaintenanceConfig`.
pub const TWCS_OPEN_DAY_FILE_CAP: usize = 2;

/// Live Parquet stats for one calendar-day partition (from year/month/day keys).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFileStats {
    /// Calendar day reconstructed from one-clock partition keys (not a lake column).
    pub record_date: NaiveDate,
    pub live_file_count: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayKind {
    /// Day `< today` — may fully merge toward target size.
    Closed,
    /// Day `== today` — soft cap only; do not force single-file merge.
    Open,
}

pub fn day_kind(record_date: NaiveDate, today: NaiveDate) -> DayKind {
    if record_date < today {
        DayKind::Closed
    } else {
        DayKind::Open
    }
}

/// Whether TWCS should merge this partition on this pass (§7.1).
///
/// Callers must pass **post-watermark** stats on the incremental path so
/// closed-day→1-file is not an incremental whole-day invariant.
///
/// Closed-day gates are **per-partition** only. Do not inherit “size pressure”
/// from other days — that permanently blocks watermark drain when today’s open
/// day sits at a healthy soft-cap with multiple small files.
pub fn should_merge_partition(
    stats: &PartitionFileStats,
    kind: DayKind,
    policy: &TwcsPolicy,
) -> bool {
    match kind {
        DayKind::Closed => {
            !closed_day_meets_file_bar(stats.live_file_count, stats.total_bytes)
        }
        DayKind::Open => stats.live_file_count > policy.open_day_file_cap,
    }
}

/// Scheduled merge scope: always carries a required `newer_than` watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MergeMode {
    pub newer_than: DateTime<Utc>,
}

impl MergeMode {
    pub(crate) fn newer_than(self) -> DateTime<Utc> {
        self.newer_than
    }
}

/// Calendar days (from the provided partition stats) that still need merge.
pub(crate) fn partitions_needing_merge(
    partitions: &[PartitionFileStats],
    today: NaiveDate,
    policy: &TwcsPolicy,
) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    for stats in partitions {
        let kind = day_kind(stats.record_date, today);
        if should_merge_partition(stats, kind, policy) {
            days.push(stats.record_date);
        }
    }
    days
}

/// Drain predicate: no post-watermark partition still fails merge bars.
pub(crate) fn post_watermark_candidates_drained(
    partitions: &[PartitionFileStats],
    today: NaiveDate,
    policy: &TwcsPolicy,
) -> bool {
    partitions_needing_merge(partitions, today, policy).is_empty()
}

/// Softprobe view of non-Parquet backlog for one logical table (AC-F7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinedFragmentStats {
    pub table: String,
    pub live_parquet_files: usize,
    pub logical_row_count: u64,
}

impl InlinedFragmentStats {
    pub fn is_inlined_only(&self) -> bool {
        self.logical_row_count > 0 && self.live_parquet_files == 0
    }
}

pub const TWCS_MAX_COMPACTED_FILES_PER_WAVE: u64 = 32;
pub const TWCS_MAX_WAVES_PER_TABLE: usize = 32;
pub const TWCS_CLOSED_DAY_MAX_COMPACTED_FILES: u64 = 256;
pub const TWCS_CLOSED_DAY_MAX_WAVES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TwcsPolicy {
    pub open_day_file_cap: usize,
    pub max_waves_per_table: usize,
    pub max_compacted_files_per_wave: u64,
    pub closed_day_max_compacted_files: u64,
    pub closed_day_max_waves: usize,
    pub max_merge_file_size_bytes: u64,
}

impl Default for TwcsPolicy {
    fn default() -> Self {
        Self {
            open_day_file_cap: TWCS_OPEN_DAY_FILE_CAP,
            max_waves_per_table: TWCS_MAX_WAVES_PER_TABLE,
            max_compacted_files_per_wave: TWCS_MAX_COMPACTED_FILES_PER_WAVE,
            closed_day_max_compacted_files: TWCS_CLOSED_DAY_MAX_COMPACTED_FILES,
            closed_day_max_waves: TWCS_CLOSED_DAY_MAX_WAVES,
            max_merge_file_size_bytes: 8 * 1024 * 1024,
        }
    }
}

impl From<&MaintenanceConfig> for TwcsPolicy {
    fn from(m: &MaintenanceConfig) -> Self {
        Self {
            open_day_file_cap: m.open_day_file_cap,
            max_waves_per_table: m.max_waves_per_table,
            max_compacted_files_per_wave: m.max_compacted_files_per_wave,
            closed_day_max_compacted_files: m.closed_day_max_compacted_files,
            closed_day_max_waves: m.closed_day_max_waves,
            max_merge_file_size_bytes: m.max_merge_file_size_bytes,
        }
    }
}

pub fn open_day_max_compacted_files(live_files: usize, policy: &TwcsPolicy) -> u64 {
    if live_files > policy.closed_day_max_compacted_files as usize {
        policy.closed_day_max_compacted_files
    } else {
        policy.max_compacted_files_per_wave
    }
}

#[cfg(test)]
fn closed_day_file_capacity(policy: &TwcsPolicy) -> u64 {
    policy.closed_day_max_waves as u64 * policy.closed_day_max_compacted_files
}

#[cfg(test)]
fn open_day_file_capacity(policy: &TwcsPolicy) -> u64 {
    policy.max_waves_per_table as u64 * policy.closed_day_max_compacted_files
}

pub fn open_day_files_for_merge(
    partitions: &[PartitionFileStats],
    today: NaiveDate,
    live_file_fallback: Option<usize>,
) -> usize {
    let open = open_day_live_file_count(partitions, today);
    let closed = closed_day_live_file_count(partitions, today);
    match live_file_fallback {
        None => open,
        Some(live) => open.max(live.saturating_sub(closed)),
    }
}

pub fn closed_day_meets_file_bar(live_file_count: usize, total_bytes: u64) -> bool {
    const TARGET: u64 = 64 * 1024 * 1024;
    if live_file_count < TWCS_TRIGGER_FILE_NUM {
        return true;
    }
    if live_file_count == TWCS_TRIGGER_FILE_NUM {
        return total_bytes > TARGET;
    }
    false
}

pub fn closed_day_live_file_count(partitions: &[PartitionFileStats], today: NaiveDate) -> usize {
    partitions
        .iter()
        .filter(|p| day_kind(p.record_date, today) == DayKind::Closed)
        .map(|p| p.live_file_count)
        .sum()
}

pub fn open_day_live_file_count(partitions: &[PartitionFileStats], today: NaiveDate) -> usize {
    partitions
        .iter()
        .filter(|p| day_kind(p.record_date, today) == DayKind::Open)
        .map(|p| p.live_file_count)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn policy() -> TwcsPolicy {
        TwcsPolicy::default()
    }

    #[test]
    fn gates_schedule_merge_when_later_parquet_appears() {
        let today = d(2026, 9, 11);
        let day = d(2026, 9, 10);
        let p = policy();
        assert!(partitions_needing_merge(&[], today, &p).is_empty());
        let after = [PartitionFileStats {
            record_date: day,
            live_file_count: 4,
            total_bytes: 1_000_000,
        }];
        assert_eq!(partitions_needing_merge(&after, today, &p), vec![day]);
    }

    #[test]
    fn twcs_policy_matches_maintenance_config_defaults() {
        assert_eq!(
            TwcsPolicy::default(),
            TwcsPolicy::from(&MaintenanceConfig::default())
        );
    }

    #[test]
    fn closed_day_triggers_at_two_files() {
        let today = d(2026, 8, 15);
        let day = d(2026, 8, 14);
        let p = policy();
        let one = PartitionFileStats {
            record_date: day,
            live_file_count: 1,
            total_bytes: 1_000,
        };
        let two = PartitionFileStats {
            record_date: day,
            live_file_count: 2,
            total_bytes: 1_000,
        };
        assert_eq!(TWCS_TRIGGER_FILE_NUM, 2);
        assert!(!should_merge_partition(&one, day_kind(day, today), &p));
        assert!(should_merge_partition(&two, day_kind(day, today), &p));
        let two_over = PartitionFileStats {
            record_date: day,
            live_file_count: 2,
            total_bytes: 65 * 1024 * 1024,
        };
        assert!(!should_merge_partition(
            &two_over,
            day_kind(day, today),
            &p
        ));
    }

    #[test]
    fn open_day_triggers_only_above_cap() {
        let today = d(2026, 8, 15);
        let p = policy();
        let under = PartitionFileStats {
            record_date: today,
            live_file_count: TWCS_OPEN_DAY_FILE_CAP,
            total_bytes: 1_000,
        };
        let over = PartitionFileStats {
            record_date: today,
            live_file_count: TWCS_OPEN_DAY_FILE_CAP + 1,
            total_bytes: 1_000,
        };
        assert!(!should_merge_partition(&under, DayKind::Open, &p));
        assert!(should_merge_partition(&over, DayKind::Open, &p));
    }

    #[test]
    fn gates_list_one_day_per_record_date() {
        let today = d(2026, 8, 15);
        let parts = vec![
            PartitionFileStats {
                record_date: d(2026, 8, 13),
                live_file_count: 5,
                total_bytes: 20_000_000,
            },
            PartitionFileStats {
                record_date: d(2026, 8, 14),
                live_file_count: 6,
                total_bytes: 20_000_000,
            },
        ];
        let days = partitions_needing_merge(&parts, today, &policy());
        assert_eq!(days, vec![d(2026, 8, 13), d(2026, 8, 14)]);
    }

    #[test]
    fn gates_skip_quiet_partitions() {
        let today = d(2026, 8, 15);
        let parts = vec![PartitionFileStats {
            record_date: d(2026, 8, 14),
            live_file_count: 1,
            total_bytes: 100,
        }];
        assert!(partitions_needing_merge(&parts, today, &policy()).is_empty());
        assert!(post_watermark_candidates_drained(&parts, today, &policy()));
    }

    #[test]
    fn closed_day_wave_budget_covers_ten_thousand_files() {
        let p = policy();
        let cap = closed_day_file_capacity(&p);
        assert!(cap >= 10_000);
        assert!(cap > open_day_file_capacity(&p));
        assert!(open_day_file_capacity(&p) >= 2000);
        assert_eq!(open_day_max_compacted_files(25, &p), 32);
        assert_eq!(open_day_max_compacted_files(1774, &p), 256);
    }

    #[test]
    fn open_day_empty_partition_stats_use_live_file_fallback() {
        let today = d(2026, 8, 15);
        assert_eq!(open_day_files_for_merge(&[], today, None), 0);
        assert_eq!(open_day_files_for_merge(&[], today, Some(1774)), 1774);
    }

    #[test]
    fn merge_mode_always_carries_newer_than() {
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(MergeMode { newer_than: ts }.newer_than(), ts);
    }

    #[test]
    fn late_arrival_closed_day_still_needs_merge_after_watermark() {
        // Post-watermark stats only: a closed day that received late files must
        // still fail the drain bar even when an older quiet day is already 1-file.
        let today = d(2026, 9, 20);
        let p = policy();
        let parts = [
            PartitionFileStats {
                record_date: d(2026, 9, 18),
                live_file_count: 1,
                total_bytes: 10_000,
            },
            PartitionFileStats {
                record_date: d(2026, 9, 19),
                live_file_count: 5,
                total_bytes: 2_000_000,
            },
        ];
        let needing = partitions_needing_merge(&parts, today, &p);
        assert!(
            needing.contains(&d(2026, 9, 19)),
            "late-arrival closed day must still need merge: {needing:?}"
        );
        assert!(!post_watermark_candidates_drained(&parts, today, &p));
    }

    #[test]
    fn open_day_above_cap_blocks_drain_even_when_closed_is_quiet() {
        let today = d(2026, 9, 20);
        let p = policy();
        let parts = [
            PartitionFileStats {
                record_date: d(2026, 9, 19),
                live_file_count: 1,
                total_bytes: 100,
            },
            PartitionFileStats {
                record_date: today,
                live_file_count: TWCS_OPEN_DAY_FILE_CAP + 3,
                total_bytes: 1_000,
            },
        ];
        assert!(!post_watermark_candidates_drained(&parts, today, &p));
        let needing = partitions_needing_merge(&parts, today, &p);
        assert!(
            needing.contains(&today),
            "open day above cap must need merge: {needing:?}"
        );
    }

    #[test]
    fn empty_post_watermark_stats_are_drained() {
        let today = d(2026, 9, 20);
        assert!(post_watermark_candidates_drained(&[], today, &policy()));
    }

    #[test]
    fn open_day_at_soft_cap_does_not_block_closed_day_drain() {
        // Critic repro: open day at soft cap with small files must not keep a
        // healthy 1-file closed day from draining (no global size_pressure).
        let today = d(2026, 9, 20);
        let p = policy();
        let parts = [
            PartitionFileStats {
                record_date: d(2026, 9, 19),
                live_file_count: 1,
                total_bytes: 10_000,
            },
            PartitionFileStats {
                record_date: today,
                live_file_count: TWCS_OPEN_DAY_FILE_CAP,
                total_bytes: 1_000_000,
            },
        ];
        assert!(
            partitions_needing_merge(&parts, today, &p).is_empty(),
            "steady-state open+closed must be drained"
        );
        assert!(post_watermark_candidates_drained(&parts, today, &p));
    }
}
