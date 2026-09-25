//! Softprobe TWCS wave orchestration with required `newer_than` scoping.

use crate::compaction::retry::{
    execute_batch_with_serialization_retry, is_ducklake_oom, is_ducklake_serialization_conflict,
    is_ducklake_unsupported, is_newer_than_unsupported, sleep_conflict_wave_backoff,
    COMPACTION_SERIALIZATION_ATTEMPTS,
};
use crate::compaction::status::ActionStatus;
use crate::compaction::twcs::{
    closed_day_live_file_count, day_kind, open_day_files_for_merge, open_day_max_compacted_files,
    partitions_needing_merge, post_watermark_candidates_drained, should_merge_partition, DayKind,
    InlinedFragmentStats, MergeMode, PartitionFileStats, TwcsPolicy,
};
use crate::config::Config;
use crate::sql::maintenance::{
    ducklake_merge_adjacent_files_sql, ducklake_set_target_file_size_sql,
    logical_table_row_count_sql, partition_live_file_stats_after_sql,
};
use crate::storage::ducklake::PhysicalScope;
use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use duckdb::Connection;
use tracing::{info, warn};

pub(crate) struct MergeOutcome {
    pub status: ActionStatus,
    /// When true, watermark may advance to `run_started_at`.
    pub drained: bool,
}

/// Compact one table with a persisted watermark. Never issues an unscoped CALL.
/// Caller advances the watermark when `drained` is true (async Postgres update).
pub(crate) fn compact_table_incremental(
    config: &Config,
    conn: &Connection,
    scope: &PhysicalScope,
    table: &str,
    scope_key: &str,
    watermark: DateTime<Utc>,
) -> Result<MergeOutcome> {
    let policy = TwcsPolicy::from(&config.maintenance);
    let today = Utc::now().date_naive();
    let mode = MergeMode {
        newer_than: watermark,
    };
    let mut last = ActionStatus::Skipped;

    if let Ok(Some(pending)) = load_inlined_fragment_stats(conn, scope, table) {
        info!(
            "TWCS backlog {}.{}: logical_rows={} live_parquet_files={} inlined_only={}",
            scope.pg_namespace(),
            table,
            pending.logical_row_count,
            pending.live_parquet_files,
            pending.is_inlined_only()
        );
    }

    let initial = match load_partition_stats_after(conn, scope.attach_alias(), table, watermark) {
        Ok(v) => v,
        Err(err) => {
            warn!(
                "TWCS post-watermark stats failed for {}.{}: {}; not draining",
                scope.pg_namespace(),
                table,
                err
            );
            return Ok(MergeOutcome {
                status: ActionStatus::Failed,
                drained: false,
            });
        }
    };
    if initial.is_empty() && post_watermark_candidates_drained(&initial, today, &policy) {
        return Ok(MergeOutcome {
            status: ActionStatus::Skipped,
            drained: true,
        });
    }

    last = twcs_compact_waves(
        config,
        conn,
        scope,
        table,
        today,
        last,
        &policy,
        scope_key,
        mode,
        WaveKind::Closed,
    )?;
    if last == ActionStatus::Failed || last == ActionStatus::Unsupported {
        return Ok(MergeOutcome {
            status: last,
            drained: false,
        });
    }

    last = twcs_compact_waves(
        config,
        conn,
        scope,
        table,
        today,
        last,
        &policy,
        scope_key,
        mode,
        WaveKind::Open,
    )?;
    if last == ActionStatus::Failed || last == ActionStatus::Unsupported {
        return Ok(MergeOutcome {
            status: last,
            drained: false,
        });
    }

    let after = match load_partition_stats_after(conn, scope.attach_alias(), table, watermark) {
        Ok(v) => v,
        Err(err) => {
            warn!(
                "TWCS post-merge stats failed for {}.{}: {}; retaining watermark",
                scope.pg_namespace(),
                table,
                err
            );
            return Ok(MergeOutcome {
                status: last,
                drained: false,
            });
        }
    };
    let drained = post_watermark_candidates_drained(&after, today, &policy);
    if drained {
        info!(
            "TWCS drain complete for {}.{}; watermark may advance",
            scope.pg_namespace(),
            table
        );
    } else {
        info!(
            "TWCS drain incomplete for {}.{}; retaining watermark {}",
            scope.pg_namespace(),
            table,
            watermark
        );
    }
    Ok(MergeOutcome {
        status: last,
        drained,
    })
}

#[derive(Debug, Clone, Copy)]
enum WaveKind {
    Closed,
    Open,
}

impl WaveKind {
    fn label(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
        }
    }

    fn max_waves(self, policy: &TwcsPolicy) -> usize {
        match self {
            Self::Closed => policy.closed_day_max_waves,
            Self::Open => policy.max_waves_per_table,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn twcs_compact_waves(
    config: &Config,
    conn: &Connection,
    scope: &PhysicalScope,
    table: &str,
    today: NaiveDate,
    mut last: ActionStatus,
    policy: &TwcsPolicy,
    tenant_id: &str,
    mode: MergeMode,
    kind: WaveKind,
) -> Result<ActionStatus> {
    let newer_than = mode.newer_than();
    let label = kind.label();
    let max_waves = kind.max_waves(policy);
    for wave in 0..max_waves {
        let partitions =
            match load_partition_stats_after(conn, scope.attach_alias(), table, newer_than) {
                Ok(v) => v,
                Err(err) => {
                    warn!(
                        "TWCS {label}-day stats failed for {}: {}; stopping waves",
                        table, err
                    );
                    return Ok(ActionStatus::Failed);
                }
            };

        let (needs_work, files_before, max_compacted) = match kind {
            WaveKind::Closed => {
                let days = partitions_needing_merge(&partitions, today, policy);
                let closed_need = days.iter().any(|d| day_kind(*d, today) == DayKind::Closed);
                (
                    closed_need,
                    closed_day_live_file_count(&partitions, today),
                    policy.closed_day_max_compacted_files,
                )
            }
            WaveKind::Open => {
                let files_before = open_day_files_for_merge(&partitions, today, None);
                let open_needs_merge = partitions.iter().any(|p| {
                    day_kind(p.record_date, today) == DayKind::Open
                        && should_merge_partition(p, DayKind::Open, policy)
                });
                (
                    files_before > policy.open_day_file_cap || open_needs_merge,
                    files_before,
                    open_day_max_compacted_files(files_before, policy),
                )
            }
        };
        if !needs_work {
            return Ok(last);
        }

        match kind {
            WaveKind::Closed => info!(
                "TWCS closed-day wave {}/{}: work pending for {}.{} ({} post-watermark closed files); max_compacted_files={}",
                wave + 1,
                max_waves,
                scope.pg_namespace(),
                table,
                files_before,
                max_compacted
            ),
            WaveKind::Open => info!(
                "TWCS open-day wave {}/{}: {}.{} has {} post-watermark live files (cap {}); max_compacted_files={}",
                wave + 1,
                max_waves,
                scope.pg_namespace(),
                table,
                files_before,
                policy.open_day_file_cap,
                max_compacted
            ),
        }

        let wave_start = std::time::Instant::now();
        last = ducklake_compact_table_wave(
            config,
            conn,
            scope,
            table,
            mode,
            max_compacted,
            policy.max_merge_file_size_bytes,
        )?;
        let partitions_after =
            match load_partition_stats_after(conn, scope.attach_alias(), table, newer_than) {
                Ok(v) => v,
                Err(_) => return Ok(ActionStatus::Failed),
            };
        let files_after = match kind {
            WaveKind::Closed => closed_day_live_file_count(&partitions_after, today),
            WaveKind::Open => open_day_files_for_merge(&partitions_after, today, None),
        };
        crate::self_monitoring::record_compaction_wave(
            tenant_id,
            table,
            label,
            wave_start.elapsed(),
            files_before as u64,
            files_after as u64,
        );

        match kind {
            WaveKind::Closed => {
                if last != ActionStatus::Completed {
                    return Ok(last);
                }
                if files_after >= files_before {
                    info!(
                        "TWCS closed-day merge {} made no file-count progress ({}); stopping waves",
                        table, files_after
                    );
                    break;
                }
            }
            WaveKind::Open => {
                info!(
                    "TWCS open-day wave {}/{} {}: status={:?} files {} → {}",
                    wave + 1,
                    max_waves,
                    table,
                    last,
                    files_before,
                    files_after
                );
                if last == ActionStatus::Unsupported || last == ActionStatus::Failed {
                    return Ok(last);
                }
            }
        }
    }
    Ok(last)
}

fn ducklake_compact_table_wave(
    config: &Config,
    conn: &Connection,
    scope: &PhysicalScope,
    table: &str,
    mode: MergeMode,
    max_compacted_files: u64,
    max_file_size_bytes: u64,
) -> Result<ActionStatus> {
    let policy = TwcsPolicy::from(&config.maintenance);
    let qualified = crate::storage::ducklake::ducklake_qualified_table_name(scope, table);
    let option_scope =
        crate::storage::ducklake::ducklake_set_option_scope_for_qualified(&qualified);
    let target_file_size =
        crate::storage::ducklake::size_literal(config.maintenance.target_file_size_bytes);
    let set_target =
        ducklake_set_target_file_size_sql(scope.attach_alias(), &target_file_size, &option_scope);
    if let Err(err) = execute_batch_with_serialization_retry(
        conn,
        &set_target,
        COMPACTION_SERIALIZATION_ATTEMPTS,
        &format!("ducklake set_option target_file_size {}", qualified),
    ) {
        if is_ducklake_serialization_conflict(&err) {
            warn!(
                "DuckLake compaction skipped for {} due to transient metadata conflict: {}",
                qualified, err
            );
            return Ok(ActionStatus::Skipped);
        }
        return Err(anyhow!(
            "DuckLake set_option failed for {}: {}",
            qualified,
            err
        ));
    }
    let sql = ducklake_merge_adjacent_files_sql(
        scope.attach_alias(),
        table,
        scope.pg_namespace(),
        Some(mode.newer_than()),
        Some(max_compacted_files),
        Some(max_file_size_bytes),
    );
    for wave in 1..=2 {
        match execute_batch_with_serialization_retry(
            conn,
            &sql,
            COMPACTION_SERIALIZATION_ATTEMPTS,
            &format!("ducklake_merge_adjacent_files {} wave{}", qualified, wave),
        ) {
            Ok(_) => return Ok(ActionStatus::Completed),
            Err(err) if is_newer_than_unsupported(&err) => {
                warn!(
                    "DuckLake newer_than unsupported for {} — fail closed: {}",
                    qualified, err
                );
                return Ok(ActionStatus::Failed);
            }
            Err(err) if is_ducklake_serialization_conflict(&err) && wave < 2 => {
                warn!(
                    "DuckLake compaction conflict on {} wave {}; backing off before retry: {}",
                    qualified, wave, err
                );
                sleep_conflict_wave_backoff();
            }
            Err(err) if is_ducklake_serialization_conflict(&err) => {
                warn!(
                    "DuckLake compaction skipped for {} due to transient metadata conflict: {}",
                    qualified, err
                );
                return Ok(ActionStatus::Skipped);
            }
            Err(err) if is_ducklake_unsupported(&err) => {
                warn!(
                    "DuckLake merge unsupported for {} (max_compacted_files={}): {}",
                    qualified, max_compacted_files, err
                );
                return Ok(ActionStatus::Unsupported);
            }
            Err(err)
                if is_ducklake_oom(&err)
                    && max_compacted_files > policy.max_compacted_files_per_wave =>
            {
                warn!(
                    "DuckLake compaction OOM for {} at max_compacted_files={}; retrying with {}",
                    qualified, max_compacted_files, policy.max_compacted_files_per_wave
                );
                return ducklake_compact_table_wave(
                    config,
                    conn,
                    scope,
                    table,
                    mode,
                    policy.max_compacted_files_per_wave,
                    max_file_size_bytes,
                );
            }
            Err(err) => {
                return Err(anyhow!(
                    "DuckLake compaction failed for {}.{}: {}",
                    scope.pg_namespace(),
                    table,
                    err
                ));
            }
        }
    }
    Ok(ActionStatus::Skipped)
}

fn load_partition_stats_after(
    conn: &Connection,
    catalog_alias: &str,
    table: &str,
    newer_than: DateTime<Utc>,
) -> Result<Vec<PartitionFileStats>> {
    let sql = partition_live_file_stats_after_sql(catalog_alias, table, newer_than);
    crate::sql::ensure_fact_scan_bound(&sql).map_err(|e| anyhow!("SQL gate: {e}"))?;
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        let date_str: String = row.get(0)?;
        let record_date = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").map_err(|e| {
            duckdb::Error::FromSqlConversionFailure(0, duckdb::types::Type::Text, Box::new(e))
        })?;
        let live_file_count: i64 = row.get(1)?;
        let total_bytes: i64 = row.get(2)?;
        Ok(PartitionFileStats {
            record_date,
            live_file_count: live_file_count.max(0) as usize,
            total_bytes: total_bytes.max(0) as u64,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

fn load_inlined_fragment_stats(
    conn: &Connection,
    scope: &PhysicalScope,
    table: &str,
) -> Result<Option<InlinedFragmentStats>> {
    let qualified = crate::storage::ducklake::ducklake_qualified_table_name(scope, table);
    let row_sql = logical_table_row_count_sql(&qualified);
    crate::sql::ensure_fact_scan_bound(&row_sql).map_err(|e| anyhow!("SQL gate: {e}"))?;
    let logical_rows: i64 = match conn.query_row(&row_sql, [], |row| row.get(0)) {
        Ok(v) => v,
        Err(err) => {
            warn!(
                "TWCS logical-row probe failed for {}: {}; treating as empty",
                qualified, err
            );
            return Ok(None);
        }
    };
    if logical_rows <= 0 {
        return Ok(None);
    }
    // Live parquet count is best-effort for logging only (not used for drain).
    let file_sql = crate::sql::maintenance::live_file_count_sql(scope.attach_alias(), table);
    crate::sql::ensure_fact_scan_bound(&file_sql).map_err(|e| anyhow!("SQL gate: {e}"))?;
    let files = conn
        .query_row(&file_sql, [], |row| row.get::<_, i64>(0))
        .map(|n| n.max(0) as usize)
        .unwrap_or(0);
    Ok(Some(InlinedFragmentStats {
        table: table.to_string(),
        live_parquet_files: files,
        logical_row_count: logical_rows as u64,
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn maintenance_does_not_flush_inlined_before_twcs() {
        let prod = include_str!("merge.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        assert!(!prod.contains("ducklake_flush_inlined_data"));
        assert!(!prod.contains("flush_inlined"));
        let engine = include_str!("engine.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        assert!(!engine.contains("ducklake_flush_inlined_data"));
        assert!(!engine.contains("flush_inlined"));
    }

    #[test]
    fn merge_fail_closed_on_newer_than_and_stats_errors() {
        let prod = include_str!("merge.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        assert!(
            prod.contains("is_newer_than_unsupported") && prod.contains("ActionStatus::Failed"),
            "newer_than rejection must map to Failed (fail closed)"
        );
        assert!(
            prod.contains("not draining") && prod.contains("drained: false"),
            "post-watermark stats failure must not drain"
        );
        assert!(
            prod.contains("retaining watermark") && prod.contains("drained: false"),
            "post-merge stats failure must retain watermark"
        );
    }

    #[test]
    fn incremental_entry_point_always_builds_watermarked_mode() {
        let prod = include_str!("merge.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        let fn_body = prod
            .split("pub(crate) fn compact_table_incremental")
            .nth(1)
            .expect("compact_table_incremental")
            .split("fn twcs_compact_waves")
            .next()
            .expect("end of compact_table_incremental");
        assert!(
            fn_body.contains("MergeMode") && fn_body.contains("newer_than: watermark"),
            "scheduled incremental entry must always pass watermark"
        );
        assert!(
            prod.contains("Some(mode.newer_than())"),
            "merge CALL must wrap newer_than as Some(...)"
        );
        assert!(
            prod.contains("WaveKind::Closed") && prod.contains("WaveKind::Open"),
            "closed and open waves must share twcs_compact_waves"
        );
        assert!(
            !prod.contains("fn twcs_compact_closed_days")
                && !prod.contains("fn twcs_compact_open_day"),
            "duplicate wave loops must be removed"
        );
    }

    #[test]
    fn logical_row_probe_uses_physical_scope_qualification() {
        let prod = include_str!("merge.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        assert!(
            prod.contains("ducklake_qualified_table_name(scope, table)"),
            "inlined fragment probe must qualify via PhysicalScope"
        );
        assert!(
            !prod.contains("logical_table_row_count_sql(catalog_alias"),
            "must not build product-table probe from bare catalog_alias"
        );
        assert!(
            !prod.contains("load_inlined_fragment_stats(conn, scope.attach_alias()"),
            "must not pass attach_alias alone into the logical-row probe"
        );
        let sql = crate::sql::maintenance::logical_table_row_count_sql("softprobe.main.traces");
        assert!(
            sql.contains("FROM softprobe.main.traces"),
            "product probe SQL must keep catalog.schema.table: {sql}"
        );
        assert!(
            !sql.contains("FROM softprobe.traces ")
                && !sql.contains("FROM softprobe.traces\n")
                && !sql.ends_with("FROM softprobe.traces"),
            "must not elide schema for main in product probes: {sql}"
        );
    }
}
