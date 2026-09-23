use crate::compaction::twcs::{
    closed_day_live_file_count, closed_days_need_complete_merge, day_kind,
    ducklake_merge_adjacent_files_sql, live_file_count_sql, logical_table_row_count_sql,
    open_day_files_for_merge, open_day_max_compacted_files, partition_live_file_stats_sql,
    plan_twcs_merges, should_merge_partition, DayKind, InlinedFragmentStats, PartitionFileStats,
    TwcsMergePlan, TwcsPolicy,
};
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::workspace_scope::{PhysicalScope, DEFAULT_WORKSPACE_ID};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Utc};
use deadpool_postgres::Pool;
use duckdb::Connection;
use tracing::{info, warn};

/// Full ordered maintenance table list (traces / logs / scores).
pub fn maintenance_table_names() -> Vec<&'static str> {
    vec!["traces", "logs", "scores"]
}

#[derive(Clone)]
/// Physical-scope maintenance facade.
///
/// This is the only production owner of DuckDB connections used for
/// compaction, metadata cleanup, and session-summary reduction. The raw
/// connection helpers remain implementation details of this module.
pub struct MaintenanceEngine {
    config: Config,
    ducklake: crate::config::DuckLakeConfig,
    scope_registry: DuckLakeScopeResolver,
}

/// Opaque physical scope selected by `MaintenanceEngine`.
///
/// Callers can pass this capability back to maintenance operations, but cannot
/// inspect or replace its catalog configuration.
#[derive(Clone)]
pub struct MaintenanceScope {
    scope_key: String,
    ducklake: crate::config::DuckLakeConfig,
    pool: Pool,
}

#[derive(Debug, Clone)]
pub struct MaintenanceSummary {
    pub tables: Vec<TableMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub struct TableMaintenanceResult {
    pub table: String,
    pub metadata: MetadataMaintenanceResult,
    pub compaction: CompactionResult,
    pub rewrite_manifests: ActionResult,
    pub remove_orphan_files: ActionResult,
}

#[derive(Debug, Clone)]
pub struct MetadataMaintenanceResult {
    pub expired_snapshots: usize,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub status: CompactionStatus,
}

#[derive(Debug, Clone)]
pub struct ActionResult {
    pub status: ActionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStatus {
    Completed,
    Skipped,
    /// Attempted and failed (ops metrics: status=error).
    Failed,
    Unsupported,
}

/// Ops metric status for orphan cleanup: `None` = do not emit (disabled / no-op).
pub fn orphan_metric_status(enabled: bool, status: ActionStatus) -> Option<&'static str> {
    if !enabled {
        return None;
    }
    match status {
        ActionStatus::Completed => Some("ok"),
        ActionStatus::Failed | ActionStatus::Unsupported => Some("error"),
        ActionStatus::Skipped => None,
    }
}

/// Ops metric status for snapshot expire: `None` = do not emit (disabled).
pub fn snapshot_metric_status(enabled: bool, skipped: bool) -> Option<&'static str> {
    if !enabled {
        return None;
    }
    Some(if skipped { "error" } else { "ok" })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionStatus {
    Completed,
    Skipped,
    Unsupported,
}

impl MaintenanceEngine {
    pub(crate) fn from_config(config: &Config, scope_registry: DuckLakeScopeResolver) -> Self {
        Self {
            config: config.clone(),
            ducklake: config.ducklake.clone(),
            scope_registry,
        }
    }

    pub(crate) async fn new(
        config: &Config,
        scope_registry: DuckLakeScopeResolver,
    ) -> Result<Self> {
        Ok(Self::from_config(config, scope_registry))
    }

    /// Build a maintenance facade from the process engine manager.
    pub(crate) async fn from_engines(
        engines: &crate::runtime_engine::RuntimeEngineManager,
    ) -> Result<Self> {
        Self::new(engines.config(), engines.scope_registry().clone()).await
    }

    /// Validate every physical scope before the service reports readiness.
    /// This exercises the same open-and-attach path used by scheduled
    /// maintenance without running retention or compaction mutations.
    pub(crate) async fn validate_startup(&self) -> Result<()> {
        for (scope_key, ducklake) in self.physical_scopes().await? {
            let conn = self
                .open_ducklake_connection(&ducklake)
                .map_err(|error| anyhow!("maintenance open failed for {scope_key}: {error}"))?;
            self.attach_ducklake(&conn, &ducklake)
                .map_err(|error| anyhow!("maintenance attach failed for {scope_key}: {error}"))?;
        }
        Ok(())
    }

    pub async fn run_once(&self) -> Result<MaintenanceSummary> {
        self.run_pass(true).await
    }

    /// Metadata (expire + DuckLake file cleanup) always; TWCS/ladder only when
    /// `run_compaction` is true so the scheduler can expire every `A` without
    /// merging every metadata tick.
    pub async fn run_pass(&self, run_compaction: bool) -> Result<MaintenanceSummary> {
        crate::self_monitoring::record_maintenance();
        self.run_once_ducklake(run_compaction).await
    }

    pub async fn resolve_scope(&self, scope_key: &str) -> Result<MaintenanceScope> {
        let ducklake = self
            .workspace_scopes()
            .await?
            .into_iter()
            .find(|(key, _)| key == scope_key)
            .map(|(_, ducklake)| ducklake)
            .ok_or_else(|| anyhow!("unknown maintenance scope {scope_key}"))?;
        Ok(MaintenanceScope {
            scope_key: scope_key.to_string(),
            ducklake,
            pool: self.scope_registry.pool().clone(),
        })
    }

    /// Reduce dirty session summaries through the physical-scope maintenance
    /// boundary. The reducer may use DuckDB internally, but callers never
    /// receive its connection or choose its attach policy.
    pub async fn reduce_session_summary(
        &self,
        scope: &MaintenanceScope,
        max_sessions: u64,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        let pool = &scope.pool;
        crate::session_summary::reduce_tenant(
            pool,
            &scope.ducklake.metadata_schema,
            &scope.scope_key,
            &self.config,
            &scope.ducklake,
            max_sessions,
            max_reduce_span_seconds,
        )
        .await
    }

    /// Rebuild session summaries through the physical-scope maintenance
    /// boundary.
    pub async fn rebuild_session_summary(
        &self,
        scope: &MaintenanceScope,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        let pool = &scope.pool;
        crate::session_summary::rebuild_tenant_window(
            pool,
            &scope.ducklake.metadata_schema,
            &self.config,
            &scope.ducklake,
            &scope.scope_key,
            from,
            to,
            max_reduce_span_seconds,
        )
        .await
    }

    pub(crate) async fn workspace_scopes(
        &self,
    ) -> Result<Vec<(String, crate::config::DuckLakeConfig)>> {
        let mut scopes = Vec::new();
        let default = self.ducklake.clone();
        let mut saw_default = false;
        for (scope_id, scope) in self.scope_registry.list_scopes().await? {
            let mut dk = default.clone();
            dk.metadata_path = scope.metadata_path;
            dk.metadata_schema = scope.metadata_schema;
            dk.data_path = scope.data_path;
            dk.catalog_alias = scope.catalog_alias;
            if dk.metadata_schema == default.metadata_schema && dk.data_path == default.data_path {
                saw_default = true;
            }
            // scope_id is the tenant id used by RuntimeEngine / inventory gauges.
            scopes.push((scope_id, dk));
        }
        if !saw_default {
            scopes.insert(0, (DEFAULT_WORKSPACE_ID.to_string(), default));
        }
        Ok(scopes)
    }

    /// Return each physical DuckLake scope once. Workspace summary jobs use
    /// `workspace_scopes`; compaction and metadata jobs use this list so a
    /// shared physical scope is maintained once per pass.
    pub(crate) async fn physical_scopes(
        &self,
    ) -> Result<Vec<(String, crate::config::DuckLakeConfig)>> {
        Ok(deduplicate_physical_scopes(self.workspace_scopes().await?))
    }

    /// Run idempotent product-hot schema bootstrap once for a physical scope.
    /// Workspace summary jobs must not trigger this physical-scope operation.
    pub(crate) async fn ensure_physical_scope_bootstrap(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let scope = PhysicalScope::from_ducklake(ducklake);
        crate::session_summary::ensure_product_hot_attrs_for_scope(&self.scope_registry, &scope)
            .await?;
        Ok(())
    }

    async fn run_once_ducklake(&self, run_compaction: bool) -> Result<MaintenanceSummary> {
        let mut results = Vec::new();
        for (scope_key, ducklake) in self.physical_scopes().await? {
            let mut part = self
                .run_physical_scope_pass(&scope_key, &ducklake, run_compaction)
                .await?;
            results.append(&mut part);
        }
        Ok(MaintenanceSummary { tables: results })
    }

    /// One physical scope: TWCS/ladder (optional) + metadata expire/orphan.
    pub(crate) async fn run_physical_scope_pass(
        &self,
        scope_key: &str,
        ducklake: &crate::config::DuckLakeConfig,
        run_compaction: bool,
    ) -> Result<Vec<TableMaintenanceResult>> {
        // §7.2 pass order per physical scope:
        // 1 ensure PARTITIONED BY / SORTED BY
        // 2 TWCS merge (metrics family first, partition-scoped plans)
        // 3–5 downsample 5m → 1h → collapse
        // 6–7 expire snapshots + orphan cleanup (once per scope)
        let tables = maintenance_table_names();
        let mut results = Vec::new();
        let label = scope_key;
        let scope_start = std::time::Instant::now();
        let conn = match self.open_ducklake_connection(ducklake) {
            Ok(c) => c,
            Err(err) => {
                warn!("Maintenance open failed for scope {}: {}", label, err);
                crate::self_monitoring::record_compaction_pass(label, false);
                return Err(anyhow!("maintenance open failed for {label}: {err}"));
            }
        };
        if let Err(err) = self.attach_ducklake(&conn, ducklake) {
            warn!("Maintenance attach failed for scope {}: {}", label, err);
            crate::self_monitoring::record_compaction_pass(label, false);
            return Err(anyhow!("maintenance attach failed for {label}: {err}"));
        }

        let files_before = count_parquet_files_under(&ducklake.data_path);

        let mut compact_status: std::collections::HashMap<String, CompactionStatus> =
            std::collections::HashMap::new();

        if self.config.maintenance.enabled && run_compaction {
            // AC-F7 wait-for-next-run: do not flush catalog-inlined rows
            // before TWCS. Inlined rows stay readable via the catalog; TWCS
            // only merges Parquet that already exists (batches over the
            // inlining limit). Paying flush every pass is intentionally
            // avoided.
            for table in ["traces", "logs", "scores"] {
                let status = if self
                    .ducklake_table_exists(&conn, ducklake, table)
                    .unwrap_or(false)
                {
                    match self.ducklake_twcs_compact_table(&conn, ducklake, table, scope_key) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!(
                                "Maintenance TWCS merge failed for {}.{} ({}): {}",
                                ducklake.metadata_schema, table, label, err
                            );
                            CompactionStatus::Skipped
                        }
                    }
                } else {
                    CompactionStatus::Skipped
                };
                compact_status.insert(table.to_string(), status);
            }
        }

        // Expire + orphan cleanup once per scope (not once per table).
        let (metadata, remove_orphan_files) =
            self.run_scope_metadata_cleanup(&conn, ducklake, label);

        // Locked cardinality: status is ok|error only. Emit only when the
        // action was attempted — disabled/no-op must not mint series.
        let orphan_enabled = self.config.maintenance.metadata_enabled
            && self.config.maintenance.remove_orphan_files_enabled;
        if let Some(orphan_status) =
            orphan_metric_status(orphan_enabled, remove_orphan_files.status)
        {
            crate::self_monitoring::record_orphan_remove(scope_key, orphan_status);
        }
        if let Some(snap_status) =
            snapshot_metric_status(self.config.maintenance.metadata_enabled, metadata.skipped)
        {
            crate::self_monitoring::record_snapshot_expire(scope_key, snap_status);
        }
        crate::self_monitoring::record_compaction_pass(scope_key, true);
        let _ = scope_start;

        for table in &tables {
            let table_ident = format!("{}.{}", ducklake.metadata_schema, table);
            let compaction = CompactionResult {
                status: if self.config.maintenance.enabled {
                    compact_status
                        .get(*table)
                        .cloned()
                        .unwrap_or(CompactionStatus::Skipped)
                } else {
                    CompactionStatus::Skipped
                },
            };
            results.push(TableMaintenanceResult {
                table: table_ident,
                metadata: metadata.clone(),
                compaction,
                rewrite_manifests: ActionResult {
                    status: ActionStatus::Unsupported,
                },
                remove_orphan_files: remove_orphan_files.clone(),
            });
        }
        let files_after = count_parquet_files_under(&ducklake.data_path);
        warn_if_too_many_parquet_files(label, &ducklake.data_path, files_before, files_after);
        Ok(results)
    }

    fn run_scope_metadata_cleanup(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        label: &str,
    ) -> (MetadataMaintenanceResult, ActionResult) {
        let metadata = if self.config.maintenance.metadata_enabled {
            match self.ducklake_expire_snapshots(conn, ducklake) {
                Ok(expired) => MetadataMaintenanceResult {
                    expired_snapshots: expired,
                    skipped: false,
                },
                Err(err) => {
                    warn!("Maintenance metadata failed ({}): {}", label, err);
                    MetadataMaintenanceResult {
                        expired_snapshots: 0,
                        skipped: true,
                    }
                }
            }
        } else {
            MetadataMaintenanceResult {
                expired_snapshots: 0,
                skipped: true,
            }
        };

        let remove_orphan_files = if self.config.maintenance.metadata_enabled
            && self.config.maintenance.remove_orphan_files_enabled
        {
            match self.ducklake_cleanup_files(conn, ducklake) {
                Ok(()) => ActionResult {
                    status: ActionStatus::Completed,
                },
                Err(err) => {
                    warn!("Maintenance orphan cleanup failed ({}): {}", label, err);
                    ActionResult {
                        status: ActionStatus::Failed,
                    }
                }
            }
        } else {
            ActionResult {
                status: ActionStatus::Skipped,
            }
        };
        (metadata, remove_orphan_files)
    }
}

pub(crate) fn deduplicate_physical_scopes(
    workspace_scopes: Vec<(String, crate::config::DuckLakeConfig)>,
) -> Vec<(String, crate::config::DuckLakeConfig)> {
    let mut seen = std::collections::HashSet::new();
    let mut physical = Vec::new();
    for (_workspace_id, ducklake) in workspace_scopes {
        let key = PhysicalScope::from_ducklake(&ducklake).key();
        if seen.insert(key.clone()) {
            physical.push((key, ducklake));
        }
    }
    physical
}

impl MaintenanceEngine {
    fn twcs_policy(&self) -> TwcsPolicy {
        TwcsPolicy::from(&self.config.maintenance)
    }

    /// TWCS-shaped merge: closed days loop until the AC-F8 file bar (high but
    /// finite cap); open day stays bounded (AC-F4 / AC-Q9).
    ///
    /// Softprobe plans which calendar days need merge. Execution is unscoped
    /// `ducklake_merge_adjacent_files` — DuckLake has no day filter API here,
    /// and merges within `PARTITIONED BY (record_date)` (T-F6).
    fn ducklake_twcs_compact_table(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        tenant_id: &str,
    ) -> Result<CompactionStatus> {
        let policy = self.twcs_policy();
        let today = Utc::now().date_naive();
        let mut last = CompactionStatus::Skipped;

        let initial = self
            .load_partition_stats(conn, &ducklake.catalog_alias, table)
            .unwrap_or_default();
        // AC-F7: observe inlined backlog every pass (no watermark). When those
        // rows later become Parquet, the next pass's partition stats pick them up.
        if let Ok(Some(pending)) =
            self.load_inlined_fragment_stats(conn, &ducklake.catalog_alias, table)
        {
            info!(
                "TWCS backlog {}.{}: logical_rows={} live_parquet_files={} inlined_only={}",
                ducklake.metadata_schema,
                table,
                pending.logical_row_count,
                pending.live_parquet_files,
                pending.is_inlined_only()
            );
        }
        if initial.is_empty() {
            // Inline-only / empty stats: one merge may materialize Parquet.
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                policy.closed_day_max_compacted_files,
                policy.max_merge_file_size_bytes,
            )?;
        }

        last =
            self.twcs_compact_closed_days(conn, ducklake, table, today, last, &policy, tenant_id)?;
        last =
            self.twcs_compact_open_day(conn, ducklake, table, today, last, &policy, tenant_id)?;
        Ok(last)
    }

    #[allow(clippy::too_many_arguments)] // TWCS wave loop; tenant_id required for ops labels
    fn twcs_compact_closed_days(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        today: NaiveDate,
        mut last: CompactionStatus,
        policy: &TwcsPolicy,
        tenant_id: &str,
    ) -> Result<CompactionStatus> {
        for wave in 0..policy.closed_day_max_waves {
            let partitions = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            if !closed_days_need_complete_merge(&partitions, today) {
                return Ok(last);
            }
            let size_pressure = partitions.iter().any(|p| {
                p.total_bytes > 0
                    && p.live_file_count > 1
                    && p.total_bytes < policy.max_merge_file_size_bytes
            });
            let actions = plan_twcs_merges(&TwcsMergePlan {
                table,
                catalog_alias: &ducklake.catalog_alias,
                schema: &ducklake.metadata_schema,
                partitions: &partitions,
                today,
                size_pressure,
                max_compacted_files: policy.closed_day_max_compacted_files,
                policy,
            });
            let files_before = closed_day_live_file_count(&partitions, today);
            info!(
                "TWCS closed-day wave {}/{}: {} day(s) need work for {}.{} ({} closed files); max_compacted_files={}",
                wave + 1,
                policy.closed_day_max_waves,
                actions.len(),
                ducklake.metadata_schema,
                table,
                files_before,
                policy.closed_day_max_compacted_files
            );
            let wave_start = std::time::Instant::now();
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                policy.closed_day_max_compacted_files,
                policy.max_merge_file_size_bytes,
            )?;
            let files_after = closed_day_live_file_count(
                &self
                    .load_partition_stats(conn, &ducklake.catalog_alias, table)
                    .unwrap_or_default(),
                today,
            );
            crate::self_monitoring::record_compaction_wave(
                tenant_id,
                table,
                "closed",
                wave_start.elapsed(),
                files_before as u64,
                files_after as u64,
            );
            if last != CompactionStatus::Completed {
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
        Ok(last)
    }

    #[allow(clippy::too_many_arguments)] // TWCS wave loop; tenant_id required for ops labels
    fn twcs_compact_open_day(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        today: NaiveDate,
        mut last: CompactionStatus,
        policy: &TwcsPolicy,
        tenant_id: &str,
    ) -> Result<CompactionStatus> {
        for wave in 0..policy.max_waves_per_table {
            let partitions = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            let fallback = self
                .load_live_file_count(conn, &ducklake.catalog_alias, table)
                .ok();
            let files_before = open_day_files_for_merge(&partitions, today, fallback);
            let size_pressure = partitions.iter().any(|p| {
                p.total_bytes > 0
                    && p.live_file_count > 1
                    && p.total_bytes < policy.max_merge_file_size_bytes
            });
            let open_needs_merge = partitions.iter().any(|p| {
                day_kind(p.record_date, today) == DayKind::Open
                    && should_merge_partition(p, DayKind::Open, size_pressure, policy)
            });
            // Empty partition stats after a merge used to look like "done" and
            // stop at wave 1 while thousands of live files remained.
            if files_before <= policy.open_day_file_cap && !open_needs_merge {
                return Ok(last);
            }
            let max_compacted = open_day_max_compacted_files(files_before, policy);
            info!(
                "TWCS open-day wave {}/{}: {}.{} has {} live files (cap {}); max_compacted_files={}",
                wave + 1,
                policy.max_waves_per_table,
                ducklake.metadata_schema,
                table,
                files_before,
                policy.open_day_file_cap,
                max_compacted
            );
            let wave_start = std::time::Instant::now();
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                max_compacted,
                policy.max_merge_file_size_bytes,
            )?;
            let partitions_after = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            let fallback_after = self
                .load_live_file_count(conn, &ducklake.catalog_alias, table)
                .ok();
            let files_after = open_day_files_for_merge(&partitions_after, today, fallback_after);
            crate::self_monitoring::record_compaction_wave(
                tenant_id,
                table,
                "open",
                wave_start.elapsed(),
                files_before as u64,
                files_after as u64,
            );
            info!(
                "TWCS open-day wave {}/{} {}: status={:?} files {} → {}",
                wave + 1,
                policy.max_waves_per_table,
                table,
                last,
                files_before,
                files_after
            );
            if last == CompactionStatus::Unsupported {
                warn!(
                    "TWCS open-day merge {} unsupported; stopping waves at {} files",
                    table, files_after
                );
                return Ok(last);
            }
        }
        Ok(last)
    }

    fn load_live_file_count(
        &self,
        conn: &Connection,
        catalog_alias: &str,
        table: &str,
    ) -> Result<usize> {
        let sql = live_file_count_sql(catalog_alias, table);
        let n: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(n.max(0) as usize)
    }

    fn load_partition_stats(
        &self,
        conn: &Connection,
        catalog_alias: &str,
        table: &str,
    ) -> Result<Vec<PartitionFileStats>> {
        let sql = partition_live_file_stats_sql(catalog_alias, table);
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

    /// Softprobe backlog probe: logical rows vs live Parquet (AC-F7).
    fn load_inlined_fragment_stats(
        &self,
        conn: &Connection,
        catalog_alias: &str,
        table: &str,
    ) -> Result<Option<InlinedFragmentStats>> {
        let row_sql = logical_table_row_count_sql(catalog_alias, table);
        crate::sql::ensure_fact_scan_bound(&row_sql).map_err(|e| anyhow!("SQL gate: {e}"))?;
        let logical_rows: i64 = match conn.query_row(&row_sql, [], |row| row.get(0)) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    "TWCS logical-row probe failed for {}: {}; treating as empty",
                    table, err
                );
                return Ok(None);
            }
        };
        if logical_rows <= 0 {
            return Ok(None);
        }
        let files = self
            .load_live_file_count(conn, catalog_alias, table)
            .unwrap_or(0);
        Ok(Some(InlinedFragmentStats {
            table: table.to_string(),
            live_parquet_files: files,
            logical_row_count: logical_rows as u64,
        }))
    }

    fn ducklake_compact_table_wave(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        max_compacted_files: u64,
        max_file_size_bytes: u64,
    ) -> Result<CompactionStatus> {
        let policy = self.twcs_policy();
        let qualified = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, table);
        let scope = crate::storage::ducklake::ducklake_set_option_scope_for_qualified(&qualified);
        let target_file_size =
            crate::storage::ducklake::size_literal(self.config.maintenance.target_file_size_bytes);
        let set_target = format!(
            "CALL {}.set_option('target_file_size', '{}', {});",
            ducklake.catalog_alias, target_file_size, scope
        );
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
                return Ok(CompactionStatus::Skipped);
            }
            return Err(anyhow!(
                "DuckLake set_option failed for {}: {}",
                qualified,
                err
            ));
        }
        let sql = ducklake_merge_adjacent_files_sql(
            &ducklake.catalog_alias,
            table,
            &ducklake.metadata_schema,
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
                Ok(_) => return Ok(CompactionStatus::Completed),
                Err(err) if is_ducklake_serialization_conflict(&err) && wave < 2 => {
                    warn!(
                        "DuckLake compaction conflict on {} wave {}; backing off before retry: {}",
                        qualified, wave, err
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
                Err(err) if is_ducklake_serialization_conflict(&err) => {
                    warn!(
                        "DuckLake compaction skipped for {} due to transient metadata conflict: {}",
                        qualified, err
                    );
                    return Ok(CompactionStatus::Skipped);
                }
                Err(err) if is_ducklake_unsupported(&err) => {
                    warn!(
                        "DuckLake merge unsupported for {} (max_compacted_files={}): {}",
                        qualified, max_compacted_files, err
                    );
                    return Ok(CompactionStatus::Unsupported);
                }
                Err(err)
                    if is_ducklake_oom(&err)
                        && max_compacted_files > policy.max_compacted_files_per_wave =>
                {
                    warn!(
                        "DuckLake compaction OOM for {} at max_compacted_files={}; retrying with {}",
                        qualified, max_compacted_files, policy.max_compacted_files_per_wave
                    );
                    return self.ducklake_compact_table_wave(
                        conn,
                        ducklake,
                        table,
                        policy.max_compacted_files_per_wave,
                        max_file_size_bytes,
                    );
                }
                Err(err) => {
                    return Err(anyhow!(
                        "DuckLake compaction failed for {}.{}: {}",
                        ducklake.metadata_schema,
                        table,
                        err
                    ));
                }
            }
        }
        Ok(CompactionStatus::Skipped)
    }

    fn open_ducklake_connection(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<Connection> {
        let access = crate::workspace_scope::DuckLakeAccess::Physical(
            crate::workspace_scope::PhysicalScope::from_ducklake(ducklake),
        );
        crate::storage::ducklake::DuckLakeSessionFactory::new(&self.config).open(
            &access,
            crate::storage::ducklake::DuckLakeSessionKind::Maintenance,
        )
    }

    fn attach_ducklake(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let access = crate::workspace_scope::DuckLakeAccess::Physical(
            crate::workspace_scope::PhysicalScope::from_ducklake(ducklake),
        );
        crate::storage::ducklake::DuckLakeSessionFactory::new(&self.config)
            .attach(conn, &access)?;
        Ok(())
    }

    fn ducklake_table_exists(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<bool> {
        let qualified = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, table);
        let sql = format!("SELECT 1 FROM {qualified} LIMIT 0;");
        Ok(conn.execute_batch(&sql).is_ok())
    }

    fn ducklake_expire_snapshots(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<usize> {
        let age_seconds = self.config.maintenance.max_snapshot_age_seconds;
        let dry_run_sql = expire_snapshots_sql(&ducklake.catalog_alias, age_seconds, true);
        let planned = count_returned_rows(conn, &dry_run_sql)?;
        let sql = expire_snapshots_sql(&ducklake.catalog_alias, age_seconds, false);
        conn.execute_batch(&sql)?;
        Ok(planned)
    }

    fn ducklake_cleanup_files(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let age = self.config.maintenance.remove_orphan_older_than_seconds;
        // Only drain ducklake_files_scheduled_for_deletion. Do NOT CALL
        // ducklake_delete_orphaned_files here: with hive_file_pattern it treats
        // live tenant parquet as untracked and deletes them while catalog rows
        // remain, so Prom/Grafana fail with "Cannot open file".
        conn.execute_batch(&cleanup_old_files_sql(&ducklake.catalog_alias, age))?;
        Ok(())
    }
}

/// AC-N6: after a maintenance pass, live `ducklake_snapshot` count must be ≤ this.
pub const SNAPSHOT_COUNT_BAR_AFTER_PASS: usize = 50;

/// AC-N6 age bar: no live snapshot older than `A + I`.
pub fn snapshot_max_age_after_pass_seconds(
    max_snapshot_age_seconds: u64,
    interval_seconds: u64,
) -> u64 {
    max_snapshot_age_seconds.saturating_add(interval_seconds)
}

/// DuckLake `older_than` interval from an age in seconds (no day flooring).
fn ducklake_older_than_interval(age_seconds: u64) -> String {
    format!("INTERVAL '{} seconds'", age_seconds)
}

pub(crate) fn expire_snapshots_sql(
    catalog_alias: &str,
    max_snapshot_age_seconds: u64,
    dry_run: bool,
) -> String {
    let interval = ducklake_older_than_interval(max_snapshot_age_seconds);
    // older_than is TIMESTAMP WITH TIME ZONE — use now(), not CAST(... AS TIMESTAMP).
    if dry_run {
        format!(
            "CALL ducklake_expire_snapshots('{}', dry_run => true, older_than => now() - {});",
            catalog_alias, interval
        )
    } else {
        format!(
            "CALL ducklake_expire_snapshots('{}', older_than => now() - {});",
            catalog_alias, interval
        )
    }
}

pub(crate) fn cleanup_old_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
    ducklake_file_cleanup_sql(
        "ducklake_cleanup_old_files",
        catalog_alias,
        older_than_seconds,
    )
}

/// Untracked parquet on the data path (`ducklake_delete_orphaned_files`).
/// Not invoked by the scheduler — hive live files look untracked (see cleanup).
#[allow(dead_code)]
pub(crate) fn delete_orphaned_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
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
    if older_than_seconds == 0 {
        format!("CALL {function}('{catalog_alias}', cleanup_all => true);")
    } else {
        let interval = ducklake_older_than_interval(older_than_seconds);
        format!("CALL {function}('{catalog_alias}', older_than => now() - {interval});")
    }
}

fn is_ducklake_unsupported(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("catalog error")
        || msg.contains("function") && (msg.contains("does not exist") || msg.contains("not found"))
        || msg.contains("no function matches")
        || msg.contains("not implemented")
}

fn is_ducklake_serialization_conflict(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("could not serialize access due to concurrent update")
        || msg.contains("serialization failure")
}

fn is_ducklake_oom(err: &duckdb::Error) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("out of memory")
}

/// Inner attempts per merge wave. Paired with a second wave in
/// [`MaintenanceEngine::ducklake_compact_table_wave`].
const COMPACTION_SERIALIZATION_ATTEMPTS: usize = 8;

/// Soft warn when a scope still has many Parquet files after a maintenance pass.
const PARQUET_FILE_WARN_THRESHOLD: usize = 200;

fn count_parquet_files_under(data_path: &str) -> usize {
    let root = std::path::Path::new(data_path);
    if !root.exists() {
        return 0;
    }
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
                e.eq_ignore_ascii_case("parquet") || e.eq_ignore_ascii_case("parq")
            }) {
                count += 1;
            }
        }
    }
    count
}

fn warn_if_too_many_parquet_files(
    scope_label: &str,
    data_path: &str,
    files_before: usize,
    files_after: usize,
) {
    if files_after >= PARQUET_FILE_WARN_THRESHOLD {
        warn!(
            "DuckLake scope {} still has {} parquet files under {} after maintenance (was {}); \
             query scans may stay expensive — check ingest batching / compaction conflicts",
            scope_label, files_after, data_path, files_before
        );
    } else if files_before > files_after {
        info!(
            "DuckLake scope {} parquet files {} → {} under {}",
            scope_label, files_before, files_after, data_path
        );
    }
}

fn execute_batch_with_serialization_retry(
    conn: &Connection,
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
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                backoff_ms = (backoff_ms.saturating_mul(2)).min(2_000);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn count_returned_rows(conn: &Connection, sql: &str) -> Result<usize> {
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
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn maintenance_does_not_flush_inlined_before_twcs() {
        // Production source only (tests module may mention flush by name).
        let prod = include_str!("executor.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("cfg(test) marker");
        assert!(
            !prod.contains("ducklake_flush_inlined_data"),
            "AC-F7 wait-for-next-run: maintenance must not flush catalog-inlined rows before TWCS"
        );
        assert!(
            !prod.contains("flush_inlined"),
            "AC-F7: flush-before-TWCS helpers must stay removed from maintenance production code"
        );
    }

    #[test]
    fn maintenance_table_order_is_traces_logs_scores() {
        assert_eq!(maintenance_table_names(), &["traces", "logs", "scores"]);
    }

    #[test]
    fn physical_scope_maintenance_deduplicates_shared_bindings() {
        let shared = crate::config::DuckLakeConfig {
            metadata_schema: "shared_scope".into(),
            data_path: "s3://warehouse/shared".into(),
            ..crate::config::DuckLakeConfig::default()
        };
        let isolated = crate::config::DuckLakeConfig {
            metadata_schema: "isolated_scope".into(),
            data_path: "s3://warehouse/isolated".into(),
            ..shared.clone()
        };
        let scopes = deduplicate_physical_scopes(vec![
            ("workspace-a".into(), shared.clone()),
            ("workspace-b".into(), shared),
            ("workspace-c".into(), isolated),
        ]);
        assert_eq!(scopes.len(), 2);
        assert!(scopes[0].0.starts_with("ducklake:"));
        assert!(scopes[1].0.starts_with("ducklake:"));
    }

    #[test]
    fn count_parquet_files_under_walks_nested_dirs() {
        let tmp = TempDir::new().expect("temp");
        let nested = tmp.path().join("metrics").join("record_date=2026-08-14");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("a.parquet"), b"x").unwrap();
        fs::write(nested.join("b.parq"), b"y").unwrap();
        fs::write(nested.join("ignore.txt"), b"z").unwrap();
        assert_eq!(count_parquet_files_under(tmp.path().to_str().unwrap()), 2);
        assert_eq!(count_parquet_files_under("/no/such/path"), 0);
    }

    #[test]
    fn parquet_warn_threshold_is_sane() {
        assert_eq!(COMPACTION_SERIALIZATION_ATTEMPTS, 8);
    }

    /// AC-N2 / T-N2: 3600s must become a seconds interval, not `INTERVAL '1 days'`.
    #[test]
    fn expire_snapshots_sql_honors_seconds() {
        let dry = expire_snapshots_sql("softprobe", 3600, true);
        let live = expire_snapshots_sql("softprobe", 3600, false);
        for sql in [&dry, &live] {
            assert!(
                sql.contains("INTERVAL '3600 seconds'"),
                "expected seconds interval, got: {sql}"
            );
            assert!(
                !sql.contains("days"),
                "must not day-floor snapshot expiry: {sql}"
            );
        }
        assert!(dry.contains("dry_run => true"));
        assert!(!live.contains("dry_run"));

        // Strengthen AC-N2/N5: sub-hour ages stay in seconds (no day floor).
        for age in [60u64, 1u64] {
            let sql = expire_snapshots_sql("softprobe", age, false);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }

    /// AC-N6 / T-N6: expire uses A seconds; count bar is 50; remaining age < A+I.
    #[test]
    fn expire_snapshots_sql_honors_n6_count_and_age_bars() {
        let cfg = crate::config::Config::default();
        assert_eq!(cfg.maintenance.max_snapshot_age_seconds, 60);
        assert_eq!(cfg.maintenance.interval_seconds, 60);
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
        assert!(
            sql.contains("INTERVAL '60 seconds'"),
            "AC-N6 expiry must use A=60 seconds, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor snapshot expiry: {sql}"
        );
        assert!(
            cfg.maintenance.max_snapshot_age_seconds < 3600,
            "AC-N1/N6: default A must stay 60s, not 1h"
        );
    }

    /// AC-N5 / T-N5: orphan cleanup older_than uses seconds, not day floor.
    #[test]
    fn cleanup_old_files_sql_honors_seconds() {
        let sql = cleanup_old_files_sql("softprobe", 3600);
        assert!(
            sql.contains("INTERVAL '3600 seconds'"),
            "expected seconds interval, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor orphan cleanup: {sql}"
        );
        let all = cleanup_old_files_sql("softprobe", 0);
        assert!(all.contains("cleanup_all => true"));
        assert!(!all.contains("older_than"));

        for age in [60u64, 1u64] {
            let sql = cleanup_old_files_sql("softprobe", age);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }

    /// Maintenance must use scheduled-file cleanup only — never the orphan
    /// sweeper (hive live files look untracked and get deleted).
    #[test]
    fn maintenance_file_cleanup_is_scheduled_only() {
        let scheduled = cleanup_old_files_sql("softprobe", 60);
        let orphan = delete_orphaned_files_sql("softprobe", 60);
        assert!(scheduled.contains("ducklake_cleanup_old_files"));
        assert!(!scheduled.contains("delete_orphaned"));
        assert!(
            orphan.contains("ducklake_delete_orphaned_files"),
            "helper exists for manual/ops use, not the scheduler"
        );
    }

    /// DuckLake leaves untracked parquet until this CALL (not automatic).
    #[test]
    fn delete_orphaned_files_sql_honors_seconds() {
        let sql = delete_orphaned_files_sql("softprobe", 3600);
        assert!(
            sql.contains("ducklake_delete_orphaned_files"),
            "expected DuckLake orphan API, got: {sql}"
        );
        assert!(
            sql.contains("INTERVAL '3600 seconds'"),
            "expected seconds interval, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor orphan delete: {sql}"
        );
        let all = delete_orphaned_files_sql("softprobe", 0);
        assert!(all.contains("cleanup_all => true"));
        assert!(!all.contains("older_than"));
        for age in [60u64, 1u64] {
            let sql = delete_orphaned_files_sql("softprobe", age);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }

    #[test]
    fn orphan_metric_status_emit_rules() {
        assert_eq!(orphan_metric_status(false, ActionStatus::Completed), None);
        assert_eq!(orphan_metric_status(false, ActionStatus::Failed), None);
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Completed),
            Some("ok")
        );
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Failed),
            Some("error")
        );
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Unsupported),
            Some("error")
        );
        assert_eq!(orphan_metric_status(true, ActionStatus::Skipped), None);
    }

    #[test]
    fn snapshot_metric_status_emit_rules() {
        assert_eq!(snapshot_metric_status(false, true), None);
        assert_eq!(snapshot_metric_status(false, false), None);
        assert_eq!(snapshot_metric_status(true, false), Some("ok"));
        assert_eq!(snapshot_metric_status(true, true), Some("error"));
    }

    #[tokio::test]
    async fn run_tenant_pass_attach_failure_is_err() {
        let mut cfg = Config::default();
        cfg.maintenance.enabled = false;
        cfg.maintenance.metadata_enabled = false;
        let resolver = crate::runtime_engine::DuckLakeScopeResolver::connect(&cfg)
            .await
            .expect("connect resolver");
        let executor = MaintenanceEngine::new(&cfg, resolver)
            .await
            .expect("executor");
        let mut ducklake = cfg.ducklake.clone();
        // Parent path is a file → prepare_local_ducklake_paths fails → attach Err.
        let blocker = tempfile::NamedTempFile::new().expect("blocker file");
        ducklake.data_path = format!("{}/data/", blocker.path().display());
        let err = executor
            .run_physical_scope_pass("t-attach-fail", &ducklake, false)
            .await
            .expect_err("attach must Err");
        assert!(
            err.to_string().contains("attach failed") || err.to_string().contains("open failed"),
            "unexpected: {err}"
        );
    }
}
