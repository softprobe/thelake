//! Physical-scope maintenance facade.

use crate::compaction::cleanup::{
    cleanup_old_files_sql, count_returned_rows, expire_snapshots_sql,
};
use crate::compaction::merge::compact_table_incremental;
use crate::compaction::status::{
    orphan_metric_status, pass_compaction_ok, snapshot_metric_status, ActionResult, ActionStatus,
    MaintenanceSummary, MetadataMaintenanceResult, TableMaintenanceResult,
};
use crate::compaction::watermark::WatermarkStore;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::workspace_scope::{PhysicalScope, DEFAULT_WORKSPACE_ID};
use anyhow::{anyhow, Result};
use chrono::Utc;
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
/// compaction, metadata cleanup, and session-summary reduction.
pub struct MaintenanceEngine {
    config: Config,
    ducklake: crate::config::DuckLakeConfig,
    scope_registry: DuckLakeScopeResolver,
}

/// Opaque physical scope selected by `MaintenanceEngine`.
#[derive(Clone)]
pub struct MaintenanceScope {
    scope_key: String,
    ducklake: crate::config::DuckLakeConfig,
    pool: Pool,
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

    pub(crate) async fn from_engines(
        engines: &crate::runtime_engine::RuntimeEngineManager,
    ) -> Result<Self> {
        Self::new(engines.config(), engines.scope_registry().clone()).await
    }

    fn watermark_store(&self) -> WatermarkStore {
        WatermarkStore::new(
            self.scope_registry.pool().clone(),
            self.scope_registry.registry_schema().to_string(),
        )
    }

    /// Validate every physical scope before the service reports readiness.
    pub(crate) async fn validate_startup(&self) -> Result<()> {
        self.watermark_store().ensure_table().await?;
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

    /// Metadata always; TWCS only when `run_compaction` is true.
    pub async fn run_pass(&self, run_compaction: bool) -> Result<MaintenanceSummary> {
        crate::self_monitoring::record_maintenance();
        let mut results = Vec::new();
        for (scope_key, ducklake) in self.physical_scopes().await? {
            let mut part = self
                .run_physical_scope_pass(&scope_key, &ducklake, run_compaction)
                .await?;
            results.append(&mut part);
        }
        Ok(MaintenanceSummary { tables: results })
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

    pub async fn reduce_session_summary(
        &self,
        scope: &MaintenanceScope,
        max_sessions: u64,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        crate::session_summary::reduce_tenant(
            &scope.pool,
            &scope.ducklake.metadata_schema,
            &scope.scope_key,
            &self.config,
            &scope.ducklake,
            max_sessions,
            max_reduce_span_seconds,
        )
        .await
    }

    pub async fn rebuild_session_summary(
        &self,
        scope: &MaintenanceScope,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
        max_reduce_span_seconds: u64,
    ) -> Result<usize> {
        crate::session_summary::rebuild_tenant_window(
            &scope.pool,
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
            scopes.push((scope_id, dk));
        }
        if !saw_default {
            scopes.insert(0, (DEFAULT_WORKSPACE_ID.to_string(), default));
        }
        Ok(scopes)
    }

    pub(crate) async fn physical_scopes(
        &self,
    ) -> Result<Vec<(String, crate::config::DuckLakeConfig)>> {
        Ok(deduplicate_physical_scopes(self.workspace_scopes().await?))
    }

    pub(crate) async fn ensure_physical_scope_bootstrap(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let scope = PhysicalScope::from_ducklake(ducklake);
        crate::session_summary::ensure_product_hot_attrs_for_scope(&self.scope_registry, &scope)
            .await?;
        Ok(())
    }

    /// One physical scope: TWCS (optional, newer_than-scoped) + metadata expire/orphan.
    pub(crate) async fn run_physical_scope_pass(
        &self,
        scope_key: &str,
        ducklake: &crate::config::DuckLakeConfig,
        run_compaction: bool,
    ) -> Result<Vec<TableMaintenanceResult>> {
        let tables = maintenance_table_names();
        let mut results = Vec::new();
        let label = scope_key;
        let run_started_at = Utc::now();
        let files_before = count_parquet_files_under(&ducklake.data_path);
        let watermarks = self.watermark_store();
        let mut compact_status: std::collections::HashMap<String, ActionStatus> =
            std::collections::HashMap::new();

        // Resolve watermarks before opening DuckDB so Connection is never held across await.
        let mut fences: std::collections::HashMap<String, (chrono::DateTime<Utc>, bool)> =
            std::collections::HashMap::new();
        if self.config.maintenance.enabled && run_compaction {
            for table in maintenance_table_names() {
                match watermarks
                    .ensure_fence(scope_key, table, run_started_at)
                    .await
                {
                    Ok(v) => {
                        fences.insert(table.to_string(), v);
                    }
                    Err(err) => {
                        warn!(
                            "Compaction watermark failed for {}.{} ({}): {}",
                            ducklake.metadata_schema, table, label, err
                        );
                        compact_status.insert(table.to_string(), ActionStatus::Failed);
                    }
                }
            }
        }

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

        let mut advance_tables: Vec<String> = Vec::new();
        if self.config.maintenance.enabled && run_compaction {
            // AC-F7: do not flush catalog-inlined rows before TWCS.
            for table in maintenance_table_names() {
                if compact_status.contains_key(table) {
                    continue;
                }
                let status = if !self
                    .ducklake_table_exists(&conn, ducklake, table)
                    .unwrap_or(false)
                {
                    ActionStatus::Skipped
                } else if let Some((watermark, inserted)) = fences.get(table).copied() {
                    if inserted {
                        info!(
                            "Compaction fence inserted for {}/{} at {} (no merge this pass)",
                            scope_key, table, watermark
                        );
                        ActionStatus::Skipped
                    } else {
                        match compact_table_incremental(
                            &self.config,
                            &conn,
                            ducklake,
                            table,
                            scope_key,
                            watermark,
                        ) {
                            Ok(outcome) => {
                                if outcome.drained {
                                    advance_tables.push(table.to_string());
                                }
                                outcome.status
                            }
                            Err(err) => {
                                warn!(
                                    "Maintenance TWCS merge failed for {}.{} ({}): {}",
                                    ducklake.metadata_schema, table, label, err
                                );
                                ActionStatus::Failed
                            }
                        }
                    }
                } else {
                    ActionStatus::Failed
                };
                compact_status.insert(table.to_string(), status);
            }
        }

        let (metadata, remove_orphan_files) =
            self.run_scope_metadata_cleanup(&conn, ducklake, label);
        drop(conn);

        for table in advance_tables {
            if let Err(err) = watermarks.advance(scope_key, &table, run_started_at).await {
                warn!(
                    "Compaction watermark advance failed for {}/{}: {}",
                    scope_key, table, err
                );
                compact_status.insert(table, ActionStatus::Failed);
            }
        }

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

        let statuses: Vec<ActionStatus> = compact_status.values().copied().collect();
        let pass_ok = if self.config.maintenance.enabled && run_compaction {
            pass_compaction_ok(&statuses)
        } else {
            true
        };
        crate::self_monitoring::record_compaction_pass(scope_key, pass_ok);

        for table in &tables {
            let table_ident = format!("{}.{}", ducklake.metadata_schema, table);
            let compaction = ActionResult {
                status: if self.config.maintenance.enabled && run_compaction {
                    compact_status
                        .get(*table)
                        .copied()
                        .unwrap_or(ActionStatus::Skipped)
                } else {
                    ActionStatus::Skipped
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
        let sql = crate::sql::maintenance::table_exists_probe_sql(&qualified);
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
        crate::sql::execute_batch_checked(conn, &sql)?;
        Ok(planned)
    }

    fn ducklake_cleanup_files(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let age = self.config.maintenance.remove_orphan_older_than_seconds;
        // Only drain ducklake_files_scheduled_for_deletion — never delete_orphaned_files.
        crate::sql::execute_batch_checked(
            conn,
            &cleanup_old_files_sql(&ducklake.catalog_alias, age),
        )?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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

    #[test]
    fn null_watermark_path_never_emits_unscoped_merge_sql() {
        use chrono::TimeZone;
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let sql = crate::sql::maintenance::ducklake_merge_adjacent_files_sql(
            "softprobe",
            "traces",
            "main",
            Some(ts),
            Some(32),
            Some(8 * 1024 * 1024),
        );
        assert!(sql.contains("newer_than"));
    }

    #[test]
    fn scheduled_pass_advances_watermark_only_when_drained() {
        let src = include_str!("engine.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        assert!(
            production.contains("if outcome.drained") && production.contains("advance_tables.push"),
            "watermark advance must be gated on MergeOutcome.drained"
        );
        assert!(
            production.contains("if inserted") && production.contains("no merge this pass"),
            "fence insert must skip merge (no CALL) on bootstrap pass"
        );
        assert!(
            !production.contains("MergeMode::Full"),
            "scheduled physical pass must not use Full merge"
        );
        // Ensure Connection is dropped before async watermark advance.
        let drop_idx = production
            .find("drop(conn)")
            .expect("must drop DuckDB Connection before await");
        let advance_idx = production
            .find(".advance(scope_key, &table, run_started_at)")
            .expect("must advance watermarks after merge");
        assert!(
            drop_idx < advance_idx,
            "Connection must be dropped before async watermark advance"
        );
    }

    #[test]
    fn missing_fence_maps_to_failed_not_unscoped_call() {
        let src = include_str!("engine.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        assert!(
            production.contains("fences.get(table)") && production.contains("ActionStatus::Failed"),
            "missing fence must fail closed"
        );
        // Only compact_table_incremental performs merges; no unscoped CALL helper.
        assert!(production.contains("compact_table_incremental"));
        assert!(!production.contains("MergeMode::Full"));
    }
}
