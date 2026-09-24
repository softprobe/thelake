use crate::config::Config;
use crate::promotion::{BusinessTableManifest, TelemetryColumnsManifest, TelemetryTable};
use crate::sql::schema::{insert_order_by, is_otlp_table, LOGS, SCORES, SCORE_CONFIGS, TRACES};
use crate::storage::schema::otlp_layout::ensure_otlp_table_partition_sort;
use crate::storage::schema::tables::{OtlpLogsTable, ScoreConfigTable, ScoreTable, TraceTable};
use crate::storage::schema::variant::parquet_select_for_table;
use crate::workspace_scope::{DuckLakeAccess, PhysicalScope, WorkspaceBinding};
use ::arrow::datatypes::Schema;
use ::arrow::record_batch::RecordBatch;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use tracing::{info, warn};

static RESET_LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();

use super::attach::{ducklake_qualified_table_name, ducklake_set_option_scope_for_qualified};
use super::util::{
    ensure_hot_map_column_types, ensure_log_timestamp_precision, ensure_trace_fidelity_columns,
    ensure_trace_timestamp_precision, escape_sql_literal, size_literal,
};

pub(super) struct TableReadinessRegistry {
    ready_tables: RwLock<HashSet<String>>,
}

impl TableReadinessRegistry {
    pub(super) fn new() -> Self {
        Self {
            ready_tables: RwLock::new(HashSet::new()),
        }
    }

    pub(super) fn is_ready(&self, table_name: &str) -> bool {
        self.ready_tables
            .read()
            .map(|r| r.contains(table_name))
            .unwrap_or(false)
    }

    pub(super) fn mark_ready(&self, table_name: &str) {
        if let Ok(mut w) = self.ready_tables.write() {
            w.insert(table_name.to_string());
        }
    }

    pub(super) fn clear(&self) {
        if let Ok(mut w) = self.ready_tables.write() {
            w.clear();
        }
    }
}

pub(super) struct WriterPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
    registry: TableReadinessRegistry,
    table_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl WriterPool {
    pub(super) fn new(conns: Vec<Mutex<Connection>>) -> Self {
        Self {
            conns,
            next: AtomicUsize::new(0),
            registry: TableReadinessRegistry::new(),
            table_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn with_conn<R>(&self, f: impl FnOnce(&Connection) -> Result<R>) -> Result<R> {
        let n = self.conns.len();
        if n == 0 {
            return Err(anyhow!("DuckLake writer pool is empty"));
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(guard) = self.conns[idx].try_lock() {
                return f(&guard);
            }
        }
        let guard = self.conns[start]
            .lock()
            .map_err(|_| anyhow!("DuckLake writer connection lock poisoned"))?;
        f(&guard)
    }

    pub(super) fn is_table_ready(&self, table_name: &str) -> bool {
        self.registry.is_ready(table_name)
    }

    pub(super) fn mark_table_ready(&self, table_name: &str) {
        self.registry.mark_ready(table_name);
    }

    pub(super) fn clear_ready(&self) {
        self.registry.clear();
    }

    pub(super) fn table_lock(&self, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self
            .table_locks
            .lock()
            .map_err(|_| anyhow!("table locks map lock poisoned"))
            .unwrap();
        map.entry(table_name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

pub(crate) struct DuckLakeWriter {
    pub(super) config: Config,
    /// Single write-path identity token (workspace + physical scope + mode).
    pub(super) binding: WorkspaceBinding,
    /// ATTACH capability built once from [`Self::binding`] at construction.
    pub(super) access: DuckLakeAccess,
    /// Per catalog-scope pools of reused ATTACH'd DuckDB connections.
    pub(super) writer_pools: Mutex<HashMap<String, Arc<WriterPool>>>,
}

impl DuckLakeWriter {
    pub(crate) fn physical_scope(&self) -> &PhysicalScope {
        self.access.physical_scope()
    }

    pub(crate) fn workspace_scope_mode(&self) -> crate::workspace_scope::WorkspaceScopeMode {
        self.binding.mode
    }

    pub(super) fn shared_workspace_id(&self) -> Result<Option<&str>> {
        if self.workspace_scope_mode() != crate::workspace_scope::WorkspaceScopeMode::Shared {
            return Ok(None);
        }
        Ok(Some(self.binding.workspace_id.as_str()))
    }

    pub(super) fn validate_shared_ownership(
        &self,
        tenant_id: Option<&str>,
        record_kind: &str,
    ) -> Result<()> {
        let Some(workspace_id) = self.shared_workspace_id()? else {
            return Ok(());
        };
        if tenant_id != Some(workspace_id) {
            return Err(anyhow!(
                "shared {record_kind} writes require tenant_id to match the authenticated workspace"
            ));
        }
        Ok(())
    }

    /// Construct a writer permanently bound to one [`WorkspaceBinding`].
    ///
    /// Warms the writer pool. Dev reset (`SPLAKE_RESET_DUCKLAKE=1`) is not performed
    /// here — callers load manifests then invoke [`Self::apply_dev_reset_if_requested`].
    pub async fn new(config: &Config, binding: WorkspaceBinding) -> Result<Self> {
        let access = DuckLakeAccess::Workspace(binding.clone());
        let writer = Self {
            config: config.clone(),
            binding,
            access,
            writer_pools: Mutex::new(HashMap::new()),
        };
        let _ = writer.get_or_create_pool(writer.physical_scope())?;
        info!(
            "DuckLake writer initialized (workspace_id={}, writer_pool_size={})",
            writer.binding.workspace_id,
            writer.config.ducklake.effective_writer_pool_size()
        );
        Ok(writer)
    }

    /// Under `SPLAKE_RESET_DUCKLAKE=1`, atomically wipe tables, ensure traces/logs,
    /// and reapply the provided promotion manifests (all under the per-scope reset lock).
    pub async fn apply_dev_reset_if_requested(
        &self,
        telemetry: &[TelemetryColumnsManifest],
        business: &[BusinessTableManifest],
    ) -> Result<()> {
        if std::env::var("SPLAKE_RESET_DUCKLAKE").ok().as_deref() != Some("1") {
            return Ok(());
        }
        let scope = self.physical_scope();
        let reset_lock = RESET_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let reset_lock = {
            let mut locks = reset_lock
                .lock()
                .map_err(|_| anyhow!("DuckLake reset lock map poisoned"))?;
            locks
                .entry(Self::conn_cache_key(scope))
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _reset_guard = reset_lock.lock().await;
        let pool = self.get_or_create_pool(scope)?;
        pool.with_conn(|conn| self.reset_tables_for_dev(conn))?;
        self.warm_pool(&pool, scope)?;
        for table in [TelemetryTable::Traces, TelemetryTable::Logs] {
            self.ensure_telemetry_table_for(&table).await?;
        }
        // Same pool map entry as above; reapply stays under `_reset_guard`.
        self.reapply_promotions(telemetry, business).await?;
        Ok(())
    }

    pub(crate) async fn reapply_promotions(
        &self,
        telemetry: &[TelemetryColumnsManifest],
        business: &[BusinessTableManifest],
    ) -> Result<()> {
        let pool = self.get_or_create_pool(self.physical_scope())?;
        self.reapply_promotions_on_pool(&pool, telemetry, business)
            .await
    }

    async fn reapply_promotions_on_pool(
        &self,
        pool: &WriterPool,
        telemetry: &[TelemetryColumnsManifest],
        business: &[BusinessTableManifest],
    ) -> Result<()> {
        let prefix = self.physical_scope().catalog_prefix();
        for manifest in telemetry {
            for ddl in crate::promotion::telemetry_column_add_ddls(&prefix, manifest)
                .map_err(|err| anyhow!("failed to rebuild telemetry promotion: {err}"))?
            {
                pool.with_conn(|conn| {
                    conn.execute_batch(&ddl)
                        .map_err(|err| anyhow!("failed to rebuild telemetry promotion: {err}"))
                })?;
            }
        }
        for manifest in business {
            let ddls = crate::promotion::business_table_create_ddls(&prefix, manifest)
                .map_err(|err| anyhow!("failed to rebuild business promotion: {err}"))?;
            for ddl in ddls {
                pool.with_conn(|conn| {
                    conn.execute_batch(&ddl)
                        .map_err(|err| anyhow!("failed to rebuild business promotion: {err}"))
                })?;
            }
        }
        Ok(())
    }

    pub(super) fn conn_cache_key(scope: &PhysicalScope) -> String {
        scope.writer_pool_key()
    }

    pub(super) fn get_or_create_pool(&self, scope: &PhysicalScope) -> Result<Arc<WriterPool>> {
        let key = Self::conn_cache_key(scope);
        let (pool, is_new) = {
            let mut guard = self
                .writer_pools
                .lock()
                .map_err(|_| anyhow!("DuckLake writer pool map lock poisoned"))?;
            if let Some(pool) = guard.get(&key) {
                (Arc::clone(pool), false)
            } else {
                let size = self.config.ducklake.effective_writer_pool_size();
                let mut conns = Vec::with_capacity(size);
                // Attach sequentially: the first connection initializes DuckLake metadata tables; later
                // pool members ATTACH the already-initialized Postgres schema (with retry on races).
                for _ in 0..size {
                    let conn = self.open_connection_for(scope)?;
                    self.attach_ducklake_for(&conn, scope)?;
                    self.ensure_schema_for(&conn, scope)?;
                    conns.push(Mutex::new(conn));
                }
                let pool = Arc::new(WriterPool::new(conns));
                guard.insert(key, Arc::clone(&pool));
                (pool, true)
            }
        };

        if is_new {
            // Warm tables outside the global writer_pools lock so other scopes are never blocked.
            self.warm_pool(&pool, scope)?;
        }
        Ok(pool)
    }

    pub(super) fn warm_pool(&self, pool: &WriterPool, scope: &PhysicalScope) -> Result<()> {
        // Do not ensure/mark OTLP tables ready here. First write must call
        // `ensure_table_with_conn` with the write-time Arrow schema so promotion
        // columns (and other evolution) are ADDed before INSERT. Marking ready
        // with the base schema skips that evolution (see promotion_telemetry_ingest).
        let _ = (pool, scope);
        Ok(())
    }

    /// DuckDB type for `ALTER TABLE … ADD COLUMN` evolution.
    ///
    /// MAP bags must not fall through to `VARCHAR`. LIST columns (e.g. events)
    /// are owned by fidelity helpers — refuse here rather than invent a wrong type.
    fn arrow_field_to_duck_add_type(field: &::arrow::datatypes::Field) -> Result<&'static str> {
        use ::arrow::datatypes::{DataType, TimeUnit};
        match field.data_type() {
            DataType::Utf8 => Ok("VARCHAR"),
            DataType::Boolean => Ok("BOOLEAN"),
            DataType::Int64 => Ok("BIGINT"),
            DataType::Int32 => Ok("INTEGER"),
            DataType::Float64 => Ok("DOUBLE"),
            DataType::Date32 => Ok("DATE"),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok("TIMESTAMP_NS"),
            DataType::Timestamp(_, _) => Ok("TIMESTAMPTZ"),
            DataType::Map(_, _) => Ok("MAP(VARCHAR, VARCHAR)"),
            DataType::List(_) => Err(anyhow!(
                "skip LIST field '{}' in generic ADD COLUMN — fidelity helpers own it",
                field.name()
            )),
            other => Err(anyhow!(
                "refusing ADD COLUMN for field '{}' with unsupported Arrow type {other:?} \
                 (do not default to VARCHAR)",
                field.name()
            )),
        }
    }

    pub(super) fn ensure_table_with_conn(
        conn: &Connection,
        scope: &PhysicalScope,
        table_name: &str,
        custom_schema: Option<&Arc<Schema>>,
        target_file_size_bytes: usize,
    ) -> Result<()> {
        let qualified_table = ducklake_qualified_table_name(scope, table_name);

        let (arrow_schema, select_prefix) = match table_name {
            "traces" => (
                custom_schema
                    .cloned()
                    .unwrap_or_else(|| Arc::new(TraceTable::schema())),
                parquet_select_for_table(table_name),
            ),
            "logs" => (
                custom_schema
                    .cloned()
                    .unwrap_or_else(|| Arc::new(OtlpLogsTable::schema())),
                parquet_select_for_table(table_name),
            ),
            "scores" => (
                custom_schema
                    .cloned()
                    .unwrap_or_else(|| Arc::new(ScoreTable::schema())),
                parquet_select_for_table(table_name),
            ),
            name if name == ScoreConfigTable::table_name() => (
                custom_schema
                    .cloned()
                    .unwrap_or_else(|| Arc::new(ScoreConfigTable::schema())),
                parquet_select_for_table(table_name),
            ),
            _ => {
                if let Some(schema) = custom_schema {
                    (Arc::clone(schema), parquet_select_for_table(table_name))
                } else {
                    return Err(anyhow!(
                        "unsupported table for DuckLake ensure: {table_name}"
                    ));
                }
            }
        };

        let batch = RecordBatch::new_empty(arrow_schema.clone());
        let temp_path = Self::write_temp_parquet(table_name, &[batch])?;
        let escaped_path = escape_sql_literal(temp_path.to_string_lossy().as_ref());
        let ddl = crate::sql::writer::create_from_parquet_sql(
            &qualified_table,
            &select_prefix,
            &escaped_path,
        );
        let ddl_res = conn.execute_batch(&ddl);
        let _ = std::fs::remove_file(&temp_path);
        ddl_res
            .map_err(|e| anyhow!("DuckLake table creation failed for {qualified_table}: {e}"))?;

        // Evolve existing tables: ADD any columns present in the Arrow schema
        // (base + product-hot + promotion custom) that the live table lacks.
        let found = crate::storage::schema::describe_table_columns(conn, &qualified_table)?;
        for field in arrow_schema.fields() {
            if !found.contains_key(&field.name().to_ascii_lowercase()) {
                // LIST columns (events) are owned by ensure_trace_fidelity_columns.
                if matches!(field.data_type(), ::arrow::datatypes::DataType::List(_)) {
                    continue;
                }
                let duck_type = Self::arrow_field_to_duck_add_type(field)?;
                let alter_sql = crate::sql::writer::add_column_sql(
                    &qualified_table,
                    &super::util::quote_duckdb_ident(field.name()),
                    duck_type,
                );
                conn.execute_batch(&alter_sql).map_err(|e| {
                    anyhow!(
                        "failed to add column {} to {}: {}",
                        field.name(),
                        qualified_table,
                        e
                    )
                })?;
            }
        }

        if table_name == "traces" {
            ensure_trace_fidelity_columns(conn, &qualified_table)?;
        }
        ensure_hot_map_column_types(conn, &qualified_table, table_name)?;
        if table_name == "traces" {
            ensure_trace_timestamp_precision(conn, &qualified_table)?;
        }
        if table_name == "logs" {
            ensure_log_timestamp_precision(conn, &qualified_table)?;
        }
        // traces / logs / scores — any OTLP layout table (D10).
        if is_otlp_table(table_name) {
            ensure_otlp_table_partition_sort(conn, &qualified_table)?;
        }

        let scope_opt = ducklake_set_option_scope_for_qualified(&qualified_table);
        let opt_size = format!(
            "CALL {}.set_option('target_file_size', '{}', {});",
            scope.attach_alias(),
            size_literal(target_file_size_bytes),
            scope_opt
        );
        let opt_hive = format!(
            "CALL {}.set_option('hive_file_pattern', true, {});",
            scope.attach_alias(),
            scope_opt
        );
        if let Err(err) = conn.execute_batch(&opt_size) {
            warn!(
                "DuckLake target_file_size set_option skipped on ensure: {}",
                err
            );
        }
        if let Err(err) = conn.execute_batch(&opt_hive) {
            warn!(
                "DuckLake hive_file_pattern set_option skipped on ensure: {}",
                err
            );
        }

        Ok(())
    }

    /// Borrow one connection from the bound writer's pool (short map lock; pool slot for SQL).
    pub(super) fn with_attached_conn<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<R> {
        let pool = self.get_or_create_pool(self.physical_scope())?;
        pool.with_conn(f)
    }

    pub(super) async fn ensure_telemetry_table_for(&self, table: &TelemetryTable) -> Result<()> {
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let table_name = match table {
            TelemetryTable::Traces => "traces",
            TelemetryTable::Logs => "logs",
        };
        let scope = self.physical_scope().clone();
        let table_name_owned = table_name.to_string();
        let target_file_size_bytes = self.config.maintenance.target_file_size_bytes;
        tokio::task::spawn_blocking({
            let pool = pool.clone();
            move || {
                pool.with_conn(|conn| {
                    Self::ensure_table_with_conn(
                        conn,
                        &scope,
                        &table_name_owned,
                        None,
                        target_file_size_bytes,
                    )
                })?;
                pool.mark_table_ready(&table_name_owned);
                Ok::<(), anyhow::Error>(())
            }
        })
        .await
        .map_err(|e| anyhow!("telemetry table ensure join failed: {e}"))??;
        Ok(())
    }

    /// Create/evolve the complete shared physical schema before shared workers
    /// install workspace-filtered views. This is intentionally idempotent,
    /// validates legacy schemas in every mode, and does not depend on any
    /// workspace's promotion manifest.
    pub(crate) async fn ensure_shared_schema(&self) -> Result<()> {
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let scope = self.physical_scope().clone();
        let target_file_size_bytes = self.config.maintenance.target_file_size_bytes;
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                for table_name in ["traces", "logs", "scores", "score_configs"] {
                    let qualified_table = ducklake_qualified_table_name(&scope, table_name);
                    match crate::storage::schema::describe_table_columns(conn, &qualified_table) {
                        Ok(columns) if !columns.contains_key("tenant_id") => {
                            return Err(anyhow::anyhow!(
                                "{}: table {table_name} is missing tenant_id; migrate it before shared startup",
                                crate::workspace_scope::SharedScopeError::new(
                                    crate::workspace_scope::SharedScopeErrorCode::SchemaIncompatible,
                                    format!("shared workspace table {table_name} has no ownership column"),
                                )
                            ));
                        }
                        Ok(_) => {}
                        Err(error) if error.to_string().contains("does not exist") => {}
                        Err(error) => return Err(error),
                    }
                    Self::ensure_table_with_conn(
                        conn,
                        &scope,
                        table_name,
                        None,
                        target_file_size_bytes,
                    )?;
                }
                crate::storage::ducklake::validate_shared_workspace_schema(conn, &scope)
            })
        })
        .await
        .map_err(|error| anyhow!("shared schema initialization task failed: {error}"))??;
        Ok(())
    }

    #[cfg(test)]
    pub async fn spans_schema(&self) -> Result<Arc<Schema>> {
        Ok(Arc::new(TraceTable::schema()))
    }

    pub(super) async fn write_record_batches_internal_with_ducklake(
        &self,
        scope: &PhysicalScope,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if record_batches.is_empty() {
            return Ok(());
        }

        let pool = self.get_or_create_pool(scope)?;
        let qualified_table = ducklake_qualified_table_name(scope, table_name);

        // Process-local table readiness gate: cold first touch ensures once and marks ready.
        if !pool.is_table_ready(table_name) {
            let lock = pool.table_lock(table_name);
            let _guard = lock.lock().await;
            if !pool.is_table_ready(table_name) {
                let scope = scope.clone();
                let table_name_owned = table_name.to_string();
                let schema_ref = record_batches[0].schema();
                let target_file_size_bytes = self.config.maintenance.target_file_size_bytes;
                let pool_clone = pool.clone();
                tokio::task::spawn_blocking(move || {
                    pool_clone.with_conn(|conn| {
                        Self::ensure_table_with_conn(
                            conn,
                            &scope,
                            &table_name_owned,
                            Some(&schema_ref),
                            target_file_size_bytes,
                        )
                    })
                })
                .await
                .map_err(|e| anyhow!("table ensure join failed: {e}"))??;
                pool.mark_table_ready(table_name);
            }
        }

        let temp_path = Self::write_temp_parquet(table_name, &record_batches)?;
        let escaped_path = escape_sql_literal(temp_path.to_string_lossy().as_ref());
        let order_clause = self.insert_order_clause(table_name);
        let select_prefix = parquet_select_for_table(table_name);
        let deduplicate_scores =
            table_name == ScoreTable::table_name() || table_name == ScoreConfigTable::table_name();
        let dedupe_id_column: Option<&'static str> = if !deduplicate_scores {
            None
        } else if table_name == ScoreConfigTable::table_name() {
            Some("config_id")
        } else {
            Some("score_id")
        };

        let insert = if let Some(id_column) = dedupe_id_column {
            if self.workspace_scope_mode() == crate::workspace_scope::WorkspaceScopeMode::Shared {
                crate::sql::writer::insert_deduped_parquet_sql_for_workspace(
                    &qualified_table,
                    &select_prefix,
                    &escaped_path,
                    id_column,
                    order_clause,
                )
            } else {
                crate::sql::writer::insert_deduped_parquet_sql(
                    &qualified_table,
                    &select_prefix,
                    &escaped_path,
                    id_column,
                    order_clause,
                )
            }
        } else {
            crate::sql::writer::insert_batch_sql(
                &qualified_table,
                &select_prefix,
                Some(&escaped_path),
                order_clause,
            )
        };

        let write_result = tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                crate::sql::execute_batch_checked(conn, "BEGIN TRANSACTION;")?;
                match crate::sql::execute_batch_checked(conn, &insert) {
                    Ok(()) => {
                        crate::sql::execute_batch_checked(conn, "COMMIT;")?;
                        Ok(())
                    }
                    Err(err) => {
                        let _ = crate::sql::execute_batch_checked(conn, "ROLLBACK;");
                        Err(anyhow!(
                            "DuckLake write failed for {}: {}",
                            qualified_table,
                            err
                        ))
                    }
                }
            })
        })
        .await
        .map_err(|e| anyhow!("DuckLake writer blocking task join failed: {e}"))?;

        let _ = std::fs::remove_file(&temp_path);
        write_result
    }

    pub(super) fn write_temp_parquet(table_name: &str, batches: &[RecordBatch]) -> Result<PathBuf> {
        let base_dir = std::env::temp_dir().join("splake-ducklake");
        std::fs::create_dir_all(&base_dir)?;
        // The staging dir is shared by every engine in the process (and other
        // processes on the host). Nanosecond timestamps alone collided under
        // concurrent writers, truncating a peer's open parquet mid-write and
        // failing its CREATE TABLE with "TProtocolException: Invalid data";
        // PID + monotonic sequence make the name collision-free.
        static TEMP_PARQUET_SEQ: AtomicUsize = AtomicUsize::new(0);
        let seq = TEMP_PARQUET_SEQ.fetch_add(1, Ordering::Relaxed);
        let temp_path = base_dir.join(format!(
            "{}-{}-{}-{}.parquet",
            table_name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            seq
        ));
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = ArrowWriter::try_new(
            file,
            batches[0].schema(),
            Some(WriterProperties::builder().build()),
        )?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(temp_path)
    }

    pub(super) fn open_connection_for(&self, scope: &PhysicalScope) -> Result<Connection> {
        let _ = scope;
        super::attach::DuckLakeSessionFactory::new(&self.config)
            .open(&self.access, super::attach::DuckLakeSessionKind::Writer)
    }

    pub(super) fn attach_ducklake_for(
        &self,
        conn: &Connection,
        scope: &PhysicalScope,
    ) -> Result<()> {
        let _ = scope;
        super::attach::DuckLakeSessionFactory::new(&self.config).attach(conn, &self.access)?;
        Ok(())
    }

    pub(super) fn ensure_schema_for(&self, conn: &Connection, scope: &PhysicalScope) -> Result<()> {
        if scope.is_default_duckdb_namespace() {
            return Ok(());
        }
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{};",
            scope.attach_alias(),
            scope.pg_namespace()
        ))?;
        Ok(())
    }

    pub(super) fn qualified_table_name(&self, table_name: &str) -> String {
        self.qualified_table_name_for(table_name, self.physical_scope())
    }

    pub(super) fn qualified_table_name_for(
        &self,
        table_name: &str,
        scope: &PhysicalScope,
    ) -> String {
        ducklake_qualified_table_name(scope, table_name)
    }

    pub(super) fn insert_order_clause(&self, table_name: &str) -> &'static str {
        insert_order_by(table_name)
    }

    pub(super) fn reset_tables_for_dev(&self, conn: &Connection) -> Result<()> {
        let scope = self.physical_scope();
        for table in [TRACES.name, LOGS.name, SCORES.name, SCORE_CONFIGS.name] {
            let qualified = self.qualified_table_name(table);
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {qualified};"))?;
            if scope.is_default_duckdb_namespace() {
                conn.execute_batch(&format!(
                    "DROP TABLE IF EXISTS {}.{};",
                    scope.attach_alias(),
                    table
                ))?;
            }
        }
        let key = Self::conn_cache_key(scope);
        if let Ok(guard) = self.writer_pools.lock() {
            if let Some(pool) = guard.get(&key) {
                pool.clear_ready();
            }
        }
        info!("DuckLake tables reset because SPLAKE_RESET_DUCKLAKE=1");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DuckLakeConfig};
    use crate::models::Log;
    use crate::storage::schema::{arrow, OtlpLogsTable};
    use std::collections::HashMap;

    #[test]
    fn arrow_field_to_duck_add_type_maps_bags_and_dates() {
        use ::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
        use std::sync::Arc;

        let utf8 = Field::new("user_id", DataType::Utf8, true);
        assert_eq!(
            DuckLakeWriter::arrow_field_to_duck_add_type(&utf8).unwrap(),
            "VARCHAR"
        );
        let date = Field::new("calendar_day", DataType::Date32, false);
        assert_eq!(
            DuckLakeWriter::arrow_field_to_duck_add_type(&date).unwrap(),
            "DATE"
        );
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        );
        let map = Field::new("attributes", DataType::Map(Arc::new(entries), false), true);
        assert_eq!(
            DuckLakeWriter::arrow_field_to_duck_add_type(&map).unwrap(),
            "MAP(VARCHAR, VARCHAR)"
        );
        let list = Field::new(
            "events",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        assert!(DuckLakeWriter::arrow_field_to_duck_add_type(&list)
            .unwrap_err()
            .to_string()
            .contains("LIST"));
        // ensure_table skips LIST fields rather than erroring (fidelity helpers).
        let _ = TimeUnit::Nanosecond;
    }

    #[tokio::test]
    async fn spans_schema_has_no_process_global_promoted_columns() {
        let config = Config::default();
        let binding = WorkspaceBinding::new(
            crate::workspace_scope::DEFAULT_WORKSPACE_ID,
            PhysicalScope::from_ducklake(&DuckLakeConfig {
                metadata_path: config.ducklake.metadata_path.clone(),
                data_path: "/tmp/unused".to_string(),
                catalog_alias: "softprobe".to_string(),
                metadata_schema: "main".to_string(),
                workspace_scope_mode: crate::workspace_scope::WorkspaceScopeMode::Isolated,
                data_inlining_row_limit: None,
                writer_pool_size: 1,
            }),
            crate::workspace_scope::WorkspaceScopeMode::Isolated,
        )
        .expect("binding");
        let access = DuckLakeAccess::Workspace(binding.clone());
        let writer = DuckLakeWriter {
            config: config.clone(),
            binding,
            access,
            writer_pools: Mutex::new(HashMap::new()),
        };

        let schema = writer.spans_schema().await.expect("schema");
        assert!(
            schema.field_with_name("division_name").is_err(),
            "promoted telemetry columns come from runtime-scoped promotion apply, not process config"
        );
    }

    #[test]
    fn migrates_existing_microsecond_log_columns_without_truncating_history() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE upgrade_logs (
                timestamp TIMESTAMPTZ NOT NULL,
                observed_timestamp TIMESTAMPTZ
             );
             INSERT INTO upgrade_logs VALUES
                ('2023-11-14 22:13:20.123456+00', '2023-11-14 22:13:20.654321+00');",
        )
        .unwrap();

        ensure_log_timestamp_precision(&conn, "upgrade_logs").unwrap();

        let observed: (String, i64, String, i64) = conn
            .query_row(
                "SELECT typeof(timestamp), epoch_ns(timestamp),
                        typeof(observed_timestamp), epoch_ns(observed_timestamp)
                 FROM upgrade_logs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            observed,
            (
                "TIMESTAMP_NS".into(),
                1_700_000_000_123_456_000,
                "TIMESTAMP_NS".into(),
                1_700_000_000_654_321_000,
            )
        );
    }

    #[test]
    fn migrates_existing_microsecond_trace_columns_without_losing_epoch_values() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE upgrade_traces (
                timestamp TIMESTAMPTZ NOT NULL,
                end_timestamp TIMESTAMPTZ
             );
             INSERT INTO upgrade_traces VALUES
                ('2023-11-14 22:13:20.123456+00', '2023-11-14 22:13:20.654321+00');",
        )
        .unwrap();

        ensure_trace_timestamp_precision(&conn, "upgrade_traces").unwrap();

        let observed: (String, i64, String, i64) = conn
            .query_row(
                "SELECT typeof(timestamp), epoch_ns(timestamp),
                        typeof(end_timestamp), epoch_ns(end_timestamp)
                 FROM upgrade_traces",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            observed,
            (
                "TIMESTAMP_NS".into(),
                1_700_000_000_123_456_000,
                "TIMESTAMP_NS".into(),
                1_700_000_000_654_321_000,
            )
        );
    }

    #[test]
    fn refuses_unsupported_log_timestamp_schema_before_ddl() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE upgrade_logs_refusal (
                timestamp VARCHAR NOT NULL,
                observed_timestamp TIMESTAMPTZ
             );",
        )
        .unwrap();

        let error = ensure_log_timestamp_precision(&conn, "upgrade_logs_refusal").unwrap_err();
        assert!(error.to_string().contains("cannot safely migrate"));
        let timestamp_type: String = conn
            .query_row(
                "SELECT column_type FROM (DESCRIBE upgrade_logs_refusal)
                 WHERE column_name = 'timestamp'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(timestamp_type, "VARCHAR");
    }

    #[test]
    fn duckdb_read_parquet_preserves_log_nanoseconds() {
        let timestamp_ns = 1_700_000_000_000_000_001;
        let log = Log {
            session_id: None,
            timestamp: chrono::DateTime::from_timestamp_nanos(timestamp_ns),
            observed_timestamp: None,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: "one".into(),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        };
        let batch = arrow::logs_to_record_batch(&[log], &OtlpLogsTable::schema()).unwrap();
        let path = std::env::temp_dir().join(format!(
            "softprobe-log-timestamp-regression-{}.parquet",
            timestamp_ns
        ));
        let file = std::fs::File::create(&path).unwrap();
        let mut parquet = ArrowWriter::try_new(
            file,
            batch.schema(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        parquet.write(&batch).unwrap();
        parquet.close().unwrap();

        let escaped_path = escape_sql_literal(path.to_string_lossy().as_ref());
        let conn = Connection::open_in_memory().unwrap();
        let observed: (String, i64) = conn
            .query_row(
                &format!(
                    "SELECT typeof(timestamp), epoch_ns(timestamp) FROM read_parquet('{escaped_path}')"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(observed, ("TIMESTAMP_NS".into(), timestamp_ns));
    }
}
