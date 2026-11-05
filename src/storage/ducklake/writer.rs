use super::catalog_pool::{CatalogPool, DevReset, WriterPool};
use super::{
    DuckLakeAccess, PhysicalScope, SharedScopeError, SharedScopeErrorCode, WorkspaceBinding,
    WorkspaceScopeMode,
};
use crate::promotion::{BusinessTableManifest, TelemetryColumnsManifest, TelemetryTable};
use crate::sql::schema::{
    base_table_ddl, insert_order_by, is_otlp_table, LOGS, SCORES, SCORE_CONFIGS, TRACES,
};
use crate::storage::schema::attribute_map::parquet_select_for_table;
use crate::storage::schema::otlp_layout::ensure_otlp_table_partition_sort;
use crate::storage::schema::tables::{ScoreConfigTable, ScoreTable};
use ::arrow::datatypes::Schema;
use ::arrow::record_batch::RecordBatch;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tracing::info;

/// Serializes dev resets per physical catalog across pools in one process.
static RESET_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

use super::attach::ducklake_qualified_table_name;
use super::util::escape_sql_literal;

pub(crate) struct DuckLakeWriter {
    /// Single write-path identity token (workspace + physical scope + mode).
    pub(super) binding: WorkspaceBinding,
    /// ATTACH capability built once from [`Self::binding`] at construction.
    pub(super) access: DuckLakeAccess,
    /// Shared catalog sessions; the writer lane serves every workspace.
    pub(super) pool: Arc<CatalogPool>,
}

impl DuckLakeWriter {
    /// Ducklake-internal physical identity (scores/otlp/promotion only).
    pub(super) fn physical_scope(&self) -> &PhysicalScope {
        self.access.physical_scope()
    }

    /// Bound Postgres metadata schema — the only codec engines need upward.
    pub(crate) fn metadata_schema(&self) -> &str {
        self.access.physical_scope().pg_namespace()
    }

    pub(crate) fn workspace_scope_mode(&self) -> WorkspaceScopeMode {
        self.binding.mode
    }

    pub(super) fn shared_workspace_id(&self) -> Result<Option<&str>> {
        if self.workspace_scope_mode() != WorkspaceScopeMode::Shared {
            return Ok(None);
        }
        Ok(Some(self.binding.workspace_id.as_str()))
    }

    pub(super) fn validate_shared_ownership(
        &self,
        record_workspace_id: Option<&str>,
        record_kind: &str,
    ) -> Result<()> {
        let Some(expected) = self.shared_workspace_id()? else {
            return Ok(());
        };
        if record_workspace_id != Some(expected) {
            return Err(anyhow!(
                "shared {record_kind} writes require workspace_id to match the authenticated workspace"
            ));
        }
        Ok(())
    }

    /// Construct a writer permanently bound to one [`WorkspaceBinding`] over a
    /// catalog pool that already attached that binding's physical scope.
    pub(crate) fn new(binding: WorkspaceBinding, pool: Arc<CatalogPool>) -> Result<Self> {
        let access = DuckLakeAccess::Workspace(binding.clone());
        if access.physical_scope() != pool.scope() {
            return Err(anyhow!(
                "workspace {} is bound to a different physical scope than the catalog pool",
                binding.workspace_id
            ));
        }
        info!(
            "DuckLake writer initialized (workspace_id={})",
            binding.workspace_id
        );
        Ok(Self {
            binding,
            access,
            pool,
        })
    }

    /// Wipe the lake tables, recreate traces/logs, and replay the promotion
    /// manifests. Runs once, while the catalog pool is being created.
    pub(super) fn reset_lake(
        writers: &WriterPool,
        scope: &PhysicalScope,
        reset: &DevReset,
    ) -> Result<()> {
        let lock = {
            let mut locks = RESET_LOCKS
                .get_or_init(|| Mutex::new(HashMap::new()))
                .lock()
                .map_err(|_| anyhow!("DuckLake reset lock map poisoned"))?;
            locks
                .entry(scope.writer_pool_key())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _reset_guard = lock
            .lock()
            .map_err(|_| anyhow!("DuckLake reset lock poisoned"))?;
        writers.with_conn(|conn| Self::drop_lake_tables(conn, scope))?;
        writers.clear_ready();
        for table_name in ["traces", "logs"] {
            writers
                .with_conn(|conn| Self::ensure_table_with_conn(conn, scope, table_name, None))?;
            writers.mark_table_ready(table_name);
        }
        Self::reapply_promotions(writers, scope, &reset.telemetry, &reset.business)
    }

    fn reapply_promotions(
        writers: &WriterPool,
        scope: &PhysicalScope,
        telemetry: &[TelemetryColumnsManifest],
        business: &[BusinessTableManifest],
    ) -> Result<()> {
        let prefix = scope.catalog_prefix();
        for manifest in telemetry {
            for ddl in crate::promotion::telemetry_column_add_ddls(&prefix, manifest)
                .map_err(|err| anyhow!("failed to rebuild telemetry promotion: {err}"))?
            {
                writers.with_conn(|conn| {
                    conn.execute_batch(&ddl)
                        .map_err(|err| anyhow!("failed to rebuild telemetry promotion: {err}"))
                })?;
            }
        }
        for manifest in business {
            let ddls = crate::promotion::business_table_create_ddls(&prefix, manifest)
                .map_err(|err| anyhow!("failed to rebuild business promotion: {err}"))?;
            for ddl in ddls {
                writers.with_conn(|conn| {
                    conn.execute_batch(&ddl)
                        .map_err(|err| anyhow!("failed to rebuild business promotion: {err}"))
                })?;
            }
        }
        Ok(())
    }

    /// DuckDB type for promotion columns added from the active manifests.
    ///
    /// MAP bags must not fall through to `VARCHAR`. LIST columns (e.g. events)
    /// are owned by fidelity helpers — refuse here rather than invent a wrong type.
    fn arrow_field_to_duck_add_type(field: &::arrow::datatypes::Field) -> Result<&'static str> {
        use ::arrow::datatypes::DataType;
        match field.data_type() {
            DataType::Utf8 => Ok("VARCHAR"),
            DataType::Boolean => Ok("BOOLEAN"),
            DataType::Int64 => Ok("BIGINT"),
            DataType::Int32 => Ok("INTEGER"),
            DataType::Float64 => Ok("DOUBLE"),
            DataType::Date32 => Ok("DATE"),
            DataType::Timestamp(_, _) => Ok("TIMESTAMP_NS"),
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
    ) -> Result<()> {
        let qualified_table = ducklake_qualified_table_name(scope, table_name);
        let ddl = base_table_ddl(table_name)
            .ok_or_else(|| anyhow!("unsupported table for DuckLake ensure: {table_name}"))?
            .replacen(
                &format!("CREATE TABLE IF NOT EXISTS {table_name}"),
                &format!("CREATE TABLE IF NOT EXISTS {qualified_table}"),
                1,
            );
        conn.execute_batch(&ddl)
            .map_err(|error| anyhow!("DuckLake base DDL failed for {qualified_table}: {error}"))?;

        // IF NOT EXISTS preserves a live table. Verify the live base types so
        // an old physical schema cannot silently survive the clean cutover.
        let found = crate::storage::schema::describe_table_columns(conn, &qualified_table)?;
        let base = crate::storage::schema::tables::base_schema(table_name);
        let base_names = base
            .fields()
            .iter()
            .map(|field| field.name().to_ascii_lowercase())
            .collect::<HashSet<_>>();
        for field in base.fields() {
            let actual = found
                .get(&field.name().to_ascii_lowercase())
                .ok_or_else(|| {
                    anyhow!(
                        "base column {} is missing from {qualified_table}; clean cutover is required",
                        field.name()
                    )
                })?;
            let expected = Self::arrow_field_to_duck_add_type(field)?;
            if normalize_duck_type(actual) != normalize_duck_type(expected) {
                return Err(anyhow!(
                    "base column {} on {qualified_table} has type {actual}, expected {expected}; clean cutover is required",
                    field.name()
                ));
            }
        }

        // Only active promotion manifests may extend the SQL-owned base.
        if let Some(schema) = custom_schema {
            let promoted_names = schema
                .fields()
                .iter()
                .map(|field| field.name().to_ascii_lowercase())
                .filter(|name| !base_names.contains(name))
                .collect::<HashSet<_>>();
            for name in found.keys() {
                if !base_names.contains(name) && !promoted_names.contains(name) {
                    return Err(anyhow!(
                        "runtime column {name} on {qualified_table} is absent from the active promotion schema"
                    ));
                }
            }
            for field in schema.fields() {
                if let Some(actual) = found.get(&field.name().to_ascii_lowercase()) {
                    if !base_names.contains(&field.name().to_ascii_lowercase()) {
                        let expected = Self::arrow_field_to_duck_add_type(field)?;
                        if normalize_duck_type(actual) != normalize_duck_type(expected) {
                            return Err(anyhow!(
                                "promoted column {} on {qualified_table} has type {actual}, expected {expected}",
                                field.name()
                            ));
                        }
                    }
                    continue;
                }
                if base_names.contains(&field.name().to_ascii_lowercase()) {
                    return Err(anyhow!(
                        "base column {} is missing from {qualified_table}; clean cutover is required",
                        field.name()
                    ));
                }
                // Custom schemas arrive only from active promotion manifests.
                let duck_type = Self::arrow_field_to_duck_add_type(field)?;
                conn.execute_batch(&crate::sql::writer::add_column_sql(
                    &qualified_table,
                    &super::util::quote_duckdb_ident(field.name()),
                    duck_type,
                ))
                .map_err(|error| {
                    anyhow!("failed to apply promoted column {}: {error}", field.name())
                })?;
            }
        }

        if is_otlp_table(table_name) {
            ensure_otlp_table_partition_sort(conn, &qualified_table)?;
            super::layout::apply_otlp_layout_profile(conn, scope, table_name)?;
        }
        Ok(())
    }

    /// Borrow one writer-lane session of the shared catalog pool.
    pub(super) fn with_attached_conn<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<R> {
        self.pool.with_writer(f)
    }

    pub(super) async fn ensure_telemetry_table_for(&self, table: &TelemetryTable) -> Result<()> {
        let table_name = match table {
            TelemetryTable::Traces => "traces",
            TelemetryTable::Logs => "logs",
        };
        let scope = self.physical_scope().clone();
        let table_name_owned = table_name.to_string();
        tokio::task::spawn_blocking({
            let pool = Arc::clone(&self.pool);
            move || {
                pool.with_writer(|conn| {
                    Self::ensure_table_with_conn(conn, &scope, &table_name_owned, None)
                })?;
                pool.writers().mark_table_ready(&table_name_owned);
                Ok::<(), anyhow::Error>(())
            }
        })
        .await
        .map_err(|e| anyhow!("telemetry table ensure join failed: {e}"))??;
        Ok(())
    }

    /// Create the complete shared physical schema before query slots install
    /// workspace-filtered views. Base columns come from SQL DDL; active
    /// manifests add only their declared promotion columns.
    pub(super) fn ensure_shared_schema(writers: &WriterPool, scope: &PhysicalScope) -> Result<()> {
        writers.with_conn(|conn| {
            for table_name in ["traces", "logs", "scores", "score_configs"] {
                let qualified_table = ducklake_qualified_table_name(scope, table_name);
                match crate::storage::schema::describe_table_columns(conn, &qualified_table) {
                    Ok(columns) if !columns.contains_key("workspace_id") => {
                        return Err(anyhow::anyhow!(
                            "{}: table {table_name} is missing workspace_id; recreate it during clean cutover",
                            SharedScopeError::new(
                                SharedScopeErrorCode::SchemaIncompatible,
                                format!("shared workspace table {table_name} has no ownership column"),
                            )
                        ));
                    }
                    Ok(_) => {}
                    Err(error) if error.to_string().contains("does not exist") => {}
                    Err(error) => return Err(error),
                }
                Self::ensure_table_with_conn(conn, scope, table_name, None)?;
            }
            crate::storage::ducklake::validate_shared_workspace_schema(conn, scope)
        })
    }

    pub(super) async fn write_record_batches_internal_with_ducklake(
        &self,
        scope: &PhysicalScope,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
        dedupe_timestamp_window: Option<crate::sql::QueryWindow>,
    ) -> Result<()> {
        if record_batches.is_empty() {
            return Ok(());
        }

        let pool = Arc::clone(&self.pool);
        let qualified_table = ducklake_qualified_table_name(scope, table_name);

        // Process-local table readiness gate: cold first touch ensures once and marks ready.
        if !pool.writers().is_table_ready(table_name) {
            let lock = pool.writers().table_lock(table_name);
            let _guard = lock.lock().await;
            if !pool.writers().is_table_ready(table_name) {
                let scope = scope.clone();
                let table_name_owned = table_name.to_string();
                let schema_ref = record_batches[0].schema();
                let pool_clone = Arc::clone(&pool);
                tokio::task::spawn_blocking(move || {
                    pool_clone.with_writer(|conn| {
                        Self::ensure_table_with_conn(
                            conn,
                            &scope,
                            &table_name_owned,
                            Some(&schema_ref),
                        )
                    })
                })
                .await
                .map_err(|e| anyhow!("table ensure join failed: {e}"))??;
                pool.writers().mark_table_ready(table_name);
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

        let shared_scope = self.workspace_scope_mode() == WorkspaceScopeMode::Shared;
        let table_name_owned = table_name.to_string();
        let qualified_table_owned = qualified_table.clone();
        let select_prefix_owned = select_prefix.clone();
        let order_clause_owned = order_clause.to_string();

        let write_result = tokio::task::spawn_blocking(move || {
            pool.with_writer(|conn| {
                super::layout::apply_otlp_writer_session(conn, &table_name_owned)?;
                crate::sql::execute_batch_checked(conn, "BEGIN TRANSACTION;")?;
                let insert_result = (|| {
                    let insert = if let Some(id_column) = dedupe_id_column {
                        if shared_scope {
                            crate::sql::writer::insert_deduped_parquet_sql_for_workspace(
                                &qualified_table_owned,
                                &select_prefix_owned,
                                &escaped_path,
                                id_column,
                                &order_clause_owned,
                                dedupe_timestamp_window.as_ref(),
                            )
                        } else {
                            crate::sql::writer::insert_deduped_parquet_sql(
                                &qualified_table_owned,
                                &select_prefix_owned,
                                &escaped_path,
                                id_column,
                                &order_clause_owned,
                                dedupe_timestamp_window.as_ref(),
                            )
                        }
                    } else {
                        crate::sql::writer::insert_batch_sql(
                            &qualified_table_owned,
                            &select_prefix_owned,
                            Some(&escaped_path),
                            &order_clause_owned,
                        )
                    };
                    crate::sql::execute_batch_for_parquet_ingest(conn, &insert)?;
                    Ok::<(), anyhow::Error>(())
                })();
                match insert_result {
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

    pub(super) fn ensure_schema_for(conn: &Connection, scope: &PhysicalScope) -> Result<()> {
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{};",
            scope.attach_alias(),
            scope.pg_namespace()
        ))?;
        Ok(())
    }

    pub(super) fn insert_order_clause(&self, table_name: &str) -> &'static str {
        insert_order_by(table_name)
    }

    fn drop_lake_tables(conn: &Connection, scope: &PhysicalScope) -> Result<()> {
        for table in [TRACES.name, LOGS.name, SCORES.name, SCORE_CONFIGS.name] {
            let qualified = ducklake_qualified_table_name(scope, table);
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {qualified};"))?;
        }
        info!("DuckLake tables reset because SPLAKE_RESET_DUCKLAKE=1");
        Ok(())
    }
}

fn normalize_duck_type(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .flat_map(char::to_uppercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Log;
    use crate::storage::schema::arrow;
    use crate::storage::schema::OtlpLogsTable;
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

    #[test]
    fn base_trace_schema_has_no_process_global_promoted_columns() {
        let schema = crate::storage::schema::tables::TraceTable::schema();
        assert!(
            schema.field_with_name("division_name").is_err(),
            "promoted telemetry columns come from runtime-scoped promotion apply, not process config"
        );
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
            workspace_id: None,
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
