use crate::config::{Config, DuckLakeConfig};
use crate::workspace_scope::DuckLakeAccess;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;

use super::object_store::configure_object_store;
use super::util::escape_sql_literal;

static ATTACH_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn attach_lock(ducklake: &DuckLakeConfig) -> Arc<Mutex<()>> {
    let key = format!(
        "{}|{}|{}|{}",
        ducklake.metadata_path,
        ducklake.metadata_schema,
        ducklake.data_path,
        ducklake.catalog_alias
    );
    let locks = ATTACH_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

const POSTGRES_ATTACH_LOCK_ALIAS: &str = "__thelake_attach_lock";

struct PostgresAttachLock<'a> {
    conn: &'a Connection,
}

impl<'a> PostgresAttachLock<'a> {
    fn acquire(conn: &'a Connection, ducklake: &DuckLakeConfig) -> Result<Self> {
        let target = ducklake.metadata_path.as_str();
        let scope_key = format!(
            "{}|{}|{}|{}",
            target, ducklake.metadata_schema, ducklake.data_path, ducklake.catalog_alias
        );
        let remote_lock = format!(
            "SELECT pg_advisory_lock(hashtextextended('{}', 0));",
            escape_sql_literal(&scope_key)
        );
        conn.execute_batch(&format!(
            "ATTACH '{}' AS {POSTGRES_ATTACH_LOCK_ALIAS} (TYPE postgres);\nSELECT * FROM postgres_execute('{POSTGRES_ATTACH_LOCK_ALIAS}', '{}');",
            escape_sql_literal(target),
            escape_sql_literal(&remote_lock),
        ))?;
        Ok(Self { conn })
    }
}

impl Drop for PostgresAttachLock<'_> {
    fn drop(&mut self) {
        let remote_unlock = "SELECT pg_advisory_unlock_all();";
        let _ = self.conn.execute_batch(&format!(
            "SELECT * FROM postgres_execute('{POSTGRES_ATTACH_LOCK_ALIAS}', '{}'); DETACH {POSTGRES_ATTACH_LOCK_ALIAS};",
            escape_sql_literal(remote_unlock),
        ));
    }
}

pub(super) fn catalog_is_attached(conn: &Connection, alias: &str) -> bool {
    let sql = format!(
        "SELECT 1 FROM duckdb_databases() WHERE database_name = '{}' LIMIT 1;",
        escape_sql_literal(alias)
    );
    conn.query_row(&sql, [], |_| Ok(())).is_ok()
}

/// Query workers: one DuckDB thread each. Default `threads = nproc` on every
/// connection made Grafana refresh occupy hundreds of OS threads and 15s timeouts.
pub(crate) const QUERY_DUCKDB_THREADS: i64 = 1;
pub(crate) const QUERY_DUCKDB_MEMORY: &str = "512MB";
/// Writers / TWCS: classic Prom dual-write + live OTEL need more than 512MB.
pub(crate) const WRITER_DUCKDB_THREADS: i64 = 1;
pub(crate) const WRITER_DUCKDB_MEMORY: &str = "1GB";
/// Compaction merges hundreds of VARIANT/postings files; 512MB OOMs (TWCS skip
/// → Grafana scans 200–500 Parquet files per PromQL). One compact connection.
pub(crate) const COMPACTION_DUCKDB_THREADS: i64 = 2;
pub(crate) const COMPACTION_DUCKDB_MEMORY: &str = "2GB";

/// Connection profile for the three production DuckDB access modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DuckLakeSessionKind {
    Query,
    Writer,
    Maintenance,
}

impl DuckLakeSessionKind {
    fn resources(self) -> (i64, &'static str) {
        match self {
            Self::Query => (QUERY_DUCKDB_THREADS, QUERY_DUCKDB_MEMORY),
            Self::Writer => (WRITER_DUCKDB_THREADS, WRITER_DUCKDB_MEMORY),
            Self::Maintenance => (COMPACTION_DUCKDB_THREADS, COMPACTION_DUCKDB_MEMORY),
        }
    }
}

/// Internal factory for every production DuckDB session.
///
/// It owns the common ordering of open, extension loading, object-store
/// configuration, retry settings, resource caps, and catalog attach. The
/// caller still owns the operation-specific connection lifetime and SQL.
pub(crate) struct DuckLakeSessionFactory<'a> {
    config: &'a Config,
}

impl<'a> DuckLakeSessionFactory<'a> {
    pub(crate) fn new(config: &'a Config) -> Self {
        Self { config }
    }

    pub(crate) fn open(
        &self,
        access: &DuckLakeAccess,
        kind: DuckLakeSessionKind,
    ) -> Result<Connection> {
        let ducklake = self.ducklake_config(access);
        let (threads, memory_limit) = kind.resources();
        self.open_with_resources(&ducklake, threads, memory_limit)
    }

    fn open_with_resources(
        &self,
        ducklake: &DuckLakeConfig,
        threads: i64,
        memory_limit: &str,
    ) -> Result<Connection> {
        let conn = open_in_memory_capped(threads, memory_limit).context("DuckDB open failed")?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
            .context("INSTALL/LOAD httpfs")?;
        configure_object_store(&conn, self.config, &ducklake.data_path)
            .context("configure object store")?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("INSTALL/LOAD ducklake")?;
        // DuckLake uses the Postgres catalog on every runtime path.
        conn.execute_batch("INSTALL postgres; LOAD postgres;")
            .context("INSTALL/LOAD postgres")?;
        apply_ducklake_retry_settings(&conn).context("configure DuckLake retry settings")?;
        if let Err(err) = configure_duckdb_resources(&conn, threads, memory_limit) {
            warn!("Failed to cap DuckDB threads/memory: {err}");
        }
        Ok(conn)
    }

    pub(crate) fn attach(&self, conn: &Connection, access: &DuckLakeAccess) -> Result<String> {
        let ducklake = self.ducklake_config(access);
        self.attach_ducklake(conn, &ducklake)
    }

    fn attach_ducklake(&self, conn: &Connection, ducklake: &DuckLakeConfig) -> Result<String> {
        // DuckLake catalog creation is a cross-connection operation. Serialize
        // first attach attempts within this process so two engines cannot both
        // enter the extension's non-idempotent CREATE TABLE path. The
        // CREATE_IF_NOT_EXISTS=false-first flow below still makes later
        // connections attach read/write catalogs without reinitializing them.
        let lock = attach_lock(ducklake);
        let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Normal runtime path: always the Postgres DuckLake catalog.
        let _postgres_lock = PostgresAttachLock::acquire(conn, ducklake)?;
        // DuckLake's default ATTACH mode initializes a catalog when it is
        // missing. That initialization is not idempotent across independent
        // DuckDB connections: a second connection can race or re-enter the
        // CREATE TABLE path and report `ducklake_metadata already exists`.
        // Connect to an existing catalog first; only a genuinely missing
        // catalog is allowed to take the creation path.
        match self.attach_ducklake_once(conn, ducklake, false) {
            Ok(prefix) => Ok(prefix),
            Err(existing_error) if ducklake_catalog_is_missing(&existing_error.to_string()) => {
                self.attach_ducklake_once(conn, ducklake, true)
            }
            Err(error) => Err(error),
        }
    }

    fn attach_ducklake_once(
        &self,
        conn: &Connection,
        ducklake: &DuckLakeConfig,
        create_if_not_exists: bool,
    ) -> Result<String> {
        prepare_local_ducklake_paths(ducklake)?;
        let options = ducklake_attach_options(ducklake, create_if_not_exists);
        let sql = format!(
            "ATTACH 'ducklake:postgres:{target}' AS {alias} ({opts});",
            target = escape_sql_literal(&ducklake.metadata_path),
            alias = ducklake.catalog_alias,
            opts = options.join(", ")
        );
        match conn.execute_batch(&sql) {
            Ok(()) => {}
            Err(err) => {
                let message = err.to_string();
                if catalog_is_attached(conn, &ducklake.catalog_alias) {
                    return Ok(catalog_prefix(
                        &ducklake.catalog_alias,
                        &ducklake.metadata_schema,
                    ));
                }
                if message.to_lowercase().contains("already exists")
                    || (message.contains("ducklake_metadata")
                        && !message.contains("does not exist"))
                {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    match conn.execute_batch(&sql) {
                        Ok(()) => {}
                        Err(_retry_err) if catalog_is_attached(conn, &ducklake.catalog_alias) => {
                            return Ok(catalog_prefix(
                                &ducklake.catalog_alias,
                                &ducklake.metadata_schema,
                            ));
                        }
                        Err(retry_err) => {
                            return Err(anyhow::anyhow!(
                                "DuckLake attach failed after retry: {retry_err} (first: {message})"
                            ));
                        }
                    }
                } else if message.contains("__ducklake_metadata_")
                    && message.contains("does not exist")
                {
                    let mut fallback_options = vec![format!(
                        "DATA_PATH '{}'",
                        escape_sql_literal(&ducklake.data_path)
                    )];
                    if let Some(limit) = ducklake.data_inlining_row_limit {
                        fallback_options.push(format!("DATA_INLINING_ROW_LIMIT {limit}"));
                    }
                    let fallback_sql = format!(
                        "ATTACH 'ducklake:postgres:{target}' AS {alias} ({opts});",
                        target = escape_sql_literal(&ducklake.metadata_path),
                        alias = ducklake.catalog_alias,
                        opts = fallback_options.join(", ")
                    );
                    conn.execute_batch(&fallback_sql).map_err(|fallback_err| {
                        anyhow::anyhow!("DuckDB ATTACH failed: {fallback_err}")
                    })?;
                } else {
                    return Err(anyhow::anyhow!("DuckDB ATTACH failed: {err}"));
                }
            }
        }

        Ok(catalog_prefix(
            &ducklake.catalog_alias,
            &ducklake.metadata_schema,
        ))
    }

    fn ducklake_config(&self, access: &DuckLakeAccess) -> DuckLakeConfig {
        let scope = access.physical_scope();
        let mut ducklake = self.config.ducklake.clone();
        ducklake.metadata_path = scope.metadata_path.clone();
        ducklake.data_path = scope.data_path.clone();
        ducklake.catalog_alias = scope.catalog_alias.clone();
        ducklake.metadata_schema = scope.metadata_schema.clone();
        ducklake
    }
}

fn ducklake_catalog_is_missing(message: &str) -> bool {
    let message = message.to_lowercase();
    message.contains("existing ducklake") && message.contains("does not exist")
}

/// Open in-memory DuckDB with thread/memory caps applied at database create
/// time. `SET threads` after INSTALL/LOAD does not fully shrink an nproc-wide
/// TaskScheduler (self-mon inventory spiked to ~40 Running threads / ~5 cores).
pub(crate) fn open_in_memory_capped(threads: i64, memory_limit: &str) -> Result<Connection> {
    let config = duckdb::Config::default()
        .threads(threads)?
        .max_memory(memory_limit)?;
    Connection::open_in_memory_with_flags(config)
        .map_err(|e| anyhow::anyhow!("DuckDB open failed: {e}"))
}

/// Cap CPU and RAM for an already-open DuckDB connection (best-effort follow-up).
pub(crate) fn configure_duckdb_resources(
    conn: &Connection,
    threads: i64,
    memory_limit: &str,
) -> Result<()> {
    conn.execute(&format!("SET threads = {threads}"), [])?;
    conn.execute(&format!("SET memory_limit = '{memory_limit}'"), [])?;
    Ok(())
}

/// Pin DuckLake extension conflict-retry defaults (official concurrent-write mechanism).
pub(super) fn apply_ducklake_retry_settings(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "SET ducklake_max_retry_count = 10;\n\
         SET ducklake_retry_backoff = 1.5;\n\
         SET ducklake_retry_wait_ms = 100;",
    )?;
    Ok(())
}

/// ATTACH options shared by writer / query / compaction.
///
/// The normal runtime path is always the Postgres DuckLake catalog, so
/// `METADATA_SCHEMA`/`META_SCHEMA` are the only backend-specific options built
/// here.
pub(crate) fn ducklake_attach_options(
    dk: &DuckLakeConfig,
    create_if_not_exists: bool,
) -> Vec<String> {
    let mut options = vec![format!("DATA_PATH '{}'", escape_sql_literal(&dk.data_path))];
    options.push(format!(
        "CREATE_IF_NOT_EXISTS {}",
        if create_if_not_exists {
            "true"
        } else {
            "false"
        }
    ));
    if dk.metadata_schema != "main" {
        let schema = escape_sql_literal(&dk.metadata_schema);
        options.push(format!("METADATA_SCHEMA '{}'", schema));
        options.push(format!("META_SCHEMA '{}'", schema));
    }
    if let Some(limit) = dk.data_inlining_row_limit {
        options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
    }
    options
}

/// Ensure local filesystem paths exist before ATTACH.
///
/// Local (non-URI) `DATA_PATH` must exist so DuckLake can create files under it.
pub(crate) fn prepare_local_ducklake_paths(dk: &DuckLakeConfig) -> Result<()> {
    if !dk.data_path.contains("://") {
        std::fs::create_dir_all(&dk.data_path)?;
    }
    Ok(())
}

/// Catalog prefix for qualified DuckLake tables (`alias` or `alias.schema`).
pub(crate) fn catalog_prefix(catalog_alias: &str, metadata_schema: &str) -> String {
    if metadata_schema == "main" {
        catalog_alias.to_string()
    } else {
        format!("{catalog_alias}.{metadata_schema}")
    }
}

/// Fully qualified DuckLake table name used for CREATE / INSERT (`catalog.table` when
/// `metadata_schema` is `main`, else `catalog.metadata_schema.table`).
pub(crate) fn ducklake_qualified_table_name(cfg: &DuckLakeConfig, bare_table: &str) -> String {
    format!(
        "{}.{}",
        catalog_prefix(&cfg.catalog_alias, &cfg.metadata_schema),
        bare_table
    )
}

/// Scoping clause for `CALL <catalog>.set_option(...)` matching a qualified table name.
/// Two-part `catalog.table` → `table_name` only; three-part → `schema` + `table_name`.
pub(crate) fn ducklake_set_option_scope_for_qualified(qualified_table: &str) -> String {
    let parts: Vec<&str> = qualified_table.split('.').collect();
    match parts.len() {
        3 => {
            let s = escape_sql_literal(parts[1]);
            let t = escape_sql_literal(parts[2]);
            format!("schema => '{s}', table_name => '{t}'")
        }
        2 => {
            let t = escape_sql_literal(parts[1]);
            format!("table_name => '{t}'")
        }
        _ => {
            let t = escape_sql_literal(parts.last().copied().unwrap_or(""));
            format!("table_name => '{t}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_scope::{
        DuckLakeAccess, PhysicalScope, WorkspaceBinding, WorkspaceScopeMode,
    };

    #[test]
    fn set_option_scope_matches_qualified_table_shape() {
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.traces"),
            "table_name => 'traces'"
        );
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.tenant_a.traces"),
            "schema => 'tenant_a', table_name => 'traces'"
        );
    }

    #[test]
    fn query_resource_caps_pin_single_thread() {
        let config = Config::default();
        let scope = PhysicalScope::from_ducklake(&config.ducklake);
        let access = DuckLakeAccess::Workspace(
            WorkspaceBinding::new("query-test", scope, WorkspaceScopeMode::Isolated)
                .expect("binding"),
        );
        let conn = DuckLakeSessionFactory::new(&config)
            .open(&access, DuckLakeSessionKind::Query)
            .expect("duckdb");
        let threads: i64 = conn
            .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
            .expect("threads setting");
        assert_eq!(threads, QUERY_DUCKDB_THREADS);
    }

    #[test]
    fn session_factory_opens_all_resource_profiles() {
        let config = Config::default();
        let access = DuckLakeAccess::Physical(PhysicalScope::from_ducklake(&config.ducklake));
        for (kind, expected_threads, expected_memory) in [
            (
                DuckLakeSessionKind::Query,
                QUERY_DUCKDB_THREADS,
                "488.2 MiB",
            ),
            (
                DuckLakeSessionKind::Writer,
                WRITER_DUCKDB_THREADS,
                "953.6 MiB",
            ),
            (
                DuckLakeSessionKind::Maintenance,
                COMPACTION_DUCKDB_THREADS,
                "1.8 GiB",
            ),
        ] {
            let conn = DuckLakeSessionFactory::new(&config)
                .open(&access, kind)
                .expect("session profile");
            let threads: i64 = conn
                .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
                .expect("threads setting");
            assert_eq!(threads, expected_threads);
            let memory: String = conn
                .query_row("SELECT current_setting('memory_limit')", [], |row| {
                    row.get(0)
                })
                .expect("memory setting");
            assert_eq!(memory, expected_memory, "memory cap for {kind:?}");
        }
    }

    #[test]
    fn compaction_memory_cap_exceeds_writer_so_twcs_can_merge() {
        const {
            assert!(
                COMPACTION_DUCKDB_THREADS >= WRITER_DUCKDB_THREADS,
                "compaction must not be thinner than writers"
            );
        }
        assert_ne!(COMPACTION_DUCKDB_MEMORY, WRITER_DUCKDB_MEMORY);
        assert!(
            COMPACTION_DUCKDB_MEMORY.ends_with("GB"),
            "TWCS merge of closed-day metric_series OOM'd at writer 512MB"
        );
    }

    #[test]
    fn object_store_ducklake_connection_installs_gcs_secret() {
        let prev_id = std::env::var("GCS_HMAC_ACCESS_KEY_ID").ok();
        let prev_secret = std::env::var("GCS_HMAC_SECRET").ok();
        std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", "attach-test-key");
        std::env::set_var("GCS_HMAC_SECRET", "attach-test-secret");

        let mut config = Config::default();
        config.ducklake.data_path = "gs://softprobe-test/ducklake/".to_string();
        let access = DuckLakeAccess::Physical(PhysicalScope::from_ducklake(&config.ducklake));
        let result = DuckLakeSessionFactory::new(&config).open(&access, DuckLakeSessionKind::Query);

        match prev_id {
            Some(v) => std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", v),
            None => std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(v) => std::env::set_var("GCS_HMAC_SECRET", v),
            None => std::env::remove_var("GCS_HMAC_SECRET"),
        }

        let conn = result.expect("open query session");
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_secrets() WHERE name = 'gcs_hmac'",
                [],
                |row| row.get(0),
            )
            .expect("duckdb_secrets");
        assert_eq!(n, 1, "gs:// paths require a GCS secret before Parquet I/O");
    }

    #[test]
    fn object_store_ducklake_connection_sets_s3_endpoint() {
        let prev_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let prev_secret = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");

        let mut config = Config::default();
        config.object_store.endpoint = Some("http://localhost:9000".to_string());
        config.object_store.region = "us-east-1".to_string();
        config.ducklake.data_path = "s3://warehouse/ducklake/".to_string();
        let access = DuckLakeAccess::Physical(PhysicalScope::from_ducklake(&config.ducklake));
        let result = DuckLakeSessionFactory::new(&config).open(&access, DuckLakeSessionKind::Query);

        match prev_id {
            Some(v) => std::env::set_var("AWS_ACCESS_KEY_ID", v),
            None => std::env::remove_var("AWS_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(v) => std::env::set_var("AWS_SECRET_ACCESS_KEY", v),
            None => std::env::remove_var("AWS_SECRET_ACCESS_KEY"),
        }

        let conn = result.expect("open query session");
        let endpoint: String = conn
            .query_row("SELECT current_setting('s3_endpoint')", [], |row| {
                row.get(0)
            })
            .expect("s3_endpoint");
        assert!(
            endpoint.contains("localhost:9000"),
            "expected minio endpoint, got {endpoint}"
        );
    }

    #[test]
    fn session_factory_opens_and_attaches_catalog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let suffix = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let mut config = Config::default();
        config.ducklake.metadata_schema = format!("thelake_attach_{suffix}");
        config.ducklake.data_path = dir.path().join("data").to_string_lossy().into_owned();

        let access = DuckLakeAccess::Physical(PhysicalScope::from_ducklake(&config.ducklake));
        let factory = DuckLakeSessionFactory::new(&config);
        let conn = factory
            .open(&access, DuckLakeSessionKind::Query)
            .expect("open session");
        let catalog = factory.attach(&conn, &access).expect("attach catalog");
        assert_eq!(catalog, format!("softprobe.thelake_attach_{suffix}"));
        assert!(catalog_is_attached(&conn, "softprobe"));
        let retry_count: i64 = conn
            .query_row(
                "SELECT current_setting('ducklake_max_retry_count')",
                [],
                |row| row.get(0),
            )
            .expect("retry setting");
        assert_eq!(retry_count, 10);
        let retry_backoff: f64 = conn
            .query_row(
                "SELECT current_setting('ducklake_retry_backoff')",
                [],
                |row| row.get(0),
            )
            .expect("retry backoff setting");
        assert_eq!(retry_backoff, 1.5);
        let retry_wait_ms: i64 = conn
            .query_row(
                "SELECT current_setting('ducklake_retry_wait_ms')",
                [],
                |row| row.get(0),
            )
            .expect("retry wait setting");
        assert_eq!(retry_wait_ms, 100);
    }

    #[test]
    fn session_factory_reports_attach_failure() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let suffix = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let mut config = Config::default();
        config.ducklake.metadata_schema = format!("thelake_attach_fail_{suffix}");
        config.ducklake.data_path = dir.path().join("data").to_string_lossy().into_owned();
        config.ducklake.catalog_alias = "bad-alias".to_string();

        let access = DuckLakeAccess::Physical(PhysicalScope::from_ducklake(&config.ducklake));
        let factory = DuckLakeSessionFactory::new(&config);
        let conn = factory
            .open(&access, DuckLakeSessionKind::Query)
            .expect("open session");
        let error = factory
            .attach(&conn, &access)
            .expect_err("invalid catalog alias must fail attach");
        assert!(error.to_string().contains("ATTACH"));
    }
}
