use crate::config::{Config, DuckLakeConfig};
use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::warn;

use super::object_store::configure_object_store;
use super::util::escape_sql_literal;

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

/// Open in-memory DuckDB with httpfs + object-store credentials + DuckLake
/// catalog extensions loaded — ready for `ATTACH`.
///
/// **Required** for any path that may read Parquet under `gs://` / `s3://`
/// (compaction, session_summary reduce/rebuild). Query workers configure the
/// same credentials separately on their long-lived pool. A connection that only
/// ATTACHes can still scan catalog-inlined rows, which is why local-disk
/// session_summary tests historically passed without this step.
pub(crate) fn open_object_store_ducklake_connection(
    config: &Config,
    ducklake: &DuckLakeConfig,
    threads: i64,
    memory_limit: &str,
) -> Result<Connection> {
    let conn = open_in_memory_capped(threads, memory_limit).context("DuckDB open failed")?;
    conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
        .context("INSTALL/LOAD httpfs")?;
    configure_object_store(&conn, config, &ducklake.data_path).context("configure object store")?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .context("INSTALL/LOAD ducklake")?;
    match ducklake.catalog_type.as_str() {
        "postgres" => conn
            .execute_batch("INSTALL postgres; LOAD postgres;")
            .context("INSTALL/LOAD postgres")?,
        "sqlite" => conn
            .execute_batch("INSTALL sqlite; LOAD sqlite;")
            .context("INSTALL/LOAD sqlite")?,
        _ => {}
    }
    if let Err(err) = configure_duckdb_resources(&conn, threads, memory_limit) {
        warn!("Failed to cap DuckDB threads/memory: {err}");
    }
    Ok(conn)
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

pub(crate) fn ducklake_attach_target(dk: &DuckLakeConfig) -> String {
    match dk.catalog_type.as_str() {
        "postgres" => {
            if dk.metadata_path.starts_with("postgres:") {
                dk.metadata_path.clone()
            } else {
                format!("postgres:{}", dk.metadata_path)
            }
        }
        "sqlite" => {
            if dk.metadata_path.starts_with("sqlite:") {
                dk.metadata_path.clone()
            } else {
                format!("sqlite:{}", dk.metadata_path)
            }
        }
        _ => dk.metadata_path.clone(),
    }
}

/// ATTACH options shared by writer / query / compaction.
pub(crate) fn ducklake_attach_options(dk: &DuckLakeConfig) -> Vec<String> {
    let mut options = vec![format!("DATA_PATH '{}'", escape_sql_literal(&dk.data_path))];
    if dk.catalog_type == "postgres" && dk.metadata_schema != "main" {
        let schema = escape_sql_literal(&dk.metadata_schema);
        options.push(format!("METADATA_SCHEMA '{}'", schema));
        options.push(format!("META_SCHEMA '{}'", schema));
    }
    // Official SQLite multi-client guidance: WAL + busy timeout (DuckLake / sqlite extension).
    // 5s absorbs concurrent query-worker ATTACH / snapshot races better than 500ms.
    if dk.catalog_type == "sqlite" {
        options.push("META_JOURNAL_MODE 'WAL'".to_string());
        options.push("META_BUSY_TIMEOUT 5000".to_string());
    }
    if let Some(limit) = dk.data_inlining_row_limit {
        options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
    }
    options
}

/// Ensure local filesystem paths exist before ATTACH.
///
/// SQLite catalogs need the metadata DB parent directory. Local (non-URI)
/// `DATA_PATH` must exist for both sqlite and postgres catalogs so DuckLake can
/// create files under it.
pub(crate) fn prepare_local_ducklake_paths(dk: &DuckLakeConfig, attach_target: &str) -> Result<()> {
    if dk.catalog_type == "sqlite" {
        let raw = attach_target
            .strip_prefix("sqlite:")
            .unwrap_or(attach_target);
        let metadata_path = std::path::PathBuf::from(raw);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
    }
    if !dk.data_path.contains("://") {
        std::fs::create_dir_all(&dk.data_path)?;
    }
    Ok(())
}

/// Fully qualified DuckLake table name used for CREATE / INSERT (`catalog.table` when
/// `metadata_schema` is `main`, else `catalog.metadata_schema.table`).

/// Catalog prefix for qualified DuckLake tables (`alias` or `alias.schema`).
pub(crate) fn catalog_prefix(catalog_alias: &str, metadata_schema: &str) -> String {
    if metadata_schema == "main" {
        catalog_alias.to_string()
    } else {
        format!("{catalog_alias}.{metadata_schema}")
    }
}

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

/// Open an in-memory DuckDB connection and attach DuckLake driven entirely by [`DuckLakeConfig`].
/// Reuses production attach logic across SQLite and PostgreSQL (DRY first).
pub fn open_and_attach_ducklake(dk: &DuckLakeConfig) -> anyhow::Result<(Connection, String)> {
    let conn = open_in_memory_capped(QUERY_DUCKDB_THREADS, QUERY_DUCKDB_MEMORY)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    if dk.catalog_type == "postgres" {
        conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
    }
    if dk.catalog_type == "sqlite" {
        conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
    }
    apply_ducklake_retry_settings(&conn)?;

    let attach_target = ducklake_attach_target(dk);
    prepare_local_ducklake_paths(dk, &attach_target)?;

    let options = ducklake_attach_options(dk);
    let sql = format!(
        "ATTACH 'ducklake:{target}' AS {alias} ({opts});",
        target = escape_sql_literal(&attach_target),
        alias = dk.catalog_alias,
        opts = options.join(", ")
    );
    conn.execute_batch(&sql)
        .map_err(|e| anyhow::anyhow!("DuckLake attach failed: {e}"))?;

    let catalog = catalog_prefix(&dk.catalog_alias, &dk.metadata_schema);
    Ok((conn, catalog))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let conn =
            open_in_memory_capped(QUERY_DUCKDB_THREADS, QUERY_DUCKDB_MEMORY).expect("duckdb");
        let threads: i64 = conn
            .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
            .expect("threads setting");
        assert_eq!(threads, QUERY_DUCKDB_THREADS);
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
        let result = open_object_store_ducklake_connection(
            &config,
            &config.ducklake,
            QUERY_DUCKDB_THREADS,
            QUERY_DUCKDB_MEMORY,
        );

        match prev_id {
            Some(v) => std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", v),
            None => std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(v) => std::env::set_var("GCS_HMAC_SECRET", v),
            None => std::env::remove_var("GCS_HMAC_SECRET"),
        }

        let conn = result.expect("open_object_store_ducklake_connection");
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
        let result = open_object_store_ducklake_connection(
            &config,
            &config.ducklake,
            QUERY_DUCKDB_THREADS,
            QUERY_DUCKDB_MEMORY,
        );

        match prev_id {
            Some(v) => std::env::set_var("AWS_ACCESS_KEY_ID", v),
            None => std::env::remove_var("AWS_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(v) => std::env::set_var("AWS_SECRET_ACCESS_KEY", v),
            None => std::env::remove_var("AWS_SECRET_ACCESS_KEY"),
        }

        let conn = result.expect("open_object_store_ducklake_connection");
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
}
