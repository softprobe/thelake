//! Render and apply the single DuckDB init SQL script.
//!
//! Template syntax (intentionally tiny — no external crate):
//! - `{{name}}` → string substitution (SQL-safe values supplied by caller)
//! - `{{#name}}...{{/name}}` → include block when section is enabled
//!
//! When optional sections are requested they are part of the same rendered
//! batch and fail hard with the rest of init.

use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;

const DUCKDB_INIT_SQL: &str = include_str!("sql/duckdb_init.sql");

/// Parameters for [`apply_duckdb_init`].
#[derive(Debug, Clone)]
pub struct DuckDbInitParams<'a> {
    pub threads: i64,
    pub memory_limit: &'a str,
    /// Query-worker native cache SETs (`enable_object_cache`, …).
    pub enable_query_tuning: bool,
    /// When set, INSTALL/LOAD `cache_httpfs` and point it at this directory.
    pub cache_directory: Option<&'a Path>,
}

impl<'a> DuckDbInitParams<'a> {
    pub fn session(threads: i64, memory_limit: &'a str) -> Self {
        Self {
            threads,
            memory_limit,
            enable_query_tuning: false,
            cache_directory: None,
        }
    }

    pub fn query(threads: i64, memory_limit: &'a str, cache_directory: Option<&'a Path>) -> Self {
        Self {
            threads,
            memory_limit,
            enable_query_tuning: true,
            cache_directory,
        }
    }
}

/// Render [`DUCKDB_INIT_SQL`] with `params` (no DuckDB I/O).
pub fn render_duckdb_init(params: &DuckDbInitParams<'_>) -> String {
    let cache_dir_sql = params.cache_directory.map(escape_sql_path);
    let mut sql = DUCKDB_INIT_SQL.to_string();
    sql = render_section(&sql, "enable_query_tuning", params.enable_query_tuning);
    sql = render_section(&sql, "cache_directory", cache_dir_sql.is_some());
    sql = sql.replace("{{threads}}", &params.threads.to_string());
    sql = sql.replace("{{memory_limit}}", params.memory_limit);
    if let Some(dir) = &cache_dir_sql {
        sql = sql.replace("{{cache_directory}}", dir);
    }
    sql
}

/// Execute the rendered init script on `conn` (single batch, fail-hard).
pub fn apply_duckdb_init(conn: &Connection, params: &DuckDbInitParams<'_>) -> Result<()> {
    let sql = render_duckdb_init(params);
    conn.execute_batch(&sql)
        .with_context(|| format!("DuckDB init SQL failed (threads={})", params.threads))?;
    Ok(())
}

fn escape_sql_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\'', "''")
        .replace('\\', "\\\\")
}

fn render_section(template: &str, name: &str, enabled: bool) -> String {
    let start_tag = format!("{{{{#{name}}}}}");
    let end_tag = format!("{{{{/{name}}}}}");
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find(&start_tag) {
        out.push_str(&rest[..start]);
        let after_start = &rest[start + start_tag.len()..];
        let Some(end) = after_start.find(&end_tag) else {
            out.push_str(&rest[start..]);
            return out;
        };
        let body = &after_start[..end];
        if enabled {
            out.push_str(body);
        }
        rest = &after_start[end + end_tag.len()..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn render_includes_extensions_and_resources() {
        let sql = render_duckdb_init(&DuckDbInitParams::session(1, "512MB"));
        for ext in ["httpfs", "ducklake", "postgres"] {
            let install = sql.find(&format!("INSTALL {ext}")).expect("INSTALL");
            let load = sql.find(&format!("LOAD {ext}")).expect("LOAD");
            assert!(install < load, "{ext}");
        }
        assert!(sql.contains("SET threads = 1;"));
        assert!(sql.contains("SET memory_limit = '512MB';"));
        assert!(sql.contains("SET unsafe_enable_version_guessing = false;"));
        assert!(!sql.contains("INSTALL cache_httpfs"));
        assert!(!sql.contains("SET enable_object_cache"));
        assert!(!sql.contains("{{threads}}"));
        assert!(!sql.contains("{{#"));
        assert!(!sql.contains("{{/"));
        assert!(!sql.contains("{{cache_directory}}"));
    }

    #[test]
    fn render_query_tuning_and_cache_directory() {
        let dir = PathBuf::from("/tmp/thelake_cache");
        let sql = render_duckdb_init(&DuckDbInitParams::query(1, "512MB", Some(dir.as_path())));
        assert!(sql.contains("SET enable_object_cache = true;"));
        assert!(sql.contains("INSTALL cache_httpfs FROM community;"));
        assert!(sql.contains("SET cache_httpfs_cache_directory = '/tmp/thelake_cache';"));
        let quoted = PathBuf::from("/tmp/o'brian");
        let sql = render_duckdb_init(&DuckDbInitParams::query(1, "512MB", Some(quoted.as_path())));
        assert!(sql.contains("SET cache_httpfs_cache_directory = '/tmp/o''brian';"));
    }

    #[test]
    fn cache_httpfs_init_sql_installs_before_loads() {
        let dir = PathBuf::from("/tmp/thelake_cache");
        let sql = render_duckdb_init(&DuckDbInitParams::query(1, "512MB", Some(dir.as_path())));
        let install = sql
            .find("INSTALL cache_httpfs")
            .expect("expected INSTALL cache_httpfs in duckdb_init.sql");
        let load = sql
            .find("LOAD cache_httpfs")
            .expect("expected LOAD cache_httpfs in duckdb_init.sql");
        assert!(
            install < load,
            "expected INSTALL cache_httpfs to appear before LOAD cache_httpfs"
        );
        assert!(
            sql.contains("SET cache_httpfs_type = 'on_disk';"),
            "cache_httpfs on-disk settings must ship in the single init script"
        );
        assert!(sql.contains("SET cache_httpfs_enable_glob_cache = true;"));
    }

    #[test]
    fn render_section_helper_ignores_disabled_blocks() {
        let t = "A{{#x}}B{{/x}}C";
        assert_eq!(render_section(t, "x", false), "AC");
        assert_eq!(render_section(t, "x", true), "ABC");
    }

    #[test]
    fn apply_session_init_on_live_connection() {
        let conn = Connection::open_in_memory().expect("duckdb");
        apply_duckdb_init(&conn, &DuckDbInitParams::session(1, "512MB")).expect("init");
        let threads: i64 = conn
            .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
            .expect("threads");
        assert_eq!(threads, 1);
        let guessing: bool = conn
            .query_row(
                "SELECT current_setting('unsafe_enable_version_guessing')",
                [],
                |row| row.get(0),
            )
            .expect("version guessing");
        assert!(!guessing);
    }

    #[test]
    fn apply_query_tuning_on_live_connection() {
        let conn = Connection::open_in_memory().expect("duckdb");
        apply_duckdb_init(&conn, &DuckDbInitParams::query(1, "512MB", None)).expect("init");
        let object_cache: bool = conn
            .query_row("SELECT current_setting('enable_object_cache')", [], |row| {
                row.get(0)
            })
            .expect("object cache");
        assert!(object_cache);
    }
}
