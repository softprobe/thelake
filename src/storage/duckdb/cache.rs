use anyhow::{anyhow, Result};
use duckdb::Connection;
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::Config;
pub(crate) use crate::storage::ducklake::cache_httpfs_disabled_by_env;

#[derive(Clone)]
pub(crate) struct CacheSettings {
    pub(crate) cache_dir: Option<PathBuf>,
}

impl CacheSettings {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            cache_dir: config.query.cache_dir.as_ref().map(PathBuf::from),
        }
    }

    /// Wrap S3/httpfs with cache_httpfs after the extension was loaded by duckdb_init.sql.
    pub(crate) fn wrap_filesystems(&self, conn: &Connection) -> Result<()> {
        if self.cache_dir.is_none() || cache_httpfs_disabled_by_env() {
            return Ok(());
        }
        for name in ["s3", "httpfs"] {
            match wrap_one(conn, name)? {
                WrapAttempt::Wrapped | WrapAttempt::NotReady => {}
                WrapAttempt::Unsupported => {
                    return Err(anyhow!(
                        "cache_httpfs wrap function not available in this DuckDB build"
                    ));
                }
            }
        }
        enable_cache_profile_if_requested(conn)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WrapAttempt {
    Wrapped,
    NotReady,
    Unsupported,
}

/// One wrap attempt for `s3` or `httpfs`. Callers decide hard-fail vs soft-disable.
pub(crate) fn wrap_one(conn: &Connection, filesystem: &str) -> Result<WrapAttempt> {
    match conn.execute(
        &format!("SELECT cache_httpfs_wrap_cache_filesystem('{filesystem}');"),
        [],
    ) {
        Ok(_) => Ok(WrapAttempt::Wrapped),
        Err(err) => {
            let message = err.to_string();
            if message.contains("already wrapped") {
                Ok(WrapAttempt::Wrapped)
            } else if message.contains("hasn't been registered yet") {
                Ok(WrapAttempt::NotReady)
            } else if message.contains("does not exist")
                || (message.contains("Catalog Error")
                    && message.contains("cache_httpfs_wrap_cache_filesystem"))
            {
                Ok(WrapAttempt::Unsupported)
            } else {
                Err(anyhow!(
                    "Failed to wrap {filesystem} filesystem with cache_httpfs: {err}. \
                     Unexpected error - ensure DuckDB 1.4.3+ with cache_httpfs support."
                ))
            }
        }
    }
}

fn enable_cache_profile_if_requested(conn: &Connection) -> Result<()> {
    if std::env::var("PERF_CACHE_PROFILE").ok().as_deref() != Some("1") {
        return Ok(());
    }
    let first = !CACHE_PROFILE_INIT.swap(true, Ordering::Relaxed);
    if first {
        conn.execute("SET cache_httpfs_profile_type = 'temp';", [])?;
        conn.execute("SELECT cache_httpfs_clear_profile();", [])?;
    }
    Ok(())
}

static CACHE_PROFILE_INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
