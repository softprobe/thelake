use anyhow::{anyhow, Result};
use duckdb::Connection;
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::Config;

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
        if self.cache_dir.is_none() {
            return Ok(());
        }
        wrap_filesystem(conn)
    }
}

/// Wrap S3 and httpfs filesystems with cache_httpfs for persistent on-disk caching.
/// Filesystems are registered lazily by DuckDB when first used, so wrapping may fail
/// until S3/httpfs paths are actually queried. This is non-fatal - caching will work
/// once filesystems are registered through actual usage.
fn wrap_filesystem(conn: &Connection) -> Result<()> {
    // Wrap S3 filesystem (will succeed once S3 is used in a query)
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('s3');", []) {
        let message = err.to_string();
        if message.contains("already wrapped") {
            // Already wrapped is fine (idempotent)
        } else if message.contains("hasn't been registered yet") {
            // S3 filesystem not registered yet - will be registered on first S3 query
            // This is expected and non-fatal. Caching will work once S3 is used.
        } else {
            return Err(anyhow!(
                "Failed to wrap S3 filesystem with cache_httpfs: {}. \
                Unexpected error - ensure DuckDB 1.4.3+ with cache_httpfs support.",
                err
            ));
        }
    }

    // Wrap httpfs filesystem (will succeed once httpfs is used in a query)
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('httpfs');", []) {
        let message = err.to_string();
        if message.contains("already wrapped") {
            // Already wrapped is fine (idempotent)
        } else if message.contains("hasn't been registered yet") {
            // httpfs filesystem not registered yet - will be registered on first HTTP query
            // This is expected and non-fatal. Caching will work once httpfs is used.
        } else {
            return Err(anyhow!(
                "Failed to wrap httpfs filesystem with cache_httpfs: {}. \
                Unexpected error - ensure DuckDB 1.4.3+ with cache_httpfs support.",
                err
            ));
        }
    }

    if std::env::var("PERF_CACHE_PROFILE").ok().as_deref() == Some("1") {
        let first = !CACHE_PROFILE_INIT.swap(true, Ordering::Relaxed);
        if first {
            conn.execute("SET cache_httpfs_profile_type = 'temp';", [])?;
            conn.execute("SELECT cache_httpfs_clear_profile();", [])?;
        }
    }
    Ok(())
}

static CACHE_PROFILE_INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
