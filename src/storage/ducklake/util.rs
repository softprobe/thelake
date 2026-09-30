use tracing::warn;

pub(crate) fn escape_sql_literal(input: &str) -> String {
    // Escape only (no quotes) — callers historically wrap the result.
    // Prefer `crate::sql::sql_string_literal` for new quoted literals.
    input.replace('\'', "''")
}

/// Process-wide kill switch for query-path cache_httpfs (SessionFactory + workers).
pub(crate) fn cache_httpfs_disabled_by_env() -> bool {
    std::env::var("PERF_DISABLE_CACHE_HTTPFS").ok().as_deref() == Some("1")
}

pub(super) fn quote_duckdb_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

pub(crate) fn size_literal(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{}GiB", bytes / GB)
    } else if bytes >= MB && bytes.is_multiple_of(MB) {
        format!("{}MiB", bytes / MB)
    } else if bytes >= KB && bytes.is_multiple_of(KB) {
        format!("{}KiB", bytes / KB)
    } else {
        warn!(
            "target_file_size_bytes={} is not power-of-1024 aligned; using byte literal",
            bytes
        );
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::size_literal;

    #[test]
    fn size_literals_preserve_binary_byte_targets() {
        assert_eq!(size_literal(8 * 1024 * 1024), "8MiB");
        assert_eq!(size_literal(128 * 1024 * 1024), "128MiB");
        assert_eq!(size_literal(1536), "1536B");
    }
}
