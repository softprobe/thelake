//! Shared DuckLake partition/sort readiness probe (metrics + OTLP tables).

use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

static DESCRIBE_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);
static PARTITION_SORT_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn describe_probe_count() -> usize {
    DESCRIBE_PROBE_COUNT.load(Ordering::Relaxed)
}

pub fn partition_sort_probe_count() -> usize {
    PARTITION_SORT_PROBE_COUNT.load(Ordering::Relaxed)
}

pub fn total_schema_probe_count() -> usize {
    describe_probe_count() + partition_sort_probe_count()
}

/// Consolidated DESCRIBE table helper tracking probes. Returns lowercase column names.
pub fn describe_table_columns(
    conn: &Connection,
    qualified_table: &str,
) -> Result<HashMap<String, String>> {
    DESCRIBE_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
    let sql = format!("DESCRIBE {qualified_table};");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} failed: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let dtype: String = row.get(1)?;
            Ok((name, dtype))
        })
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} query failed: {e}"))?;

    let mut found = HashMap::new();
    for row in rows {
        let (name, dtype) = row.map_err(|e| anyhow!("DESCRIBE row failed: {e}"))?;
        found.insert(name.to_ascii_lowercase(), dtype);
    }
    Ok(found)
}

/// True when a live DuckLake table already has partition + sort metadata.
pub fn table_partition_sort_ready(
    conn: &Connection,
    catalog_or_qualified: &str,
    table_name: &str,
) -> Result<bool> {
    PARTITION_SORT_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
    // catalog may be `softprobe`, `softprobe.tenant_schema`, or `softprobe.traces` —
    // metadata is always `__ducklake_metadata_<attach_alias>` (first path segment).
    let attach = catalog_or_qualified
        .split('.')
        .next()
        .unwrap_or(catalog_or_qualified);
    let meta = format!("__ducklake_metadata_{attach}");
    let sql = format!(
        "SELECT \
            (SELECT count(*) FROM {meta}.ducklake_partition_info info \
             JOIN {meta}.ducklake_table t ON info.table_id = t.table_id \
             WHERE t.table_name = ? AND t.end_snapshot IS NULL) AS parts, \
            (SELECT count(*) FROM {meta}.ducklake_sort_info info \
             JOIN {meta}.ducklake_table t ON info.table_id = t.table_id \
             WHERE t.table_name = ? AND t.end_snapshot IS NULL) AS sorts"
    );
    let (parts, sorts): (i64, i64) = conn
        .query_row(&sql, [table_name, table_name], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap_or((0, 0));
    Ok(parts > 0 && sorts > 0)
}
