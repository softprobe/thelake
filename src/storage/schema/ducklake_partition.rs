//! Shared DuckLake partition/sort readiness probe (metrics + OTLP tables).

use anyhow::Result;
use duckdb::Connection;

/// True when a live DuckLake table already has partition + sort metadata.
pub fn table_partition_sort_ready(
    conn: &Connection,
    catalog_or_qualified: &str,
    table_name: &str,
) -> Result<bool> {
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
