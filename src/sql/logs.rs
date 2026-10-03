//! Loki log scan SQL.

const SCAN_SQL: &str = include_str!("logs/scan.sql");

pub fn scan_sql(window: &str, promoted: &str, cap: usize) -> String {
    SCAN_SQL
        .replace("{{promoted}}", promoted)
        .replace("{{timestamp_filter}}", window)
        .replace("{{limit}}", &cap.saturating_add(1).to_string())
}
