//! Loki log scan SQL.

const SCAN_SQL: &str = include_str!("logs/scan.sql");

pub fn scan_sql(window: &str, promoted: &str, cap: usize) -> String {
    SCAN_SQL
        .replace("{{promoted}}", promoted)
        .replace("{{timestamp_filter}}", window)
        .replace("{{limit}}", &cap.saturating_add(1).to_string())
}

#[cfg(test)]
mod tests {
    use super::scan_sql;

    #[test]
    fn scan_sql_uses_template_and_preserves_required_window() {
        let sql = scan_sql(
            " AND timestamp >= TIMESTAMP_NS '2026-09-10' AND timestamp < TIMESTAMP_NS '2026-09-11'",
            "service_name, user_id",
            99,
        );
        assert!(sql.contains("FROM logs"));
        assert!(sql.contains("timestamp >= TIMESTAMP_NS '2026-09-10'"));
        assert!(sql.contains("timestamp < TIMESTAMP_NS '2026-09-11'"));
        assert!(sql.contains("service_name, user_id"));
        assert!(sql.contains("LIMIT 100"));
        assert!(!sql.contains("{{"));
    }
}
