//! Health / readiness lake probes.

use crate::sql::trusted::{approved_query, TrustedSql};

/// Static readiness probe (`SELECT 1`).
pub(crate) fn readiness_sql() -> TrustedSql {
    approved_query("SELECT 1").expect("static readiness query")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_sql_is_select_one_trusted() {
        assert_eq!(readiness_sql().as_str().trim(), "SELECT 1");
    }
}
