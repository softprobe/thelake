//! Opaque SQL produced by an approved query builder.
//!
//! `TrustedSql` is intentionally crate-private. User-provided SQL must remain
//! on the ordinary compatibility path until the query engine applies its
//! shared-scope policy. The constructor is kept behind this module so future
//! builders can become the only producers of trusted statements.

use crate::workspace_scope::{PhysicalScope, SharedScopeErrorCode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrustedSql(String);

pub(crate) fn approved_query(sql: impl Into<String>) -> Result<TrustedSql, TrustedSqlError> {
    TrustedSql::from_approved_builder(sql)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustedSqlError {
    Empty,
    Forbidden(&'static str),
}

impl TrustedSql {
    /// Construct the opaque result of an approved internal query builder.
    pub(crate) fn from_approved_builder(sql: impl Into<String>) -> Result<Self, TrustedSqlError> {
        let sql = sql.into();
        if sql.trim().is_empty() {
            return Err(TrustedSqlError::Empty);
        }
        let upper = sql.to_ascii_uppercase();
        if upper.matches(';').count() > 1
            || (upper.contains(';') && !upper.trim_end().ends_with(';'))
        {
            return Err(TrustedSqlError::Forbidden("multiple SQL statements"));
        }
        for token in [
            "ATTACH", "DETACH", "CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "COPY",
            "CALL", "MERGE", "TRUNCATE", "VACUUM", "PRAGMA", "SET", "LOAD", "INSTALL", "EXPORT",
            "IMPORT",
        ] {
            if contains_sql_token(&upper, token) {
                return Err(TrustedSqlError::Forbidden(token));
            }
        }
        if upper.contains("__DUCKLAKE_METADATA_") || upper.contains("DUCKLAKE:") {
            return Err(TrustedSqlError::Forbidden("physical DuckLake reference"));
        }
        Ok(Self(sql))
    }

    pub(crate) fn validate_for_scope(&self, scope: &PhysicalScope) -> Result<(), TrustedSqlError> {
        let upper = self.0.to_ascii_uppercase();
        for identifier in physical_identifiers(scope) {
            if !identifier.trim().is_empty() && upper.contains(&identifier.to_ascii_uppercase()) {
                return Err(TrustedSqlError::Forbidden("physical identifier"));
            }
        }
        if contains_qualified_table_reference(&upper) {
            return Err(TrustedSqlError::Forbidden("qualified physical table"));
        }
        Ok(())
    }

    #[cfg(test)]
    fn from_builder(
        sql: impl Into<String>,
        scope: &PhysicalScope,
    ) -> Result<Self, TrustedSqlError> {
        let trusted = Self::from_approved_builder(sql)?;
        trusted.validate_for_scope(scope)?;
        Ok(trusted)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

fn physical_identifiers(scope: &PhysicalScope) -> Vec<String> {
    [
        scope.catalog_alias.clone(),
        scope.metadata_schema.clone(),
        format!("__ducklake_metadata_{}", scope.catalog_alias),
        scope.metadata_path.clone(),
        scope.data_path.clone(),
    ]
    .into_iter()
    .filter(|value| !value.trim().is_empty())
    .collect()
}

fn contains_sql_token(sql: &str, token: &str) -> bool {
    sql.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .any(|part| part == token)
}

fn contains_qualified_table_reference(sql: &str) -> bool {
    for keyword in ["FROM", "JOIN"] {
        let mut remainder = sql;
        while let Some(index) = remainder.find(keyword) {
            let before = &remainder[..index];
            let after = &remainder[index + keyword.len()..];
            let boundary_before = before
                .chars()
                .next_back()
                .map(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
                .unwrap_or(true);
            let boundary_after = after
                .chars()
                .next()
                .map(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
                .unwrap_or(true);
            if boundary_before && boundary_after {
                let table = after
                    .trim_start()
                    .split(|ch: char| ch.is_ascii_whitespace() || matches!(ch, ',' | ';' | ')'))
                    .next()
                    .unwrap_or_default();
                if table.contains('.') {
                    return true;
                }
            }
            remainder = &remainder[index + keyword.len()..];
        }
    }
    false
}

impl std::fmt::Display for TrustedSqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "{}: empty query", SharedScopeErrorCode::RawSqlForbidden),
            Self::Forbidden(reason) => {
                write!(f, "{}: {reason}", SharedScopeErrorCode::RawSqlForbidden)
            }
        }
    }
}

impl std::error::Error for TrustedSqlError {}

#[cfg(test)]
mod tests {
    use super::{TrustedSql, TrustedSqlError};
    use crate::{config::DuckLakeConfig, workspace_scope::PhysicalScope};

    #[test]
    fn trusted_sql_is_opaque_and_non_empty() {
        let scope = scope();
        let sql = TrustedSql::from_builder("SELECT 1", &scope).expect("trusted query");
        assert_eq!(sql.as_str(), "SELECT 1");
        assert_eq!(
            TrustedSql::from_builder("  ", &scope),
            Err(TrustedSqlError::Empty)
        );
    }

    #[test]
    fn trusted_sql_rejects_unsafe_statement_classes() {
        for sql in [
            "ATTACH 'ducklake:x' AS softprobe",
            "CREATE TABLE traces (id INTEGER)",
            "INSERT INTO traces VALUES (1)",
            "CALL ducklake_merge()",
            "MERGE INTO traces USING logs ON true WHEN MATCHED THEN UPDATE SET id = 1",
            "TRUNCATE traces",
            "LOAD httpfs",
            "INSTALL ducklake",
            "SET enable_object_cache = true",
            "VACUUM traces",
            "SELECT * FROM softprobe.traces",
            "SELECT * FROM __ducklake_metadata_softprobe.ducklake_table",
        ] {
            let result = TrustedSql::from_builder(sql, &scope());
            assert!(result.is_err(), "unsafe SQL was accepted: {sql}");
            assert!(result
                .expect_err("unsafe SQL")
                .to_string()
                .starts_with("shared_scope_raw_sql_forbidden:"));
        }
    }

    #[test]
    fn trusted_sql_rejects_multiple_statements() {
        assert!(TrustedSql::from_builder("SELECT 1; SELECT 2", &scope()).is_err());
    }

    #[test]
    fn trusted_sql_rejects_qualified_tables_without_caller_supplied_identifiers() {
        let scope = scope();
        for sql in [
            "SELECT * FROM other.traces",
            "SELECT * FROM other_schema.other.traces",
            "SELECT * FROM traces JOIN other.logs ON true",
            "SELECT * FROM \"SOFTPROBE\".shared_scope.traces",
            "SELECT * FROM SOFTPROBE.shared_scope.traces",
        ] {
            assert!(
                TrustedSql::from_builder(sql, &scope).is_err(),
                "qualified table was accepted: {sql}"
            );
        }
    }

    fn scope() -> PhysicalScope {
        PhysicalScope::from_ducklake(&DuckLakeConfig::default())
    }
}
