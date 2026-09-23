//! Workspace-to-DuckLake scope contracts.
//!
//! This module defines the value types used to describe a workspace's logical
//! binding to a physical DuckLake scope. Runtime access policy is implemented
//! by the ingest, query, and maintenance engines in later layers.

use crate::config::DuckLakeConfig;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Sentinel workspace id for the process-default / unauthenticated lake.
///
/// Empty tenant or workspace ids resolve to this identity for engines, leases,
/// maintenance scope lists, and readiness probes.
pub const DEFAULT_WORKSPACE_ID: &str = "_default";

/// Map empty (or whitespace-only) workspace ids to [`DEFAULT_WORKSPACE_ID`].
pub fn effective_workspace_id(workspace_id: &str) -> &str {
    if workspace_id.trim().is_empty() {
        DEFAULT_WORKSPACE_ID
    } else {
        workspace_id
    }
}

/// Selects whether workspaces receive isolated or shared physical storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceScopeMode {
    /// Preserve the current one-workspace-per-scope behavior.
    #[default]
    Isolated,
    /// Allow multiple workspaces to use one physical DuckLake scope.
    Shared,
}

impl fmt::Display for WorkspaceScopeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Isolated => "isolated",
            Self::Shared => "shared",
        })
    }
}

/// The physical DuckLake inputs that identify one storage scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalScope {
    pub metadata_path: String,
    pub data_path: String,
    pub catalog_alias: String,
    pub metadata_schema: String,
}

impl PhysicalScope {
    /// Capture the physical scope selected by the runtime configuration.
    pub fn from_ducklake(config: &DuckLakeConfig) -> Self {
        Self {
            metadata_path: config.metadata_path.clone(),
            data_path: config.data_path.clone(),
            catalog_alias: config.catalog_alias.clone(),
            metadata_schema: config.metadata_schema.clone(),
        }
    }

    /// Return a deterministic, unambiguous key for scope-local registries.
    pub fn key(&self) -> String {
        let mut key = String::from("ducklake:");
        for part in [
            &self.metadata_path,
            &self.data_path,
            &self.catalog_alias,
            &self.metadata_schema,
        ] {
            key.push_str(&part.len().to_string());
            key.push(':');
            key.push_str(part);
        }
        key
    }
}

/// The logical workspace-to-physical-scope binding used by engine contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub workspace_id: String,
    pub physical_scope: PhysicalScope,
    pub mode: WorkspaceScopeMode,
}

impl WorkspaceBinding {
    pub fn new(
        workspace_id: impl Into<String>,
        physical_scope: PhysicalScope,
        mode: WorkspaceScopeMode,
    ) -> Result<Self, WorkspaceBindingError> {
        let workspace_id = workspace_id.into();
        if workspace_id.trim().is_empty() {
            return Err(WorkspaceBindingError::EmptyWorkspaceId);
        }
        Ok(Self {
            workspace_id,
            physical_scope,
            mode,
        })
    }
}

/// Classified capability used to construct a DuckLake session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuckLakeAccess {
    /// Authenticated workspace access for ingest or query operations.
    Workspace(WorkspaceBinding),
    /// Complete physical-scope access for maintenance and catalog operations.
    Physical(PhysicalScope),
}

impl DuckLakeAccess {
    pub fn physical_scope(&self) -> &PhysicalScope {
        match self {
            Self::Workspace(binding) => &binding.physical_scope,
            Self::Physical(scope) => scope,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceBindingError {
    EmptyWorkspaceId,
}

impl fmt::Display for WorkspaceBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyWorkspaceId => f.write_str("workspace_id must not be empty"),
        }
    }
}

impl std::error::Error for WorkspaceBindingError {}

/// Stable machine-readable error identifiers for shared-scope failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedScopeErrorCode {
    UnsupportedBackend,
    NotEnabled,
    SchemaIncompatible,
    RawSqlForbidden,
    ConnectionUnavailable,
    PromotionUnsupported,
}

impl SharedScopeErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedBackend => "shared_scope_unsupported_backend",
            Self::NotEnabled => "shared_scope_not_enabled",
            Self::SchemaIncompatible => "shared_scope_schema_incompatible",
            Self::RawSqlForbidden => "shared_scope_raw_sql_forbidden",
            Self::ConnectionUnavailable => "shared_scope_connection_unavailable",
            Self::PromotionUnsupported => "shared_scope_promotion_unsupported",
        }
    }
}

impl fmt::Display for SharedScopeErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Shared-mode failure with a stable machine-readable code and human detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedScopeError {
    code: SharedScopeErrorCode,
    detail: String,
}

impl SharedScopeError {
    pub fn new(code: SharedScopeErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub const fn code(&self) -> SharedScopeErrorCode {
        self.code
    }
}

impl fmt::Display for SharedScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.detail.is_empty() {
            f.write_str(self.code.as_str())
        } else {
            write!(f, "{}: {}", self.code, self.detail)
        }
    }
}

impl std::error::Error for SharedScopeError {}

#[cfg(test)]
mod tests {
    use super::{
        effective_workspace_id, PhysicalScope, SharedScopeError, SharedScopeErrorCode,
        WorkspaceBinding, WorkspaceScopeMode, DEFAULT_WORKSPACE_ID,
    };
    use crate::config::DuckLakeConfig;

    #[test]
    fn effective_workspace_id_maps_empty_to_default() {
        assert_eq!(effective_workspace_id(""), DEFAULT_WORKSPACE_ID);
        assert_eq!(effective_workspace_id("   "), DEFAULT_WORKSPACE_ID);
        assert_eq!(effective_workspace_id("ws-1"), "ws-1");
        assert_eq!(DEFAULT_WORKSPACE_ID, "_default");
    }

    #[test]
    fn scope_mode_defaults_to_isolated_and_round_trips() {
        assert_eq!(WorkspaceScopeMode::default(), WorkspaceScopeMode::Isolated);
        let encoded = serde_yaml::to_string(&WorkspaceScopeMode::Shared).expect("serialize");
        assert_eq!(encoded.trim(), "shared");
        let decoded: WorkspaceScopeMode = serde_yaml::from_str("shared").expect("deserialize");
        assert_eq!(decoded, WorkspaceScopeMode::Shared);
    }

    #[test]
    fn physical_scope_is_derived_from_ducklake_configuration() {
        let config = DuckLakeConfig::default();
        let scope = PhysicalScope::from_ducklake(&config);
        assert_eq!(scope.metadata_path, config.metadata_path);
        assert_eq!(scope.data_path, config.data_path);
        assert_eq!(scope.catalog_alias, config.catalog_alias);
        assert_eq!(scope.metadata_schema, config.metadata_schema);
        assert!(!scope.key().is_empty());
    }

    #[test]
    fn physical_scope_key_changes_when_a_scope_input_changes() {
        let config = DuckLakeConfig::default();
        let first = PhysicalScope::from_ducklake(&config);
        let mut changed_config = config;
        changed_config.metadata_path.push_str("-other");
        let second = PhysicalScope::from_ducklake(&changed_config);
        assert_ne!(first.key(), second.key());
    }

    #[test]
    fn workspace_binding_requires_a_workspace_id() {
        let scope = PhysicalScope::from_ducklake(&DuckLakeConfig::default());
        let error = WorkspaceBinding::new(" ", scope, WorkspaceScopeMode::Isolated)
            .expect_err("blank workspace id must be rejected");
        assert_eq!(error.to_string(), "workspace_id must not be empty");
    }

    #[test]
    fn shared_scope_error_codes_are_stable() {
        assert_eq!(
            SharedScopeErrorCode::UnsupportedBackend.as_str(),
            "shared_scope_unsupported_backend"
        );
        assert_eq!(
            SharedScopeErrorCode::NotEnabled.as_str(),
            "shared_scope_not_enabled"
        );
        assert_eq!(
            SharedScopeErrorCode::SchemaIncompatible.as_str(),
            "shared_scope_schema_incompatible"
        );
        assert_eq!(
            SharedScopeErrorCode::RawSqlForbidden.as_str(),
            "shared_scope_raw_sql_forbidden"
        );
        assert_eq!(
            SharedScopeErrorCode::ConnectionUnavailable.as_str(),
            "shared_scope_connection_unavailable"
        );
        assert_eq!(
            SharedScopeErrorCode::PromotionUnsupported.as_str(),
            "shared_scope_promotion_unsupported"
        );
        let error = SharedScopeError::new(
            SharedScopeErrorCode::RawSqlForbidden,
            "ordinary workspace SQL is not trusted",
        );
        assert_eq!(error.code(), SharedScopeErrorCode::RawSqlForbidden);
        assert_eq!(
            error.to_string(),
            "shared_scope_raw_sql_forbidden: ordinary workspace SQL is not trusted"
        );
    }

    #[test]
    fn production_access_inventory_covers_connection_sites() {
        let inventory = include_str!("../docs/ducklake-access-inventory.md");
        for path in [
            "src/storage/ducklake/attach.rs",
            "src/storage/ducklake/object_store.rs",
            "src/storage/ducklake/writer.rs",
            "src/storage/schema/otlp_layout.rs",
            "src/storage/schema/ducklake_partition.rs",
            "src/storage/ducklake/util.rs",
            "src/query/duckdb.rs",
            "src/query/cache.rs",
            "src/compaction/engine.rs",
            "src/compaction/merge.rs",
            "src/compaction/session_summary_access.rs",
            "src/sql/maintenance/mod.rs",
            "src/session_summary/reduce.rs",
            "src/storage/ducklake/promotion.rs",
            "src/sql/bounds/execute_gate.rs",
        ] {
            assert!(
                inventory.contains(path),
                "missing inventory entry for {path}"
            );
        }
    }
}
