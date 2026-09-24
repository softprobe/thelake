//! Opaque DuckLake catalog identity and workspace↔physical binding.
//!
//! [`PhysicalScope`] lives in storage: ATTACH, qualification, and registry packing
//! stay next to the DuckLake session stack. Handlers never import this module;
//! public product API is re-exported from [`crate::workspace_scope`].

use crate::config::{
    default_ducklake_catalog_alias, default_ducklake_data_path, default_ducklake_metadata_path,
    default_ducklake_metadata_schema, DuckLakeConfig,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

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

/// Opaque identity for maps, dedupe, and registry primary keys.
///
/// Crate-internal — not a DSN API and not a SQL prefix. Business callers must
/// not depend on the inner encoding.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ScopeId(String);

impl ScopeId {
    pub(crate) fn from_encoded(encoded: String) -> Self {
        Self(encoded)
    }

    /// Registry / lock map token. Crate-internal only.
    pub(crate) fn registry_token(&self) -> &str {
        &self.0
    }
}

impl Hash for ScopeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Debug for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScopeId(redacted)")
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Stable non-secret label: length only (encoding embeds DSN).
        write!(f, "scope:{}", self.0.len())
    }
}

/// Opaque immutable DuckLake catalog identity.
///
/// Crate-internal token: storage/runtime pass it into capabilities; handlers and
/// external crates never import this type. Public product API is workspace id +
/// engine façades only.
///
/// Path/schema/alias strings are private; storage/runtime use `pub(crate)` codecs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhysicalScope {
    #[serde(default = "default_ducklake_metadata_path")]
    metadata_path: String,
    #[serde(default = "default_ducklake_data_path")]
    data_path: String,
    #[serde(default = "default_ducklake_catalog_alias")]
    catalog_alias: String,
    #[serde(default = "default_ducklake_metadata_schema")]
    metadata_schema: String,
}

impl Default for PhysicalScope {
    fn default() -> Self {
        Self::from_ducklake(&DuckLakeConfig::default())
    }
}

impl fmt::Debug for PhysicalScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalScope")
            .field("catalog_alias", &self.catalog_alias)
            .field("metadata_schema", &self.metadata_schema)
            .field("data_path", &"<redacted>")
            .field("metadata_path", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for PhysicalScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}@{}",
            self.catalog_alias,
            self.metadata_schema,
            self.id()
        )
    }
}

impl PhysicalScope {
    /// Boundary constructor: all identity parts supplied together (immutable).
    /// Crate-internal only (unit tests / storage attach). Outside the crate, use
    /// `open_attached_from_config` / `open_attached_from_warehouse` façades.
    pub(crate) fn new(
        metadata_path: impl Into<String>,
        data_path: impl Into<String>,
        catalog_alias: impl Into<String>,
        metadata_schema: impl Into<String>,
    ) -> Self {
        Self {
            metadata_path: metadata_path.into(),
            data_path: data_path.into(),
            catalog_alias: catalog_alias.into(),
            metadata_schema: metadata_schema.into(),
        }
    }

    /// Capture the physical scope selected by the runtime configuration.
    ///
    /// Config → scope ingress only. Crate-internal (handlers never call this).
    pub(crate) fn from_ducklake(config: &DuckLakeConfig) -> Self {
        Self::new(
            config.metadata_path.clone(),
            config.data_path.clone(),
            config.catalog_alias.clone(),
            config.metadata_schema.clone(),
        )
    }

    /// Opaque map/dedupe/registry identity.
    pub(crate) fn id(&self) -> ScopeId {
        let mut encoded = String::from("ducklake:");
        for part in [
            &self.metadata_path,
            &self.data_path,
            &self.catalog_alias,
            &self.metadata_schema,
        ] {
            encoded.push_str(&part.len().to_string());
            encoded.push(':');
            encoded.push_str(part);
        }
        ScopeId::from_encoded(encoded)
    }

    /// Immutable rebuilder: same identity with a different Postgres metadata schema.
    pub(crate) fn with_pg_namespace(&self, metadata_schema: impl Into<String>) -> Self {
        Self::new(
            self.metadata_path.clone(),
            self.data_path.clone(),
            self.catalog_alias.clone(),
            metadata_schema,
        )
    }

    /// Immutable rebuilder: same identity with a different warehouse URI/path.
    pub(crate) fn with_warehouse_uri(&self, data_path: impl Into<String>) -> Self {
        Self::new(
            self.metadata_path.clone(),
            data_path,
            self.catalog_alias.clone(),
            self.metadata_schema.clone(),
        )
    }

    /// Immutable rebuilder: same identity with a different catalog DSN.
    #[cfg(test)]
    pub(crate) fn with_catalog_dsn(&self, metadata_path: impl Into<String>) -> Self {
        Self::new(
            metadata_path,
            self.data_path.clone(),
            self.catalog_alias.clone(),
            self.metadata_schema.clone(),
        )
    }

    /// Provision from default scope + request overrides (schema + data path).
    pub(crate) fn from_provision(
        default: &Self,
        metadata_schema: impl Into<String>,
        data_path: impl Into<String>,
    ) -> Self {
        Self::new(
            default.metadata_path.clone(),
            data_path,
            default.catalog_alias.clone(),
            metadata_schema,
        )
    }

    /// Registry SELECT row → scope.
    pub(crate) fn from_registry_row(
        metadata_path: String,
        metadata_schema: String,
        data_path: String,
        catalog_alias: String,
    ) -> Self {
        Self::new(metadata_path, data_path, catalog_alias, metadata_schema)
    }

    // --- crate-private codecs for storage/runtime (not public getters) ---

    pub(crate) fn catalog_dsn(&self) -> &str {
        &self.metadata_path
    }

    pub(crate) fn warehouse_uri(&self) -> &str {
        &self.data_path
    }

    pub(crate) fn attach_alias(&self) -> &str {
        &self.catalog_alias
    }

    pub(crate) fn pg_namespace(&self) -> &str {
        &self.metadata_schema
    }

    pub(crate) fn is_default_duckdb_namespace(&self) -> bool {
        self.metadata_schema == "main"
    }

    pub(crate) fn catalog_prefix(&self) -> String {
        if self.is_default_duckdb_namespace() {
            self.catalog_alias.clone()
        } else {
            format!("{}.{}", self.catalog_alias, self.metadata_schema)
        }
    }

    pub(crate) fn qualified_table(&self, bare_table: &str) -> String {
        format!("{}.{}", self.catalog_prefix(), bare_table)
    }

    pub(crate) fn attach_serialization_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.metadata_path, self.metadata_schema, self.data_path, self.catalog_alias
        )
    }

    pub(crate) fn writer_pool_key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.metadata_path, self.metadata_schema, self.data_path
        )
    }

    pub(crate) fn same_warehouse_as(&self, other: &Self) -> bool {
        self.metadata_schema == other.metadata_schema && self.data_path == other.data_path
    }

    pub(crate) fn forbidden_sql_identifiers(&self) -> Vec<String> {
        vec![
            self.metadata_schema.clone(),
            self.catalog_alias.clone(),
            format!("__ducklake_metadata_{}", self.catalog_alias),
            self.metadata_path.clone(),
            self.data_path.clone(),
        ]
    }
}

/// The logical workspace-to-physical-scope binding used by engine contracts.
///
/// Physical identity is crate-private: handlers and protocol adapters must not
/// reach through to [`PhysicalScope`]; use RuntimeEngine / manager façades.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub workspace_id: String,
    physical_scope: PhysicalScope,
    pub mode: WorkspaceScopeMode,
}

impl WorkspaceBinding {
    /// Crate-internal constructor — physical identity is not a public product type.
    pub(crate) fn new(
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

    /// Stable lock / lease map key for this binding's warehouse identity.
    ///
    /// Does not expose [`PhysicalScope`] codecs — only an opaque token.
    pub(crate) fn registry_lock_token(&self) -> String {
        self.physical_scope.id().registry_token().to_string()
    }
}

/// Classified capability used to construct a DuckLake session.
///
/// Crate-internal ATTACH token — not part of the public product API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DuckLakeAccess {
    /// Authenticated workspace access for ingest or query operations.
    Workspace(WorkspaceBinding),
    /// Complete physical-scope access for maintenance and catalog operations.
    Physical(PhysicalScope),
}

impl DuckLakeAccess {
    /// Storage/attach path only — the sole crate funnel from a binding to physical.
    pub(crate) fn physical_scope(&self) -> &PhysicalScope {
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
}

impl SharedScopeErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedBackend => "shared_scope_unsupported_backend",
            Self::NotEnabled => "shared_scope_not_enabled",
            Self::SchemaIncompatible => "shared_scope_schema_incompatible",
            Self::RawSqlForbidden => "shared_scope_raw_sql_forbidden",
            Self::ConnectionUnavailable => "shared_scope_connection_unavailable",
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
        assert_eq!(scope.catalog_dsn(), config.metadata_path.as_str());
        assert_eq!(scope.warehouse_uri(), config.data_path.as_str());
        assert_eq!(scope.attach_alias(), config.catalog_alias.as_str());
        assert_eq!(scope.pg_namespace(), config.metadata_schema.as_str());
        assert!(!scope.id().registry_token().is_empty());
    }

    #[test]
    fn physical_scope_id_changes_when_a_scope_input_changes() {
        let config = DuckLakeConfig::default();
        let first = PhysicalScope::from_ducklake(&config);
        let second = first.with_catalog_dsn(format!("{}-other", first.catalog_dsn()));
        assert_ne!(first.id(), second.id());
    }

    #[test]
    fn catalog_prefix_hides_main_namespace() {
        let main = PhysicalScope::new("dsn", "/data/", "softprobe", "main");
        assert_eq!(main.catalog_prefix(), "softprobe");
        let named = PhysicalScope::new("dsn", "/data/", "softprobe", "tenant_a");
        assert_eq!(named.catalog_prefix(), "softprobe.tenant_a");
        assert_eq!(named.qualified_table("traces"), "softprobe.tenant_a.traces");
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
        let inventory = include_str!("../../../docs/ducklake-access-inventory.md");
        for path in [
            "src/storage/ducklake/attach.rs",
            "src/storage/ducklake/object_store.rs",
            "src/storage/ducklake/writer.rs",
            "src/storage/schema/otlp_layout.rs",
            "src/storage/schema/ducklake_partition.rs",
            "src/storage/ducklake/util.rs",
            "src/storage/duckdb/engine.rs",
            "src/storage/duckdb/cache.rs",
            "src/storage/ducklake/workspace_views.rs",
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
