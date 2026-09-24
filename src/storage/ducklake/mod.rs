// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod object_store;
mod otlp;
pub(crate) mod physical_scope;
mod promotion;
mod scores;
mod util;
pub(crate) mod workspace_views;
mod writer;

pub(crate) use physical_scope::{
    DuckLakeAccess, PhysicalScope, SharedScopeError, SharedScopeErrorCode, WorkspaceBinding,
    WorkspaceScopeMode,
};
pub(crate) use writer::DuckLakeWriter;

pub(crate) use attach::{
    ducklake_qualified_table_name, ducklake_set_option_scope_for_qualified, DuckLakeSessionFactory,
    DuckLakeSessionKind,
};
pub use attach::{open_attached_from_config, open_attached_from_warehouse, AttachedSession};
pub(crate) use util::{cache_httpfs_disabled_by_env, size_literal};
pub(crate) use workspace_views::validate_shared_workspace_schema;
