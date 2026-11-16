// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod catalog_pool;
pub(crate) mod layout;
mod object_store;
mod otlp;
pub(crate) mod physical_scope;
mod promotion;
mod scores;
mod util;
pub(crate) mod workspace_views;
mod writer;

pub(crate) use catalog_pool::{
    CatalogPool, DevReset, QueryCaller, QueryCancelToken, QuerySession, QuerySlot,
    QUERY_CANCELLED_MESSAGE,
};
pub(crate) use physical_scope::{
    DuckLakeAccess, PhysicalScope, SharedScopeError, SharedScopeErrorCode, WorkspaceBinding,
    WorkspaceScopeMode,
};
pub(crate) use writer::DuckLakeWriter;

pub(crate) use attach::{
    ducklake_qualified_table_name, DuckLakeSessionFactory, DuckLakeSessionKind,
};
pub use attach::{
    migrate_ducklake_catalog_from_config, open_attached_from_config, open_attached_from_warehouse,
    AttachedSession,
};
pub(crate) use object_store::configure_object_store;
pub(crate) use util::{cache_httpfs_disabled_by_env, escape_sql_literal};
pub(crate) use workspace_views::validate_shared_workspace_schema;
