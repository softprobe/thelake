// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod object_store;
mod otlp;
mod promotion;
mod scores;
mod util;
pub(crate) mod workspace_views;
mod writer;

pub(crate) use writer::DuckLakeWriter;

pub use attach::AttachedSession;
pub(crate) use attach::{
    ducklake_qualified_table_name, ducklake_set_option_scope_for_qualified,
    open_attached_connection, DuckLakeSessionFactory, DuckLakeSessionKind,
};
pub(crate) use util::{cache_httpfs_disabled_by_env, size_literal};
pub(crate) use workspace_views::validate_shared_workspace_schema;
