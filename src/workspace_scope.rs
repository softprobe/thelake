//! Public workspace binding contracts.
//!
//! Product/handler surface: workspace id, scope mode, binding, and shared-scope
//! errors. Opaque DuckLake catalog identity lives in
//! [`crate::storage::ducklake`] and is not part of this public module.

pub use crate::storage::ducklake::physical_scope::{
    effective_workspace_id, SharedScopeError, SharedScopeErrorCode, WorkspaceBinding,
    WorkspaceBindingError, WorkspaceScopeMode, DEFAULT_WORKSPACE_ID,
};
