//! Self-monitoring helpers.

/// Reserved ops workspace id; rejected by `POST /v1/workspaces` so it cannot
/// collide with a customer workspace name.
pub const OPS_WORKSPACE_ID: &str = "thelake-ops";

pub fn is_reserved_workspace_id(workspace_id: &str) -> bool {
    workspace_id.trim() == OPS_WORKSPACE_ID
}

/// True when ingest/write/query instrumentation should run for this workspace.
pub fn instrument_customer_workspace(workspace_id: &str) -> bool {
    !is_reserved_workspace_id(workspace_id)
}
