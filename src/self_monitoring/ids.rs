//! Self-monitoring helpers.

/// Historical reserved id; still rejected by `POST /v1/workspaces` so it cannot
/// collide with a customer workspace name.
pub const OPS_TENANT_ID: &str = "thelake-ops";

pub fn is_reserved_workspace_id(workspace_id: &str) -> bool {
    workspace_id.trim() == OPS_TENANT_ID
}

/// True when ingest/write/query instrumentation should run for this workspace.
pub fn instrument_customer_tenant(workspace_id: &str) -> bool {
    !is_reserved_workspace_id(workspace_id)
}
