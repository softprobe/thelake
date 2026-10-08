//! Shared axum middleware that injects a local workspace for router-level tests.

use axum::middleware::Next;
use softprobe_runtime::api::AppState;
use softprobe_runtime::authn::WorkspaceAuth;
use softprobe_runtime::workspace::ScopeProvisioningRequest;

/// Default test workspace UUID when no `x-test-workspace-id` header is set.
pub const LOCAL_WORKSPACE_ID: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

// Re-export for compat routers; unused when only `integration_perf` is compiled.
#[allow(unused_imports)]
pub use super::workspace_ids::*;

/// Register [`LOCAL_WORKSPACE_ID`] in the durable scope registry, reusing
/// this process's configured physical scope. Production requires an explicit
/// `POST /v1/workspaces` admin provisioning step before a workspace can resolve a
/// DuckLake scope; router-level tests that bypass real auth via
/// [`inject_local_workspace`] must provision it directly instead.
pub async fn provision_local_workspace(state: &AppState) {
    let ducklake = state.workspaces.config().ducklake.clone();
    state
        .workspaces
        .provision_scope(ScopeProvisioningRequest {
            scope_id: LOCAL_WORKSPACE_ID.to_string(),
            metadata_schema: ducklake.metadata_schema,
            data_path: ducklake.data_path,
        })
        .await
        .expect("provision local workspace scope");
}

/// Header used by multi-workspace Prom isolation tests to select the injected workspace.
pub const TEST_WORKSPACE_HEADER: &str = "x-test-workspace-id";

pub async fn inject_local_workspace(
    mut request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let workspace_id = request
        .headers()
        .get(TEST_WORKSPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(LOCAL_WORKSPACE_ID)
        .to_string();
    request.extensions_mut().insert(WorkspaceAuth {
        workspace_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    });
    next.run(request).await
}
