//! Shared axum middleware that injects a local workspace for router-level tests.

use axum::middleware::Next;
use softprobe_runtime::api::AppState;
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::runtime_engine::ScopeProvisioningRequest;

/// Default test workspace UUID when no `x-test-tenant-id` header is set.
pub const LOCAL_WORKSPACE_ID: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

/// Register [`LOCAL_WORKSPACE_ID`] in the durable scope registry, reusing
/// this process's configured physical scope. Production requires an explicit
/// `POST /v1/workspaces` admin provisioning step before a workspace can resolve a
/// DuckLake scope; router-level tests that bypass real auth via
/// [`inject_local_sqlite_tenant`] must provision it directly instead.
pub async fn provision_local_sqlite_tenant(state: &AppState) {
    let ducklake = state.engines.config().ducklake.clone();
    state
        .engines
        .provision_scope(ScopeProvisioningRequest {
            scope_id: LOCAL_WORKSPACE_ID.to_string(),
            metadata_schema: ducklake.metadata_schema,
            data_path: ducklake.data_path,
        })
        .await
        .expect("provision local workspace scope");
}

/// Header used by multi-workspace Prom isolation tests to select the injected workspace.
pub const TEST_TENANT_HEADER: &str = "x-test-tenant-id";

pub async fn inject_local_sqlite_tenant(
    mut request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let workspace_id = request
        .headers()
        .get(TEST_TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(LOCAL_WORKSPACE_ID)
        .to_string();
    request.extensions_mut().insert(TenantInfo {
        workspace_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    });
    next.run(request).await
}
