//! Shared axum middleware that injects a local-SQLite tenant for router-level tests.

use axum::middleware::Next;
use softprobe_runtime::api::AppState;
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::runtime_engine::ScopeProvisioningRequest;

pub const LOCAL_SQLITE_TENANT_ID: &str = "local-sqlite-tenant";

/// Register [`LOCAL_SQLITE_TENANT_ID`] in the durable scope registry, reusing
/// this process's configured physical scope. Production requires an explicit
/// `POST /v1/tenants` admin provisioning step before a tenant can resolve a
/// DuckLake scope; router-level tests that bypass real auth via
/// [`inject_local_sqlite_tenant`] must provision it directly instead.
pub async fn provision_local_sqlite_tenant(state: &AppState) {
    let ducklake = state.engines.config().ducklake.clone();
    state
        .engines
        .provision_scope(ScopeProvisioningRequest {
            scope_id: LOCAL_SQLITE_TENANT_ID.to_string(),
            metadata_schema: ducklake.metadata_schema,
            data_path: ducklake.data_path,
        })
        .await
        .expect("provision local-sqlite-tenant scope");
}

/// Header used by multi-tenant Prom isolation tests to select the injected tenant.
pub const TEST_TENANT_HEADER: &str = "x-test-tenant-id";

pub async fn inject_local_sqlite_tenant(
    mut request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let tenant_id = request
        .headers()
        .get(TEST_TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(LOCAL_SQLITE_TENANT_ID)
        .to_string();
    request.extensions_mut().insert(TenantInfo {
        tenant_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    });
    next.run(request).await
}
