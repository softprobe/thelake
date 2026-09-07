//! Self-monitoring ops lake constants and helpers.

/// Reserved tenant id for thelake self-monitoring. Auth must map the ops Bearer
/// to this id; `POST /v1/tenants` rejects it on every path.
pub const OPS_TENANT_ID: &str = "thelake-ops";

pub fn is_reserved_tenant_id(tenant_id: &str) -> bool {
    tenant_id.trim() == OPS_TENANT_ID
}

/// True when ingest/write/query instrumentation should run for this tenant.
pub fn instrument_customer_tenant(tenant_id: &str) -> bool {
    !is_reserved_tenant_id(tenant_id)
}
