//! Postgres SQL templates for bounded capture correlation staging.

use crate::workspace::quote_pg_ident;

pub(crate) const ADVISORY_LOCK: &str = include_str!("capture_correlation/advisory_lock.sql");
pub(crate) const EXPIRE_MAPPINGS: &str = include_str!("capture_correlation/expire_mappings.sql");
pub(crate) const EXPIRE_PENDING: &str = include_str!("capture_correlation/expire_pending.sql");
pub(crate) const EXPIRE_MAPPINGS_BATCH: &str =
    include_str!("capture_correlation/expire_mappings_batch.sql");
pub(crate) const EXPIRE_PENDING_BATCH: &str =
    include_str!("capture_correlation/expire_pending_batch.sql");
pub(crate) const DELETE_WORKSPACE_MAPPINGS: &str =
    include_str!("capture_correlation/delete_workspace_mappings.sql");
pub(crate) const DELETE_WORKSPACE_PENDING: &str =
    include_str!("capture_correlation/delete_workspace_pending.sql");
pub(crate) const UPSERT_MAPPING: &str = include_str!("capture_correlation/upsert_mapping.sql");
pub(crate) const FIND_MAPPING: &str = include_str!("capture_correlation/find_mapping.sql");
pub(crate) const MAP_PENDING: &str = include_str!("capture_correlation/map_pending.sql");
pub(crate) const INSERT_PENDING: &str = include_str!("capture_correlation/insert_pending.sql");
pub(crate) const FIND_PENDING: &str = include_str!("capture_correlation/find_pending.sql");
pub(crate) const PROFILE_USAGE: &str = include_str!("capture_correlation/profile_usage.sql");
pub(crate) const MAPPING_COUNT: &str = include_str!("capture_correlation/mapping_count.sql");
pub(crate) const CLAIM_READY: &str = include_str!("capture_correlation/claim_ready.sql");
pub(crate) const ACK_CLAIM: &str = include_str!("capture_correlation/ack_claim.sql");
pub(crate) const RELEASE_CLAIM: &str = include_str!("capture_correlation/release_claim.sql");

pub(crate) fn render(template: &str, metadata_schema: &str) -> String {
    template.replace("{{schema}}", &quote_pg_ident(metadata_schema))
}

pub(crate) fn schema_ddl(metadata_schema: &str) -> String {
    render(
        include_str!("schema/capture_correlation.sql"),
        metadata_schema,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_correlation_sql_quotes_the_catalog_schema() {
        let ddl = schema_ddl("capture\"catalog");
        assert!(ddl.contains("\"capture\"\"catalog\".capture_session_mappings"));
        assert!(ddl.contains("\"capture\"\"catalog\".pending_capture_spans"));
    }

    #[test]
    fn capture_correlation_schema_ddl_creates_catalog_schema_before_tables() {
        let ddl = schema_ddl("tenant_a");
        let create_schema = ddl
            .find("CREATE SCHEMA IF NOT EXISTS \"tenant_a\";")
            .expect("create tenant schema");
        let create_table = ddl
            .find("CREATE TABLE IF NOT EXISTS \"tenant_a\".capture_session_mappings")
            .expect("create capture session mappings");

        assert!(create_schema < create_table);
    }
}
