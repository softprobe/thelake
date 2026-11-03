const WORKSPACE_TABLE_EXISTS_SQL: &str = include_str!("workspace_table_exists.sql");
const WORKSPACE_TABLE_HAS_ROWS_SQL: &str = include_str!("workspace_table_has_rows.sql");
const WORKSPACE_TABLE_PURGE_SQL: &str = include_str!("workspace_table_purge.sql");
const WORKSPACE_SUMMARY_DELETE_SQL: &str = include_str!("workspace_summary_delete.sql");
const WORKSPACE_BINDING_ACCESS_SQL: &str = include_str!("workspace_binding_access.sql");
const WORKSPACE_BINDING_TOMBSTONE_SQL: &str = include_str!("workspace_binding_tombstone.sql");
const WORKSPACE_BINDING_MARK_DELETED_SQL: &str = include_str!("workspace_binding_mark_deleted.sql");
const WORKSPACE_BINDING_RESOLVE_SQL: &str = include_str!("workspace_binding_resolve.sql");
const WORKSPACE_BINDING_PROVISION_SQL: &str = include_str!("workspace_binding_provision.sql");
const WORKSPACE_BINDING_LIST_SQL: &str = include_str!("workspace_binding_list.sql");

pub(crate) fn workspace_table_exists_sql() -> &'static str {
    WORKSPACE_TABLE_EXISTS_SQL
}

pub(crate) fn render_workspace_table_has_rows_sql(physical_table: &str) -> String {
    WORKSPACE_TABLE_HAS_ROWS_SQL.replace("{{physical_table}}", physical_table)
}

pub(crate) fn render_workspace_table_purge_sql(
    physical_table: &str,
    workspace_id: &str,
    catalog: &str,
    table: &str,
    schema: &str,
    registry_schema: &str,
    scope_key: &str,
    holder_id: &str,
    lease_epoch: i64,
) -> String {
    WORKSPACE_TABLE_PURGE_SQL
        .replace("{{physical_table}}", physical_table)
        .replace(
            "{{workspace_id}}",
            &crate::sql::literal::sql_string_literal(workspace_id),
        )
        .replace(
            "{{catalog}}",
            &crate::sql::literal::sql_string_literal(catalog),
        )
        .replace("{{table}}", &crate::sql::literal::sql_string_literal(table))
        .replace(
            "{{scope_key}}",
            &crate::sql::literal::sql_string_literal(scope_key),
        )
        .replace(
            "{{holder_id}}",
            &crate::sql::literal::sql_string_literal(holder_id),
        )
        .replace("{{lease_epoch}}", &lease_epoch.to_string())
        .replace("{{registry_schema}}", &quote_identifier(registry_schema))
        .replace(
            "{{schema}}",
            &crate::sql::literal::sql_string_literal(schema),
        )
}

pub(crate) fn workspace_binding_access_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_ACCESS_SQL.replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_binding_tombstone_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_TOMBSTONE_SQL
        .replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_binding_mark_deleted_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_MARK_DELETED_SQL
        .replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_binding_resolve_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_RESOLVE_SQL.replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_binding_provision_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_PROVISION_SQL
        .replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_binding_list_sql(registry_schema: &str) -> String {
    WORKSPACE_BINDING_LIST_SQL.replace("{{registry_schema}}", &quote_identifier(registry_schema))
}

pub(crate) fn workspace_summary_delete_sql(schema: &str, table: &str) -> String {
    WORKSPACE_SUMMARY_DELETE_SQL
        .replace("{{schema}}", &quote_identifier(schema))
        .replace("{{table}}", &quote_identifier(table))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn workspace_table_purge_sql_escapes_literals_and_checks_the_maintenance_fence() {
        let sql = render_workspace_table_purge_sql(
            "softprobe.shared.traces",
            "workspace'42",
            "softprobe",
            "traces",
            "shared",
            "registry",
            "scope-key",
            "holder'42",
            7,
        );
        let sql = normalize_sql(&sql);

        assert!(sql
            .contains("DELETE FROM softprobe.shared.traces WHERE workspace_id = 'workspace''42';"));
        assert!(sql.contains("CALL ducklake_rewrite_data_files( 'softprobe', 'traces', schema => 'shared', delete_threshold => 0 );"));
        assert!(sql.contains("holder_id = 'holder''42' AND epoch = 7 AND lease_until > now()"));
        assert!(sql.contains("thelake_job_lease"));
    }

    #[test]
    fn workspace_control_plane_sql_is_parameterized_and_schema_qualified() {
        let access = workspace_binding_access_sql("registry\"schema");
        let tombstone = workspace_binding_tombstone_sql("registry\"schema");
        let deleted = workspace_binding_mark_deleted_sql("registry\"schema");
        let resolve = workspace_binding_resolve_sql("registry\"schema");
        let provision = workspace_binding_provision_sql("registry\"schema");
        let list = workspace_binding_list_sql("registry\"schema");
        let summary = workspace_summary_delete_sql("workspace_staging", "session_summary_dirty");

        assert!(normalize_sql(&access).contains("SELECT workspace_id FROM \"registry\"\"schema\".workspace_scope_binding WHERE workspace_id = $1 AND lifecycle_status = 'active' FOR SHARE;"));
        assert!(normalize_sql(&tombstone).contains("INSERT INTO \"registry\"\"schema\".workspace_scope_binding (workspace_id, physical_scope_id, lifecycle_status) VALUES ($1, $2, 'deleting') ON CONFLICT (workspace_id) DO UPDATE SET lifecycle_status = 'deleting', updated_at = NOW();"));
        assert!(normalize_sql(&deleted).contains("UPDATE \"registry\"\"schema\".workspace_scope_binding SET lifecycle_status = 'deleted', updated_at = NOW() WHERE workspace_id = $1 AND lifecycle_status = 'deleting';"));
        assert!(normalize_sql(&summary).contains(
            "DELETE FROM \"workspace_staging\".\"session_summary_dirty\" WHERE workspace_id = $1;"
        ));
        assert!(normalize_sql(&resolve).contains("AND wb.lifecycle_status = 'active'"));
        assert!(normalize_sql(&provision).contains(
            "AND \"registry\"\"schema\".workspace_scope_binding.lifecycle_status = 'active'"
        ));
        assert!(normalize_sql(&list).contains("WHERE wb.lifecycle_status = 'active'"));
    }
}
