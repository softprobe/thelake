SELECT ps.metadata_path, ps.ducklake_metadata_schema, ps.data_path, ps.catalog_alias
FROM {{registry_schema}}.workspace_scope_binding wb
JOIN {{registry_schema}}.physical_scope ps
  ON ps.physical_scope_id = wb.physical_scope_id
WHERE wb.workspace_id = $1 AND wb.lifecycle_status = 'active';
