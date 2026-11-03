SELECT CASE WHEN EXISTS (
  SELECT 1 FROM __thelake_registry.{{registry_schema}}.thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = {{scope_key}}
    AND holder_id = {{holder_id}}
    AND epoch = {{lease_epoch}}
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;

DELETE FROM {{physical_table}}
WHERE workspace_id = {{workspace_id}};

SELECT CASE WHEN EXISTS (
  SELECT 1 FROM __thelake_registry.{{registry_schema}}.thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = {{scope_key}}
    AND holder_id = {{holder_id}}
    AND epoch = {{lease_epoch}}
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;

CALL ducklake_rewrite_data_files(
  {{catalog}},
  {{table}},
  schema => {{schema}},
  delete_threshold => 0
);
