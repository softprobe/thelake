SELECT lifecycle_status
FROM {{registry_schema}}.workspace_scope_binding
WHERE workspace_id = $1;
