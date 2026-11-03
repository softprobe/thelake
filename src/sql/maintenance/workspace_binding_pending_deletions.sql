SELECT workspace_id
FROM {{registry_schema}}.workspace_scope_binding
WHERE lifecycle_status = 'deleting'
ORDER BY updated_at, workspace_id;
