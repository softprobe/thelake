SELECT workspace_id
FROM {{registry_schema}}.workspace_scope_binding
WHERE workspace_id = $1 AND lifecycle_status = 'active'
FOR SHARE;
