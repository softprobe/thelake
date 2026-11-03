INSERT INTO {{registry_schema}}.workspace_scope_binding
  (workspace_id, physical_scope_id, lifecycle_status)
VALUES ($1, $2, 'active')
ON CONFLICT (workspace_id) DO UPDATE SET
  updated_at = NOW()
WHERE {{registry_schema}}.workspace_scope_binding.physical_scope_id = EXCLUDED.physical_scope_id
  AND {{registry_schema}}.workspace_scope_binding.lifecycle_status = 'active'
RETURNING workspace_id;
