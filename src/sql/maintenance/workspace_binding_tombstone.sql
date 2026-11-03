INSERT INTO {{registry_schema}}.workspace_scope_binding
  (workspace_id, physical_scope_id, lifecycle_status)
VALUES ($1, $2, 'deleting')
ON CONFLICT (workspace_id) DO UPDATE SET
  lifecycle_status = 'deleting',
  updated_at = NOW()
WHERE {{registry_schema}}.workspace_scope_binding.lifecycle_status <> 'deleted';
