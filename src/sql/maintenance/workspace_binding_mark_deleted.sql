UPDATE {{registry_schema}}.workspace_scope_binding
SET lifecycle_status = 'deleted', updated_at = NOW()
WHERE workspace_id = $1 AND lifecycle_status = 'deleting';
