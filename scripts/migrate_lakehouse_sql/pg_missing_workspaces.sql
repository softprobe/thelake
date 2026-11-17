SELECT COUNT(*)
FROM {{src_schema}}.workspace_scope_binding s
WHERE s.workspace_id <> '__self_monitoring__'
  AND NOT EXISTS (
      SELECT 1 FROM {{dst_schema}}.workspace_scope_binding d
      WHERE d.workspace_id = s.workspace_id
  );
