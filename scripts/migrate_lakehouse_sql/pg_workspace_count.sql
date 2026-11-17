SELECT COUNT(*)
FROM {{src_schema}}.workspace_scope_binding
WHERE workspace_id <> '__self_monitoring__';
