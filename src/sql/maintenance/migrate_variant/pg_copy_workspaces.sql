INSERT INTO {{dst_schema}}.workspace_scope_binding ({{insert_list}})
SELECT {{select_list}}
FROM {{src_schema}}.workspace_scope_binding
WHERE workspace_id <> '__self_monitoring__'
ON CONFLICT (workspace_id) DO NOTHING
