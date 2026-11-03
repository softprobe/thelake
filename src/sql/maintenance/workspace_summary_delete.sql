DELETE FROM {{schema}}.{{table}}
WHERE workspace_id = $1;
