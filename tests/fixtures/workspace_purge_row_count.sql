SELECT count(*)
FROM {{catalog}}.{{schema}}.{{table}}
WHERE workspace_id = ?
