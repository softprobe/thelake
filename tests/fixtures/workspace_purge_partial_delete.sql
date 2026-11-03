DELETE FROM {{catalog}}.{{schema}}.traces WHERE workspace_id = '{{workspace_id}}';
DELETE FROM {{catalog}}.{{schema}}.logs WHERE workspace_id = '{{workspace_id}}';
DELETE FROM {{catalog}}.{{schema}}.scores WHERE workspace_id = '{{workspace_id}}';
DELETE FROM {{catalog}}.{{schema}}.score_configs WHERE workspace_id = '{{workspace_id}}';
