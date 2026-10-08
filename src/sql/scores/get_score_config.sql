SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR,
       description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR,
       CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR
FROM score_configs
WHERE config_id = {{config_id}}
LIMIT 1
