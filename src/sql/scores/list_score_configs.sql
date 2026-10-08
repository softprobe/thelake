SELECT config_id::VARCHAR, timestamp::VARCHAR, name::VARCHAR, data_type::VARCHAR,
       description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR,
       CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR
FROM score_configs
ORDER BY timestamp DESC, config_id DESC
