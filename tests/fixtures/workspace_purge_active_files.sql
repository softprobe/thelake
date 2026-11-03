SELECT df.path, df.path_is_relative, t.path, t.path_is_relative,
       s.path, s.path_is_relative
FROM {{metadata_catalog}}.ducklake_data_file df
JOIN {{metadata_catalog}}.ducklake_table t ON t.table_id = df.table_id
JOIN {{metadata_catalog}}.ducklake_schema s ON s.schema_id = t.schema_id
WHERE t.table_name IN ('traces', 'logs', 'scores', 'score_configs')
  AND s.schema_name = '{{schema}}'
  AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
