SELECT key, value
FROM {{METADATA}}.ducklake_metadata
WHERE scope = 'table'
  AND scope_id = (
    SELECT t.table_id
    FROM {{METADATA}}.ducklake_table t
    JOIN {{METADATA}}.ducklake_schema s ON s.schema_id = t.schema_id
    WHERE t.table_name = 'traces'
      AND s.schema_name = ?
      AND t.end_snapshot IS NULL
  )
  AND key IN (
    'target_file_size',
    'parquet_row_group_size_bytes',
    'parquet_compression',
    'parquet_compression_level'
  )
