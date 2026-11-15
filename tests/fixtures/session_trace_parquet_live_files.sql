SELECT df.path AS rel_path
FROM __ducklake_metadata_softprobe.ducklake_data_file df
JOIN __ducklake_metadata_softprobe.ducklake_table t ON df.table_id = t.table_id
WHERE t.table_name = 'traces'
  AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL
ORDER BY df.data_file_id;
