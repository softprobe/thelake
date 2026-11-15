SELECT
    row_group_id::BIGINT AS row_group_id,
    max(CASE WHEN path_in_schema = 'session_id' THEN COALESCE(stats_min_value, '') END) AS session_min,
    max(CASE WHEN path_in_schema = 'session_id' THEN COALESCE(stats_max_value, '') END) AS session_max,
    max(CASE WHEN path_in_schema = 'timestamp' THEN COALESCE(stats_min_value, '') END) AS ts_min,
    max(CASE WHEN path_in_schema = 'timestamp' THEN COALESCE(stats_max_value, '') END) AS ts_max,
    max(row_group_num_rows)::BIGINT AS num_rows,
    sum(total_uncompressed_size)::BIGINT AS uncompressed_bytes
FROM parquet_metadata(?)
GROUP BY row_group_id
ORDER BY row_group_id;
