INSERT INTO {{dst_schema}}._migration_progress
    (migration_id, table_name, partition_day, src_rows, dst_rows)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (migration_id, table_name, partition_day)
DO UPDATE SET src_rows = EXCLUDED.src_rows,
              dst_rows = EXCLUDED.dst_rows,
              completed_at = NOW();
