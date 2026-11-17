SELECT 1
FROM {{dst_schema}}._migration_progress
WHERE migration_id = %s AND table_name = %s AND partition_day = %s;
