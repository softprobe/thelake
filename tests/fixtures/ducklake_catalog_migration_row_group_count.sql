SELECT COUNT(*) FROM "{{SCHEMA}}".ducklake_data_file WHERE row_group_count IS NOT NULL AND row_group_count > 0
