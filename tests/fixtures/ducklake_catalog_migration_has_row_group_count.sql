SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = 'ducklake_data_file' AND column_name = 'row_group_count'
)
