-- Canonical Parquet and DuckLake settings shared by runtime, compaction and
-- the one-off export tool. Consumers execute this file and read the variables;
-- do not copy these values into another writer path.
SET VARIABLE thelake_otlp_partition_by = 'year(timestamp), month(timestamp), day(timestamp)';
SET VARIABLE thelake_otlp_sorted_by = 'session_id, trace_id, timestamp';
SET VARIABLE thelake_otlp_row_group_size_bytes = 8388608;
SET VARIABLE thelake_otlp_target_file_size_bytes = 134217728;
SET VARIABLE thelake_otlp_parquet_compression = 'zstd';
SET VARIABLE thelake_otlp_parquet_compression_level = 3;
SET VARIABLE thelake_otlp_data_inlining_row_limit = 500;
SET VARIABLE thelake_otlp_sort_on_insert = true;
SET VARIABLE thelake_otlp_per_thread_output = false;
-- DuckDB's byte-sized Parquet row-group target requires this session setting.
-- Writers retain explicit ORDER BY clauses for the canonical sort contract.
SET VARIABLE thelake_otlp_preserve_insertion_order = false;
