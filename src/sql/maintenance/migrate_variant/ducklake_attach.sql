ATTACH 'ducklake:postgres:{{dsn}}' AS src_lake (METADATA_SCHEMA '{{src_schema}}', DATA_PATH '{{src_data_path}}', AUTOMATIC_MIGRATION true, CREATE_IF_NOT_EXISTS false);
ATTACH 'ducklake:postgres:{{dsn}}' AS dst_lake (METADATA_SCHEMA '{{dst_schema}}', DATA_PATH '{{dst_data_path}}', CREATE_IF_NOT_EXISTS false);
