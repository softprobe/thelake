CREATE TABLE IF NOT EXISTS {{dst_schema}}._migration_progress (
    migration_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    partition_day TEXT NOT NULL,
    src_rows BIGINT NOT NULL,
    dst_rows BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (migration_id, table_name, partition_day)
);
