pub mod arrow;
pub mod ducklake_partition;
pub mod otlp_layout;
pub mod tables;
pub mod variant;

pub use ducklake_partition::{
    describe_probe_count, describe_table_columns, partition_sort_probe_count,
    table_partition_sort_ready, total_schema_probe_count,
};
pub use otlp_layout::{ensure_otlp_table_partition_sort, insert_order_by};
pub use tables::{OtlpLogsTable, ScoreConfigTable, ScoreTable, TraceTable};
pub use variant::{
    encode_attributes_json, hot_map_columns, parquet_select_for_table, parse_projected_json_value,
    prefer_attr_try_cast, prefer_attr_varchar, rehydrate_map_json_values, variant_as_json,
    variant_json_to_string_map, variant_try_cast, variant_varchar,
};
