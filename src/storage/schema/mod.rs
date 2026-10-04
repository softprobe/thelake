pub mod arrow;
pub mod attribute_map;
pub mod ducklake_partition;
pub mod otlp_layout;
pub mod tables;
#[deprecated(note = "use attribute_map")]
pub mod variant;

pub use attribute_map::{
    attribute_map_as_json, attribute_map_json_to_string_map, attribute_map_try_cast,
    attribute_map_varchar, encode_attributes_json, hot_map_columns, parquet_select_for_table,
    parse_projected_json_value, prefer_attr_try_cast, prefer_attr_varchar,
    rehydrate_map_json_values,
};
#[allow(deprecated)]
pub use attribute_map::{
    variant_as_json, variant_json_to_string_map, variant_try_cast, variant_varchar,
};
pub(crate) use ducklake_partition::describe_table_columns;
pub use ducklake_partition::{
    describe_probe_count, partition_sort_probe_count, total_schema_probe_count,
};
pub use otlp_layout::insert_order_by;
pub use tables::{OtlpLogsTable, ScoreConfigTable, ScoreTable, TraceTable};
