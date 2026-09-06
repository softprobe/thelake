pub mod arrow;
pub mod ducklake_partition;
pub mod metrics_layout;
pub mod otlp_layout;
pub mod tables;
pub mod variant;

pub use ducklake_partition::{
    describe_probe_count, describe_table_columns, partition_sort_probe_count,
    table_partition_sort_ready, total_schema_probe_count,
};
pub use metrics_layout::{
    apply_metrics_layout_partition_sort, ensure_metrics_layout_core_tables,
    ensure_metrics_layout_family_tables, union_metrics_from_layout_sql,
    union_metrics_layout_relation_sql, MetricsLayoutTable, MAINTENANCE_METRICS_FAMILY_TABLES,
    METRICS_LAYOUT_COLLAPSE_TABLES, METRICS_LAYOUT_CORE_TABLES, METRICS_LAYOUT_DOWNSAMPLE_TABLES,
};
pub use otlp_layout::ensure_otlp_table_partition_sort;
pub use tables::{OtlpLogsTable, ScoreConfigTable, ScoreTable, TraceTable};
pub use variant::{
    encode_attributes_json, hot_variant_columns, parquet_select_with_variant_casts,
    parse_projected_json_value, variant_as_json, variant_json_to_string_map, variant_try_cast,
    variant_varchar,
};
