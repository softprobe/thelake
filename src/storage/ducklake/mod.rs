// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod object_store;
mod otlp;
mod promotion;
mod scores;
mod util;
mod writer;

pub use object_store::{configure_httpfs_gcs_for_data_path, configure_object_store};
pub use writer::DuckLakeWriter;

pub(crate) use attach::{
    ducklake_attach_options, ducklake_attach_target, ducklake_global_parquet_compression_stmt,
    ducklake_qualified_table_name, ducklake_table_write_option_stmts, prepare_local_ducklake_paths,
    DUCKLAKE_OPT_HIVE_FILE_PATTERN, DUCKLAKE_OPT_PARQUET_COMPRESSION,
    DUCKLAKE_OPT_TARGET_FILE_SIZE,
};
pub(crate) use util::escape_sql_literal;
