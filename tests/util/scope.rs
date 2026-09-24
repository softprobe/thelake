//! Integration-test attach helpers that never name crate-internal catalog identity.
//!
//! The only allowed crate-boundary path for opening an attached DuckDB session
//! from outside `softprobe_runtime` production APIs when an explicit warehouse
//! path/schema is required. Default-config attach uses
//! `open_attached_from_config` directly (public storage façade).

use softprobe_runtime::storage::ducklake::{open_attached_from_warehouse, AttachedSession};

/// Attach using explicit warehouse path/schema (isolation / provisioned-scope checks).
pub fn open_attached_warehouse(
    metadata_path: impl Into<String>,
    data_path: impl Into<String>,
    metadata_schema: impl Into<String>,
    data_inlining_row_limit: Option<u64>,
) -> AttachedSession {
    open_attached_from_warehouse(
        metadata_path,
        data_path,
        metadata_schema,
        "softprobe",
        data_inlining_row_limit,
    )
}
