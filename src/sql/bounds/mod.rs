pub mod execute_gate;
pub mod window;

pub(crate) use execute_gate::ensure_fact_scan_uses_timestamp_pruning;
#[cfg(test)]
pub(crate) use execute_gate::ensure_sql_has_bare_timestamp_predicate;
pub(crate) use execute_gate::{
    execute_batch_checked, execute_batch_for_parquet_ingest, prepare_checked,
};
pub use window::{query_window_from_exclusive_ns, QueryWindow, TimestampFilteredSql};
