pub mod execute_gate;
pub mod window;

pub(crate) use execute_gate::{
    ensure_fact_scan_has_timestamp_predicate, execute_batch_checked, prepare_checked,
};
pub use window::{query_window_from_exclusive_ns, QueryWindow, TimestampFilteredSql};
