//! Session list summary: derived Postgres rows + durable dirty queue + reduce/rebuild jobs.
//!
//! Schema + best-effort dirty UPSERT after coalesced traces commits.
//! Reduce/rebuild: leased jobs on the shared async job runner.

mod ddl;
mod dirty;
mod hot_attrs;
mod job;
mod list;
mod list_sql;
mod reduce;
pub(crate) mod reduce_sql;

pub use ddl::{ensure_session_summary_tables, session_summary_table_ddls};
pub use dirty::{fold_dirty_hints, DirtyHint, SessionSummaryDirty};
pub use hot_attrs::ensure_product_hot_attrs_for_scope;
pub use job::{SessionSummaryRebuildJob, SessionSummaryReduceJob};
pub use list::{search_session_summary, SessionSummaryListError};
pub use reduce::{rebuild_tenant_window, reduce_tenant, validate_rebuild_window};

#[cfg(test)]
pub(crate) mod test_span;

#[cfg(test)]
mod reduce_accuracy_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod list_filters_tests;
