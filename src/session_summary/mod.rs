//! Session list summary: derived Postgres rows + durable dirty queue + reduce/rebuild jobs.
//!
//! Schema + best-effort dirty UPSERT after coalesced traces commits.
//! Reduce/rebuild: leased jobs on the shared async job runner.

mod ddl;
mod dirty;
mod hot_attrs;
mod job;
mod list;
mod reduce;

pub use ddl::{
    ensure_session_summary_tables, ensure_shared_session_summary_tables,
    session_summary_table_ddls, shared_session_summary_table_ddls,
    validate_shared_session_summary_tables,
};
pub use dirty::{fold_dirty_hints, DirtyHint, SessionSummaryDirty};
pub(crate) use hot_attrs::ensure_product_hot_attrs_for_scope;
pub(crate) use job::WORKSPACE_SESSION_SUMMARY_REBUILD_JOB;
pub use job::{SessionSummaryRebuildJob, SessionSummaryReduceJob};
pub use list::{
    lookup_session_summary_window, lookup_session_summary_window_for_workspace,
    search_session_summary, search_session_summary_for_workspace, SessionSummaryListError,
};
pub use reduce::validate_rebuild_window;
pub(crate) use reduce::SummaryRow;
pub(crate) use reduce::{rebuild_tenant_window, reduce_tenant};

#[cfg(test)]
pub(crate) mod test_span;

#[cfg(test)]
mod reduce_accuracy_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod list_filters_tests;
