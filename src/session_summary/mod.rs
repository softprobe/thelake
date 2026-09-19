//! Session list summary: derived Postgres rows + durable dirty queue.
//!
//! Schema + best-effort dirty UPSERT after coalesced traces commits.
//! Reduce / list API / rebuild land in later work.

mod ddl;
mod dirty;

pub use ddl::{ensure_session_summary_tables, session_summary_table_ddls};
pub use dirty::{fold_dirty_hints, DirtyHint, SessionSummaryDirty};

#[cfg(test)]
pub(crate) mod test_span;

#[cfg(test)]
mod tests;
