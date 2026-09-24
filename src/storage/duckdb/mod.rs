//! In-process DuckDB sessions: init SQL, query workers, and httpfs cache wrap.
//!
//! DuckLake ATTACH / catalog identity live in [`super::ducklake`]. This module
//! owns connection bootstrap and the query-worker pool.

pub(crate) mod cache;
pub mod engine;
pub(crate) mod init;

pub use engine::{
    self_heal_snapshot, set_self_heal_failures_for_test, DuckDBQueryEngine, QueryResult,
    SelfHealSnapshot,
};
pub use init::{apply_duckdb_init, render_duckdb_init, DuckDbInitParams};
