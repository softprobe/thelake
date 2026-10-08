//! In-process DuckDB sessions: init SQL and httpfs cache wrap.
//!
//! DuckLake ATTACH / catalog identity live in [`super::ducklake`]. This module
//! owns connection bootstrap.

pub(crate) mod cache;
pub(crate) mod init;

pub use init::{apply_duckdb_init, render_duckdb_init, DuckDbInitParams};
