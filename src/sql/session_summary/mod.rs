//! Session-summary SQL recipes: lake reduce/rebuild + Postgres list UPSERT/SELECT.

pub mod list_sql;
pub mod reduce_sql;

pub use list_sql::compile_session_summary_list_sql;
pub use reduce_sql::{
    compile_session_summary_aggregate_sql, compile_session_summary_rebuild_sql,
    compile_session_summary_reduce_sql, compile_session_summary_upsert_sql,
};
