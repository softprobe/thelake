//! Session-summary SQL recipes: lake reduce/rebuild + Postgres list UPSERT/SELECT.

pub mod list_sql;
pub mod reduce_sql;

pub use list_sql::{
    compile_session_summary_list_sql, compile_session_summary_list_sql_for_workspace,
};
pub use reduce_sql::{
    compile_session_summary_aggregate_sql, compile_session_summary_aggregate_sql_for_workspace,
    compile_session_summary_rebuild_sql, compile_session_summary_rebuild_sql_for_workspace,
    compile_session_summary_reduce_sql, compile_session_summary_reduce_sql_for_workspace,
    compile_session_summary_upsert_sql, compile_session_summary_upsert_sql_for_workspace,
};
