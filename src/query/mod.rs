//! Analytical query subsystem over DuckLake.
//!
//! Exposes the [`QueryEngine`] surface, query results, and constructors.
//! All execution worker pools, connection lifecycles, and query scheduling
//! are encapsulated within the query engine implementation.

mod engine;

use crate::config::Config;
use crate::storage::ducklake::PhysicalScope;
use crate::workspace_scope::DEFAULT_WORKSPACE_ID;

pub use crate::sql::lake_reads::{LogCountFilter, TraceCountFilter};
pub use engine::{
    self_heal_snapshot, set_self_heal_failures_for_test, HttpSpan, QueryEngine, QueryResult,
    SelfHealSnapshot,
};

/// Construct a query engine using the default physical scope from process configuration.
pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let scope = crate::workspace::physical_scope_from_config(config);
    create_query_engine_for_scope_with_liveness(config, &scope, true, DEFAULT_WORKSPACE_ID).await
}

/// Build a query engine for a bound physical scope with SelfHeal liveness participation.
pub(crate) async fn create_query_engine_for_scope_with_liveness(
    config: &Config,
    scope: &PhysicalScope,
    counts_toward_liveness: bool,
    workspace_id: &str,
) -> anyhow::Result<QueryEngine> {
    QueryEngine::new_with_scope(config, scope, counts_toward_liveness, workspace_id).await
}
