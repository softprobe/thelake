//! Analytical query subsystem over DuckLake.
//!
//! Exposes the [`QueryEngine`] surface, query results, and constructors.
//! Execution slots, connection lifecycles, and scheduling live in the shared
//! catalog pool; an engine only binds a workspace identity to that pool.

mod engine;

use crate::config::Config;
use crate::storage::ducklake::CatalogPool;
use crate::workspace_scope::DEFAULT_WORKSPACE_ID;

pub use crate::sql::lake_reads::{LogCountFilter, TraceCountFilter};
pub use engine::{
    self_heal_snapshot, set_self_heal_failures_for_test, HttpSpan, QueryEngine, QueryResult,
    SelfHealSnapshot,
};

/// Construct a query engine over its own pool for the default physical scope
/// from process configuration.
pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let scope = crate::workspace::physical_scope_from_config(config);
    let pool = CatalogPool::open(config, &scope, None).await?;
    QueryEngine::bind(pool, DEFAULT_WORKSPACE_ID, true)
}
