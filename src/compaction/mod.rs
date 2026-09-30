//! Physical-scope maintenance: compaction, metadata cleanup, session-summary façade.
//!
//! Public surface: [`MaintenanceEngine`] and [`PhysicalScopeMaintenanceJob`].
//! SQL recipes live in [`crate::sql::maintenance`].

mod engine;
pub(crate) mod maint_conn_pool;
mod maintenance_job;
pub mod scheduler;
pub(crate) mod session_summary_access;

pub use engine::{maintenance_table_names, MaintenanceEngine};
pub(crate) use maint_conn_pool::MaintenanceConnPool;
pub use maintenance_job::{PhysicalScopeMaintenanceJob, PHYSICAL_SCOPE_MAINTENANCE_JOB};
