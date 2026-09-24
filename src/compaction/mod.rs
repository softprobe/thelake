//! Physical-scope maintenance: compaction, metadata cleanup, session-summary façade.
//!
//! Public surface: [`MaintenanceEngine`], [`PhysicalScopeMaintenanceJob`], status types.
//! SQL recipes live in [`crate::sql::maintenance`].

mod cleanup;
mod engine;
mod maintenance_job;
mod merge;
mod retry;
pub mod scheduler;
pub(crate) mod session_summary_access;
mod status;
pub(crate) mod twcs;
mod watermark;

pub use engine::{maintenance_table_names, MaintenanceEngine};
pub use maintenance_job::{PhysicalScopeMaintenanceJob, PHYSICAL_SCOPE_MAINTENANCE_JOB};
pub use status::{
    pass_compaction_ok, ActionResult, ActionStatus, MaintenanceSummary, MetadataMaintenanceResult,
    TableMaintenanceResult,
};

pub use twcs::{open_day_files_for_merge, PartitionFileStats, TwcsPolicy};
