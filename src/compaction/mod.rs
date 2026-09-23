pub mod executor;
mod maintenance_job;
pub mod scheduler;
pub(crate) mod session_summary_access;
pub mod twcs;

pub use executor::MaintenanceEngine;

use crate::runtime_engine::RuntimeEngineManager;
use anyhow::Result;

impl RuntimeEngineManager {
    /// Construct the process maintenance facade. Prefer this over building
    /// [`MaintenanceEngine`] directly so registry ownership stays with the manager.
    pub async fn maintenance_engine(&self) -> Result<MaintenanceEngine> {
        MaintenanceEngine::from_engines(self).await
    }
}
