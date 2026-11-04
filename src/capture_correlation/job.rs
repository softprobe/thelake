//! Periodic expiry cleanup for durable capture correlation rows.

use crate::workspace::WorkspaceManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub fn start_capture_expiry_sweeper(workspaces: Arc<WorkspaceManager>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = workspaces.sweep_expired_capture_correlations().await {
                warn!(%error, "capture correlation expiry sweep failed");
            }
            tokio::time::sleep(SWEEP_INTERVAL).await;
        }
    })
}
