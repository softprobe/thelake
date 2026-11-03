use crate::async_jobs::Job;
use crate::workspace::WorkspaceManager;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

pub(crate) const SHARED_WORKSPACE_PURGE_JOB: &str = "shared_workspace_purge";

/// Retryable worker for durable shared-workspace deletion tombstones.
pub(crate) struct WorkspacePurgeJob {
    workspaces: Arc<WorkspaceManager>,
    interval: Duration,
}

impl WorkspacePurgeJob {
    pub(crate) fn new(workspaces: Arc<WorkspaceManager>, interval: Duration) -> Self {
        Self {
            workspaces,
            interval: interval.max(Duration::from_millis(50)),
        }
    }
}

#[async_trait]
impl Job for WorkspacePurgeJob {
    fn name(&self) -> &'static str {
        SHARED_WORKSPACE_PURGE_JOB
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn scope_keys(&self) -> Result<Vec<String>> {
        self.workspaces.pending_shared_workspace_deletions().await
    }

    async fn run(&self, workspace_id: &str) -> Result<()> {
        let started = Instant::now();
        info!(workspace_id, "shared workspace purge started");
        self.workspaces
            .process_shared_workspace_deletion(workspace_id)
            .await?;
        info!(
            workspace_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "shared workspace purge completed"
        );
        Ok(())
    }
}
