use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::watch;

use super::LeaseToken;

/// Background work unit registered with the shared runner.
#[async_trait]
pub trait Job: Send + Sync {
    fn name(&self) -> &'static str;
    fn interval(&self) -> Duration;
    async fn scope_keys(&self) -> Result<Vec<String>>;
    async fn run(&self, scope_key: &str) -> Result<()>;

    /// Run under one acquired fencing token. The runner drops this future if
    /// heartbeats report loss; jobs with multiple side effects should also
    /// check the receiver between major actions.
    async fn run_fenced(
        &self,
        scope_key: &str,
        _token: &LeaseToken,
        mut lease_lost: watch::Receiver<bool>,
    ) -> Result<()> {
        tokio::select! {
            result = self.run(scope_key) => result,
            changed = lease_lost.changed() => {
                let _ = changed;
                anyhow::bail!("job lease lost")
            }
        }
    }
}
