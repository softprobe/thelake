use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

/// Background work unit registered with the shared runner.
#[async_trait]
pub trait Job: Send + Sync {
    fn name(&self) -> &'static str;
    fn interval(&self) -> Duration;
    async fn scope_keys(&self) -> Result<Vec<String>>;
    async fn run(&self, scope_key: &str) -> Result<()>;
}
