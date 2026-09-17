//! Serialize session_stats ingest tests that share process-global fault hooks.

use std::sync::OnceLock;
use tokio::sync::Mutex as AsyncMutex;

pub fn session_stats_ingest_serial() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}
