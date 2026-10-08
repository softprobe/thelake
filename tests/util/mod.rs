pub mod config;
pub mod otlp;
pub mod promotion_contract;
pub mod promotion_file_backed;
pub mod promotion_fixtures;
#[cfg(feature = "integration-e2e")]
pub mod scope;
pub mod workspace;
pub mod workspace_ids;

/// Narrow time window for integration data seeded around the test run.
pub fn query_window() -> softprobe_runtime::sql::QueryWindow {
    let now = chrono::Utc::now();
    softprobe_runtime::sql::QueryWindow::try_new(
        now - chrono::Duration::days(30),
        now + chrono::Duration::days(1),
    )
    .expect("valid integration query window")
}

// E2E-only helpers. `integration_perf` needs pipeline + storage_config; the rest
// are for `integration-e2e` modules in the main `tests` binary.
#[cfg(feature = "integration-e2e")]
pub mod http;
#[cfg(feature = "integration-e2e")]
pub mod perf;
#[cfg(feature = "integration-e2e")]
pub mod pipeline;
#[cfg(feature = "integration-e2e")]
pub mod poll;
#[cfg(feature = "integration-e2e")]
pub mod storage_config;
