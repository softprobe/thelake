use anyhow::Result;
use softprobe_runtime::config::Config;
use softprobe_runtime::storage::ducklake::migrate_trace_events;

fn main() -> Result<()> {
    let config = Config::load()?;
    migrate_trace_events(&config)
}
