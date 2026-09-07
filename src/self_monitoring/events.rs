//! Slow-query ops log events (bounded channel).

use crate::models::Log;
use chrono::Utc;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use tokio::sync::mpsc;

static SLOW_TX: OnceCell<mpsc::Sender<SlowQueryEvent>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct SlowQueryEvent {
    pub tenant: String,
    pub sql_kind: String,
    pub elapsed_ms: u64,
    pub queue_wait_ms: u64,
    pub sql_preview: String,
}

pub fn init_slow_query_channel() -> mpsc::Receiver<SlowQueryEvent> {
    let (tx, rx) = mpsc::channel(256);
    let _ = SLOW_TX.set(tx);
    rx
}

/// Non-blocking enqueue; drops only when the channel is full or unset.
/// Anti-recursion for ops write is `counts_toward_liveness=false` on the ops
/// DuckDB engine — not an export-depth guard on enqueue.
pub fn try_enqueue_slow_query(ev: SlowQueryEvent) {
    if let Some(tx) = SLOW_TX.get() {
        let _ = tx.try_send(ev);
    }
}

pub fn to_log(ev: &SlowQueryEvent) -> Log {
    let mut attributes = HashMap::new();
    attributes.insert("sql_kind".into(), ev.sql_kind.clone());
    attributes.insert("tenant".into(), ev.tenant.clone());
    attributes.insert("elapsed_ms".into(), ev.elapsed_ms.to_string());
    attributes.insert("queue_wait_ms".into(), ev.queue_wait_ms.to_string());
    attributes.insert("event".into(), "thelake.slow_query".into());
    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".into(), "thelake".into());
    Log {
        session_id: None,
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 13, // WARN
        severity_text: "WARN".into(),
        // Prefix so Loki line filters can match without relying on non-indexed attrs.
        body: format!("thelake.slow_query {}", ev.sql_preview),
        attributes,
        resource_attributes,
        trace_id: None,
        span_id: None,
    }
}
