//! Process-wide ingest CPU budget: one heavy critical section at a time.
//!
//! Softprobe's ingest role runs Tokio `worker_threads=1` plus
//! `max_blocking_threads=1`. OTLP decode happens on the worker while DuckLake
//! commits run on the blocking pool — without a gate those two cores stack to
//! ~200% process CPU and fail the live p95&lt;100 budget even when average load
//! is low.
//!
//! Hold this lock around (a) OTLP protobuf/JSON → model decode and (b) DuckLake
//! coalesce flushes / maintenance passes so decode and lake IO never overlap.

use once_cell::sync::Lazy;
use tokio::sync::{Mutex, MutexGuard};

static INGEST_CPU: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Exclusive permit for ingest-side CPU-heavy work (decode or DuckLake write).
pub async fn hold_ingest_cpu() -> MutexGuard<'static, ()> {
    INGEST_CPU.lock().await
}

#[cfg(test)]
mod tests {
    use super::hold_ingest_cpu;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_cpu_gate_is_exclusive() {
        let concurrent = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let concurrent = concurrent.clone();
            let peak = peak.clone();
            handles.push(tokio::spawn(async move {
                let _g = hold_ingest_cpu().await;
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "ingest CPU gate must never allow overlapping holders"
        );
    }
}
