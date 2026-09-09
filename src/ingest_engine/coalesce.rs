//! Soft coalesce buffer: ack on enqueue; background flush after N seconds.
//!
//! When the last `Arc` is dropped, in-flight timer tasks fail `Weak::upgrade`
//! and leave pending rows discarded (no WAL). An already-running flush may still
//! complete and WARN on write error.

use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tracing::warn;

type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type WriteFn<T> = Arc<dyn Fn(Vec<Vec<T>>) -> BoxFuture + Send + Sync>;

/// Cap batches per DuckLake commit so a slow metrics flush cannot absorb
/// minutes of OTLP requests into one megatransaction (demo IO/CPU hotspot).
const MAX_BATCHES_PER_FLUSH: usize = 2;
/// Hard row cap across those batches (OTLP metrics posts are often ~1k points).
const MAX_ROWS_PER_FLUSH: usize = 4096;
/// Start an eager flush once pending reaches this (must be > [`MAX_BATCHES_PER_FLUSH`]
/// so light load still gets temporal coalesce within `flush_interval_seconds`).
const EAGER_PENDING_BATCHES: usize = 8;
/// Hard queue depth — enqueue waits (OTLP backpressure) instead of growing forever.
const MAX_PENDING_BATCHES: usize = 64;

struct State<T> {
    pending: VecDeque<Vec<T>>,
    pending_rows: usize,
    timer_armed: bool,
    flushing: bool,
    /// `force_flush` / backpressure waiters for the current in-flight write.
    flight_waiters: Vec<oneshot::Sender<Result<()>>>,
}

/// Per-signal soft coalesce queue (logs / spans / metrics).
pub struct CoalesceBuf<T: Send + 'static> {
    interval: Duration,
    state: Arc<Mutex<State<T>>>,
    write: WriteFn<T>,
}

fn drain_capped<T>(pending: &mut VecDeque<Vec<T>>, pending_rows: &mut usize) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut rows = 0usize;
    while let Some(front) = pending.front() {
        let front_len = front.len();
        if !out.is_empty()
            && (out.len() >= MAX_BATCHES_PER_FLUSH || rows + front_len > MAX_ROWS_PER_FLUSH)
        {
            break;
        }
        let batch = pending.pop_front().expect("front checked");
        *pending_rows = pending_rows.saturating_sub(batch.len());
        rows += batch.len();
        out.push(batch);
        if out.len() >= MAX_BATCHES_PER_FLUSH || rows >= MAX_ROWS_PER_FLUSH {
            break;
        }
    }
    out
}

impl<T: Send + 'static> CoalesceBuf<T> {
    pub fn new(interval_secs: u64, write: WriteFn<T>) -> Arc<Self> {
        Arc::new(Self {
            interval: Duration::from_secs(interval_secs.max(1)),
            state: Arc::new(Mutex::new(State {
                pending: VecDeque::new(),
                pending_rows: 0,
                timer_armed: false,
                flushing: false,
                flight_waiters: Vec::new(),
            })),
            write,
        })
    }

    /// Push a batch. Returns after enqueue when under the pending cap (OTLP
    /// ack-on-enqueue). At [`MAX_PENDING_BATCHES`], waits for drain capacity
    /// (backpressure) so the queue cannot grow without bound.
    pub async fn enqueue(self: &Arc<Self>, items: Vec<T>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        loop {
            let wait_rx = {
                let mut g = self.state.lock().await;
                if g.pending.len() >= MAX_PENDING_BATCHES {
                    if g.flushing {
                        let (tx, rx) = oneshot::channel();
                        g.flight_waiters.push(tx);
                        Some(rx)
                    } else {
                        drop(g);
                        let _ = self.flush_once(false).await;
                        None
                    }
                } else {
                    g.pending_rows += items.len();
                    g.pending.push_back(items);
                    crate::self_monitoring::gauge_store::add_ingest_pending(1);
                    let overflow = g.pending.len() >= EAGER_PENDING_BATCHES
                        || g.pending_rows >= MAX_ROWS_PER_FLUSH;
                    if overflow && !g.flushing {
                        drop(g);
                        self.spawn_eager_flush();
                    } else if !g.timer_armed && !g.flushing {
                        g.timer_armed = true;
                        drop(g);
                        self.arm_timer();
                    }
                    return Ok(());
                }
            };
            if let Some(rx) = wait_rx {
                match rx.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(anyhow!("coalesce backpressure waiter dropped")),
                }
            }
            // Retry enqueue after capacity freed (items still owned only on first path).
            // When we waited, loop to push; when we flushed, loop to push.
        }
    }

    /// Drain until empty under single-flight (tests / explicit flush).
    /// Returns the first write error after attempting to drain remaining pending.
    pub async fn force_flush(self: &Arc<Self>) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        loop {
            let wait_rx = {
                let mut g = self.state.lock().await;
                if g.pending.is_empty() && !g.flushing {
                    break;
                }
                if g.flushing {
                    let (tx, rx) = oneshot::channel();
                    g.flight_waiters.push(tx);
                    Some(rx)
                } else {
                    None
                }
            };
            if let Some(rx) = wait_rx {
                match rx.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                    Err(_) => {
                        if first_err.is_none() {
                            first_err = Some(anyhow!("coalesce flush waiter dropped"));
                        }
                    }
                }
                continue;
            }
            match self.flush_once(false).await {
                Ok(()) => {}
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn arm_timer(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let interval = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(interval).await;
            let Some(this) = weak.upgrade() else {
                return;
            };
            let _ = this.flush_drain_timer().await;
        });
    }

    fn spawn_eager_flush(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let _ = this.flush_once(false).await;
        });
    }

    /// Timer path: one capped chunk, then re-arm if overflow remains.
    ///
    /// Do not tight-loop drain — self-mon export can enqueue huge row sets and a
    /// back-to-back drain pegged Softprobe at ~100% CPU for minutes with no OTLP.
    async fn flush_drain_timer(self: &Arc<Self>) {
        match self.flush_once(true).await {
            Ok(()) => {}
            Err(e) => warn!("coalesce background flush failed after OTLP ack: {e}"),
        }
        let should_arm = {
            let g = self.state.lock().await;
            !g.pending.is_empty() && !g.flushing && !g.timer_armed
        };
        if should_arm {
            let mut g = self.state.lock().await;
            if !g.pending.is_empty() && !g.flushing && !g.timer_armed {
                g.timer_armed = true;
                drop(g);
                self.arm_timer();
            }
        }
    }

    async fn flush_once(self: &Arc<Self>, from_timer: bool) -> Result<()> {
        let batches = {
            let mut g = self.state.lock().await;
            // Any drain (timer or force) clears the armed flag; re-arm below if needed.
            g.timer_armed = false;
            if g.flushing {
                // Timer lost the race to force_flush / another timer; re-arm if work remains.
                if from_timer && !g.pending.is_empty() && !g.timer_armed {
                    g.timer_armed = true;
                    drop(g);
                    self.arm_timer();
                }
                return Ok(());
            }
            if g.pending.is_empty() {
                return Ok(());
            }
            g.flushing = true;
            let batches = {
                let State {
                    pending,
                    pending_rows,
                    ..
                } = &mut *g;
                drain_capped(pending, pending_rows)
            };
            crate::self_monitoring::gauge_store::sub_ingest_pending(batches.len());
            batches
        };

        let result = (self.write)(batches).await;

        let waiters = {
            let mut g = self.state.lock().await;
            g.flushing = false;
            std::mem::take(&mut g.flight_waiters)
        };

        let notify = match &result {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow!("{e}")),
        };
        for w in waiters {
            let _ = w.send(match &notify {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow!("{e}")),
            });
        }

        if from_timer {
            // Overflow re-arm is handled by `flush_drain_timer` (paced, not tight-loop).
            return Ok(());
        }

        // Non-timer flush (eager/force): schedule a follow-up if overflow remains.
        let (overflow, has_pending, can_schedule) = {
            let g = self.state.lock().await;
            let overflow = g.pending.len() >= EAGER_PENDING_BATCHES
                || g.pending_rows >= MAX_ROWS_PER_FLUSH;
            let has_pending = !g.pending.is_empty();
            let can_schedule = has_pending && !g.flushing && !g.timer_armed;
            (overflow, has_pending, can_schedule)
        };
        if can_schedule {
            if overflow {
                self.spawn_eager_flush();
            } else if has_pending {
                let mut g = self.state.lock().await;
                if !g.pending.is_empty() && !g.flushing && !g.timer_armed {
                    g.timer_armed = true;
                    drop(g);
                    self.arm_timer();
                }
            }
        }

        result
    }
}

impl<T: Send + 'static> Drop for CoalesceBuf<T> {
    fn drop(&mut self) {
        // Ack-on-enqueue gauges pending depth; discarded rows on engine recycle
        // must heal the counter or ops panels stick high under coalesce.
        // Use try_lock: Drop may run on a tokio worker (cannot blocking_lock).
        let n = self
            .state
            .try_lock()
            .map(|g| g.pending.len())
            .unwrap_or(0);
        crate::self_monitoring::gauge_store::sub_ingest_pending(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;
    use tokio::sync::Mutex as TokioMutex;

    fn counting_writer(
        calls: StdArc<AtomicUsize>,
        rows: StdArc<TokioMutex<Vec<usize>>>,
        fail: bool,
    ) -> WriteFn<u32> {
        Arc::new(move |batches: Vec<Vec<u32>>| {
            let calls = calls.clone();
            let rows = rows.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if fail {
                    return Err(anyhow!("forced write failure"));
                }
                let n: usize = batches.iter().map(|b| b.len()).sum();
                rows.lock().await.push(n);
                Ok(())
            })
        })
    }

    #[tokio::test]
    async fn enqueue_returns_without_waiting_for_write() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let gate = StdArc::new(TokioMutex::new(()));
        let hold = gate.clone().lock_owned().await;

        let write: WriteFn<u32> = {
            let calls = calls.clone();
            let rows = rows.clone();
            let gate = gate.clone();
            Arc::new(move |batches: Vec<Vec<u32>>| {
                let calls = calls.clone();
                let rows = rows.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    let _g = gate.lock().await;
                    calls.fetch_add(1, Ordering::SeqCst);
                    let n: usize = batches.iter().map(|b| b.len()).sum();
                    rows.lock().await.push(n);
                    Ok(())
                })
            })
        };

        let buf = CoalesceBuf::new(60, write);
        // Must return while write is blocked.
        tokio::time::timeout(Duration::from_millis(200), buf.enqueue(vec![1, 2]))
            .await
            .expect("enqueue timed out — blocked on write")
            .expect("enqueue ok");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(hold);
        buf.force_flush().await.expect("flush");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*rows.lock().await, vec![2]);
    }

    #[tokio::test]
    async fn two_enqueues_one_write_after_force_flush() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows.clone(), false));
        buf.enqueue(vec![1]).await.unwrap();
        buf.enqueue(vec![2, 3]).await.unwrap();
        buf.force_flush().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*rows.lock().await, vec![3]);
    }

    #[tokio::test]
    async fn enqueue_during_flush_no_overlapping_writes() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let inflight = StdArc::new(TokioMutex::new(0usize));
        let max_inflight = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(tokio::sync::Semaphore::new(0));

        let write: WriteFn<u32> = {
            let calls = calls.clone();
            let inflight = inflight.clone();
            let max_inflight = max_inflight.clone();
            let release = release.clone();
            Arc::new(move |_batches| {
                let calls = calls.clone();
                let inflight = inflight.clone();
                let max_inflight = max_inflight.clone();
                let release = release.clone();
                Box::pin(async move {
                    {
                        let mut n = inflight.lock().await;
                        *n += 1;
                        max_inflight.fetch_max(*n, Ordering::SeqCst);
                    }
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _permit = release.acquire().await.unwrap();
                    *inflight.lock().await -= 1;
                    Ok(())
                })
            })
        };

        let buf = CoalesceBuf::new(60, write);
        buf.enqueue(vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        // Wait until first write is in flight.
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        buf.enqueue(vec![2]).await.unwrap();
        // Allow both single-flight writes (second runs after first completes).
        release.add_permits(2);
        flush.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(max_inflight.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn background_write_error_does_not_fail_enqueue() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(1, counting_writer(calls.clone(), rows, true));
        buf.enqueue(vec![1])
            .await
            .expect("enqueue ok despite later fail");
        // force_flush surfaces the error for tests; enqueue already succeeded.
        let err = buf.force_flush().await;
        assert!(err.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timer_flushes_without_force_flush() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(1, counting_writer(calls.clone(), rows.clone(), false));
        buf.enqueue(vec![1, 2, 3]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while calls.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("timer did not flush");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*rows.lock().await, vec![3]);
    }

    #[tokio::test]
    async fn force_flush_splits_overflow_into_bounded_writes() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows.clone(), false));
        // Past eager threshold → spawned flushes; force_flush drains remainder.
        for i in 0..(EAGER_PENDING_BATCHES + 1) {
            buf.enqueue(vec![i as u32]).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        buf.force_flush().await.unwrap();
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "overflow must produce more than one DuckLake write"
        );
        let total: usize = rows.lock().await.iter().sum();
        assert_eq!(total, EAGER_PENDING_BATCHES + 1);
        assert!(
            rows
                .lock()
                .await
                .iter()
                .all(|&n| n <= MAX_ROWS_PER_FLUSH && n <= MAX_BATCHES_PER_FLUSH),
            "each write must stay within batch/row caps"
        );
    }

    #[tokio::test]
    async fn row_cap_splits_large_batches() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows.clone(), false));
        let half = MAX_ROWS_PER_FLUSH / 2;
        buf.enqueue(vec![0u32; half]).await.unwrap();
        buf.enqueue(vec![1u32; half]).await.unwrap();
        buf.enqueue(vec![2u32; half]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        buf.force_flush().await.unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        let wrote: Vec<usize> = rows.lock().await.clone();
        assert_eq!(wrote.iter().sum::<usize>(), half * 3);
        assert!(wrote.iter().all(|&n| n <= MAX_ROWS_PER_FLUSH));
    }

    #[tokio::test]
    async fn pending_cap_applies_backpressure_instead_of_unbounded_growth() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(tokio::sync::Semaphore::new(0));

        let write: WriteFn<u32> = {
            let calls = calls.clone();
            let release = release.clone();
            Arc::new(move |batches| {
                let calls = calls.clone();
                let release = release.clone();
                let n = batches.len();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _p = release.acquire().await.unwrap();
                    let _ = n;
                    Ok(())
                })
            })
        };

        let buf = CoalesceBuf::new(60, write);
        // Fill past the hard cap without completing writes (hold flushes on semaphore).
        let filler = {
            let buf = buf.clone();
            tokio::spawn(async move {
                for i in 0..(MAX_PENDING_BATCHES + 8) {
                    buf.enqueue(vec![i as u32]).await.unwrap();
                }
            })
        };

        // Let eager flushes start and block on the semaphore.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(calls.load(Ordering::SeqCst) >= 1);

        // Enqueue task must not finish while writes are blocked past the cap —
        // it should be waiting on backpressure.
        assert!(
            !filler.is_finished(),
            "enqueue must block once pending hits MAX_PENDING_BATCHES"
        );

        // Unblock enough writes to drain and finish the filler.
        release.add_permits(64);
        tokio::time::timeout(Duration::from_secs(5), filler)
            .await
            .expect("backpressured enqueue did not complete")
            .unwrap();
        buf.force_flush().await.unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn force_flush_drains_after_write_error() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(tokio::sync::Semaphore::new(0));
        let fail_next = StdArc::new(AtomicUsize::new(1));
        let write: WriteFn<u32> = {
            let calls = calls.clone();
            let release = release.clone();
            let fail_next = fail_next.clone();
            Arc::new(move |_batches| {
                let calls = calls.clone();
                let release = release.clone();
                let fail_next = fail_next.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _p = release.acquire().await.unwrap();
                    if fail_next.fetch_sub(1, Ordering::SeqCst) == 1 {
                        return Err(anyhow!("first write fails"));
                    }
                    Ok(())
                })
            })
        };
        let buf = CoalesceBuf::new(60, write);
        buf.enqueue(vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        buf.enqueue(vec![2]).await.unwrap();
        release.add_permits(2);
        let err = flush.await.unwrap();
        assert!(err.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn eager_threshold_exceeds_flush_batch_cap() {
        assert!(EAGER_PENDING_BATCHES > MAX_BATCHES_PER_FLUSH);
        assert!(MAX_PENDING_BATCHES > EAGER_PENDING_BATCHES);
    }

    #[test]
    fn drop_heals_ingest_pending_gauge() {
        use crate::self_monitoring::gauge_store::INGEST_PENDING_BATCHES;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let before = INGEST_PENDING_BATCHES.load(Ordering::SeqCst);
        let buf = rt.block_on(async {
            let calls = StdArc::new(AtomicUsize::new(0));
            let rows = StdArc::new(TokioMutex::new(Vec::new()));
            // Long interval so enqueue does not flush before drop.
            let buf = CoalesceBuf::new(3600, counting_writer(calls, rows, false));
            buf.enqueue(vec![1]).await.unwrap();
            buf.enqueue(vec![2, 3]).await.unwrap();
            assert_eq!(
                INGEST_PENDING_BATCHES.load(Ordering::SeqCst),
                before + 2,
                "enqueue must raise pending gauge"
            );
            buf
        });
        // Drop outside the runtime so try_lock is uncontended.
        drop(buf);
        assert_eq!(
            INGEST_PENDING_BATCHES.load(Ordering::SeqCst),
            before,
            "CoalesceBuf drop must heal pending gauge for discarded batches"
        );
    }
}
