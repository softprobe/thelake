//! Soft coalesce buffer: ack on enqueue; one background worker owns the queue.
//!
//! **Enqueue** sends `Data` on an unbounded channel (after byte-budget wait) and
//! returns. It never writes to DuckLake.
//!
//! **Worker** is the only place that buffers rows and decides when to flush:
//! coalesce timer (`flush_interval_seconds`, first-byte deadline), eager drain
//! near the effective eager threshold, or `Flush` (`force_flush` / tests).
//!
//! Soft budget comes from `ingest.buffer_size_mb` (clamped to
//! [`ABSOLUTE_MAX_PENDING_BYTES`]). Commit / Parquet file sizing is the writer's
//! and maintenance's job.
//!
//! Dropping the last `CoalesceBuf` closes the channel; the worker discards any
//! remaining pending rows (no WAL) and exits.

use anyhow::{anyhow, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, Notify};
use tracing::warn;

type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type WriteFn<T> = Arc<dyn Fn(Vec<Vec<T>>) -> BoxFuture + Send + Sync>;

/// Absolute eager ceiling — soft config cannot raise eager above this.
pub(crate) const ABSOLUTE_EAGER_PENDING_BYTES: usize = 128 * 1024 * 1024; // 128 MiB
/// Absolute hard ceiling — soft `buffer_size_mb` is clamped to this.
pub(crate) const ABSOLUTE_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Resolve soft `buffer_size_mb` into (max_pending, eager_pending) wire bytes.
/// Clamped to absolute ceilings; eager is half of effective max (capped).
pub(crate) fn resolve_byte_limits(buffer_size_mb: u64) -> (usize, usize) {
    let mb = if buffer_size_mb == 0 {
        (ABSOLUTE_MAX_PENDING_BYTES / (1024 * 1024)) as u64
    } else {
        buffer_size_mb
    };
    let soft = (mb as usize).saturating_mul(1024 * 1024);
    let max = soft.clamp(1, ABSOLUTE_MAX_PENDING_BYTES);
    let eager = (max / 2).clamp(1, ABSOLUTE_EAGER_PENDING_BYTES).min(max);
    (max, eager)
}

enum Msg<T> {
    Data { items: Vec<T>, bytes: usize },
    Flush(oneshot::Sender<Result<()>>),
}

/// Per-signal soft coalesce queue (logs / spans / metrics).
pub struct CoalesceBuf<T: Send + 'static> {
    tx: mpsc::UnboundedSender<Msg<T>>,
    /// OTLP body bytes accepted but not yet drained (includes channel in-flight).
    pending_bytes: Arc<AtomicUsize>,
    /// Soft backpressure threshold (≤ [`ABSOLUTE_MAX_PENDING_BYTES`]).
    max_pending_bytes: usize,
    /// Wakes enqueues blocked on `max_pending_bytes`.
    capacity: Arc<Notify>,
}

impl<T: Send + 'static> CoalesceBuf<T> {
    /// Absolute-limit buffer (unit tests).
    #[cfg(test)]
    pub fn new(interval_secs: u64, write: WriteFn<T>) -> Arc<Self> {
        Self::with_limits(
            interval_secs,
            ABSOLUTE_MAX_PENDING_BYTES,
            ABSOLUTE_EAGER_PENDING_BYTES,
            write,
        )
    }

    pub fn with_limits(
        interval_secs: u64,
        max_pending_bytes: usize,
        eager_pending_bytes: usize,
        write: WriteFn<T>,
    ) -> Arc<Self> {
        let max_pending_bytes = max_pending_bytes.clamp(1, ABSOLUTE_MAX_PENDING_BYTES);
        let eager_cap = ABSOLUTE_EAGER_PENDING_BYTES.min(max_pending_bytes).max(1);
        let eager_pending_bytes = eager_pending_bytes.clamp(1, eager_cap);
        let (tx, rx) = mpsc::unbounded_channel();
        let pending_bytes = Arc::new(AtomicUsize::new(0));
        let capacity = Arc::new(Notify::new());
        let this = Arc::new(Self {
            tx,
            pending_bytes: pending_bytes.clone(),
            max_pending_bytes,
            capacity: capacity.clone(),
        });
        spawn_worker(
            rx,
            write,
            Duration::from_secs(interval_secs),
            pending_bytes,
            capacity,
            eager_pending_bytes,
        );
        this
    }

    /// Push a batch. `request_size` is the OTLP body length for this POST.
    /// Waits only when the buffer is at the soft max.
    pub async fn enqueue(&self, items: Vec<T>, request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        wait_for_capacity(
            &self.tx,
            &self.pending_bytes,
            &self.capacity,
            self.max_pending_bytes,
        )
        .await?;
        self.pending_bytes.fetch_add(request_size, Ordering::AcqRel);
        crate::self_monitoring::gauge_store::add_ingest_pending(1);
        self.tx
            .send(Msg::Data {
                items,
                bytes: request_size,
            })
            .map_err(|_| {
                release_budget(&self.pending_bytes, &self.capacity, request_size, 1);
                anyhow!("coalesce worker gone")
            })?;
        Ok(())
    }

    /// Ask the worker to drain and wait until empty (tests / explicit flush).
    pub async fn force_flush(&self) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        loop {
            let (tx, rx) = oneshot::channel();
            if self.tx.send(Msg::Flush(tx)).is_err() {
                return Err(first_err.unwrap_or_else(|| anyhow!("coalesce worker gone")));
            }
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
                    break;
                }
            }
            if self.pending_bytes.load(Ordering::Acquire) == 0 {
                break;
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

async fn wait_for_capacity<T>(
    tx: &mpsc::UnboundedSender<Msg<T>>,
    pending_bytes: &AtomicUsize,
    capacity: &Notify,
    max_pending_bytes: usize,
) -> Result<()> {
    loop {
        let wait = capacity.notified();
        if tx.is_closed() {
            return Err(anyhow!("coalesce worker gone"));
        }
        if pending_bytes.load(Ordering::Acquire) < max_pending_bytes {
            return Ok(());
        }
        tokio::select! {
            _ = wait => {}
            _ = tx.closed() => {
                return Err(anyhow!("coalesce worker gone"));
            }
        }
    }
}

fn spawn_worker<T: Send + 'static>(
    mut rx: mpsc::UnboundedReceiver<Msg<T>>,
    write: WriteFn<T>,
    interval: Duration,
    pending_bytes: Arc<AtomicUsize>,
    capacity: Arc<Notify>,
    eager_pending_bytes: usize,
) {
    tokio::spawn(async move {
        let mut pending: Vec<Vec<T>> = Vec::new();
        let mut local_bytes: usize = 0;
        let mut deadline: Option<Instant> = None;
        let mut flush_acks: Vec<oneshot::Sender<Result<()>>> = Vec::new();

        loop {
            // Last CoalesceBuf dropped: drain channel + local pending without writing.
            if rx.is_closed() {
                discard_on_shutdown(
                    &mut rx,
                    &mut pending,
                    &mut local_bytes,
                    &pending_bytes,
                    &capacity,
                    &mut flush_acks,
                );
                return;
            }

            ingest_ready(
                &mut rx,
                &mut pending,
                &mut local_bytes,
                &mut deadline,
                interval,
                &mut flush_acks,
            );

            if should_flush_now(
                &pending,
                local_bytes,
                deadline,
                interval,
                !flush_acks.is_empty(),
                eager_pending_bytes,
            ) {
                let result = flush_pending(
                    &mut pending,
                    &mut local_bytes,
                    &mut deadline,
                    &pending_bytes,
                    &capacity,
                    &write,
                )
                .await;
                if let Err(e) = &result {
                    crate::self_monitoring::record_job_error("ingest_coalesce", "flush");
                    warn!("coalesce background flush failed after OTLP ack: {e}");
                }
                for ack in flush_acks.drain(..) {
                    let _ = ack.send(match &result {
                        Ok(()) => Ok(()),
                        Err(e) => Err(anyhow!("{e}")),
                    });
                }
                continue;
            }

            if !flush_acks.is_empty() && pending.is_empty() {
                for ack in flush_acks.drain(..) {
                    let _ = ack.send(Ok(()));
                }
                deadline = None;
                continue;
            }

            let timer = deadline.filter(|_| !pending.is_empty() && !interval.is_zero());
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    match msg {
                        None => {
                            discard_on_shutdown(
                                &mut rx,
                                &mut pending,
                                &mut local_bytes,
                                &pending_bytes,
                                &capacity,
                                &mut flush_acks,
                            );
                            return;
                        }
                        Some(msg) => {
                            handle_msg(
                                msg,
                                &mut pending,
                                &mut local_bytes,
                                &mut deadline,
                                interval,
                                &mut flush_acks,
                            );
                        }
                    }
                }
                _ = async {
                    if let Some(d) = timer {
                        let rem = d.saturating_duration_since(Instant::now());
                        tokio::time::sleep(rem).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if timer.is_some() => {
                    // Deadline reached; next loop iteration flushes.
                }
            }
        }
    });
}

fn ingest_ready<T>(
    rx: &mut mpsc::UnboundedReceiver<Msg<T>>,
    pending: &mut Vec<Vec<T>>,
    local_bytes: &mut usize,
    deadline: &mut Option<Instant>,
    interval: Duration,
    flush_acks: &mut Vec<oneshot::Sender<Result<()>>>,
) {
    while let Ok(msg) = rx.try_recv() {
        handle_msg(msg, pending, local_bytes, deadline, interval, flush_acks);
    }
}

fn handle_msg<T>(
    msg: Msg<T>,
    pending: &mut Vec<Vec<T>>,
    local_bytes: &mut usize,
    deadline: &mut Option<Instant>,
    interval: Duration,
    flush_acks: &mut Vec<oneshot::Sender<Result<()>>>,
) {
    match msg {
        Msg::Data { items, bytes } => {
            if pending.is_empty() && !interval.is_zero() {
                *deadline = Some(Instant::now() + interval);
            }
            *local_bytes = local_bytes.saturating_add(bytes);
            pending.push(items);
        }
        Msg::Flush(ack) => flush_acks.push(ack),
    }
}

fn should_flush_now<T>(
    pending: &[Vec<T>],
    local_bytes: usize,
    deadline: Option<Instant>,
    interval: Duration,
    force: bool,
    eager_pending_bytes: usize,
) -> bool {
    if pending.is_empty() {
        return false;
    }
    force
        || interval.is_zero()
        || local_bytes >= eager_pending_bytes
        || deadline.map(|d| Instant::now() >= d).unwrap_or(false)
}

async fn flush_pending<T>(
    pending: &mut Vec<Vec<T>>,
    local_bytes: &mut usize,
    deadline: &mut Option<Instant>,
    pending_bytes: &AtomicUsize,
    capacity: &Notify,
    write: &WriteFn<T>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let batches = std::mem::take(pending);
    let bytes = std::mem::take(local_bytes);
    *deadline = None;
    let n = batches.len();
    release_budget(pending_bytes, capacity, bytes, n);
    (write)(batches).await
}

fn release_budget(pending_bytes: &AtomicUsize, capacity: &Notify, bytes: usize, batches: usize) {
    if bytes > 0 {
        // Saturating: a double-release must not wrap to ~usize::MAX (enqueue
        // would then see "always full").
        let _ = pending_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
            Some(cur.saturating_sub(bytes))
        });
        capacity.notify_waiters();
    }
    if batches > 0 {
        crate::self_monitoring::gauge_store::sub_ingest_pending(batches);
    }
}

fn discard_pending<T>(
    pending: &mut Vec<Vec<T>>,
    local_bytes: &mut usize,
    pending_bytes: &AtomicUsize,
    capacity: &Notify,
) {
    let n = pending.len();
    let bytes = std::mem::take(local_bytes);
    pending.clear();
    release_budget(pending_bytes, capacity, bytes, n);
}

fn discard_on_shutdown<T>(
    rx: &mut mpsc::UnboundedReceiver<Msg<T>>,
    pending: &mut Vec<Vec<T>>,
    local_bytes: &mut usize,
    pending_bytes: &AtomicUsize,
    capacity: &Notify,
    flush_acks: &mut Vec<oneshot::Sender<Result<()>>>,
) {
    loop {
        match rx.try_recv() {
            Ok(Msg::Data { bytes, .. }) => {
                release_budget(pending_bytes, capacity, bytes, 1);
            }
            Ok(Msg::Flush(ack)) => {
                let _ = ack.send(Err(anyhow!("coalesce worker shutting down")));
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
    discard_pending(pending, local_bytes, pending_bytes, capacity);
    for ack in flush_acks.drain(..) {
        let _ = ack.send(Err(anyhow!("coalesce worker shutting down")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;
    use tokio::sync::{Mutex as TokioMutex, Semaphore};

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

    fn gated_writer(
        calls: StdArc<AtomicUsize>,
        rows: StdArc<TokioMutex<Vec<usize>>>,
        gate: StdArc<TokioMutex<()>>,
    ) -> WriteFn<u32> {
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
    }

    fn semaphore_writer(
        calls: StdArc<AtomicUsize>,
        release: StdArc<Semaphore>,
        panic_after: bool,
    ) -> WriteFn<u32> {
        Arc::new(move |_batches| {
            let calls = calls.clone();
            let release = release.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _p = release.acquire().await.unwrap();
                if panic_after {
                    panic!("forced worker death");
                }
                Ok(())
            })
        })
    }

    async fn enq(buf: &CoalesceBuf<u32>, items: Vec<u32>) -> Result<()> {
        let n = items.len();
        buf.enqueue(items, n).await
    }

    async fn wait_calls(calls: &AtomicUsize, n: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while calls.load(Ordering::SeqCst) < n {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("flush worker did not run");
    }

    #[tokio::test]
    async fn interval_zero_worker_flushes_after_enqueue_tick() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(0, counting_writer(calls.clone(), rows.clone(), false));
        enq(&buf, vec![1, 2, 3]).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        wait_calls(&calls, 1).await;
        assert_eq!(*rows.lock().await, vec![3]);
    }

    #[tokio::test]
    async fn interval_zero_coalesces_bursts_via_try_recv() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let gate = StdArc::new(TokioMutex::new(()));
        let hold = gate.clone().lock_owned().await;
        let buf = CoalesceBuf::new(0, gated_writer(calls.clone(), rows.clone(), gate));
        for i in 0..50u32 {
            enq(&buf, vec![i]).await.unwrap();
        }
        drop(hold);
        wait_calls(&calls, 1).await;
        buf.force_flush().await.unwrap();
        let total: usize = rows.lock().await.iter().sum();
        assert_eq!(total, 50);
        assert!(
            calls.load(Ordering::SeqCst) <= 2,
            "gated burst should coalesce into at most a couple writes, got {}",
            calls.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn enqueue_returns_without_waiting_for_write() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let gate = StdArc::new(TokioMutex::new(()));
        let hold = gate.clone().lock_owned().await;
        let buf = CoalesceBuf::new(60, gated_writer(calls.clone(), rows.clone(), gate));
        tokio::time::timeout(Duration::from_millis(200), enq(&buf, vec![1, 2]))
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
    async fn many_small_enqueues_flush_together() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows.clone(), false));
        for i in 0..100u32 {
            enq(&buf, vec![i]).await.unwrap();
        }
        buf.force_flush().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*rows.lock().await, vec![100]);
    }

    #[tokio::test]
    async fn enqueue_during_flush_no_overlapping_writes() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let inflight = StdArc::new(TokioMutex::new(0usize));
        let max_inflight = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));

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
        enq(&buf, vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        enq(&buf, vec![2]).await.unwrap();
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
        enq(&buf, vec![1])
            .await
            .expect("enqueue ok despite later fail");
        let err = buf.force_flush().await;
        assert!(err.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timer_flushes_without_force_flush() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(1, counting_writer(calls.clone(), rows.clone(), false));
        enq(&buf, vec![1, 2, 3]).await.unwrap();
        wait_calls(&calls, 1).await;
        assert_eq!(*rows.lock().await, vec![3]);
    }

    #[tokio::test]
    async fn first_byte_deadline_not_reset_by_later_enqueues() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(1, counting_writer(calls.clone(), rows.clone(), false));
        let start = Instant::now();
        enq(&buf, vec![1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        enq(&buf, vec![2]).await.unwrap();
        wait_calls(&calls, 1).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(1300),
            "deadline appears reset: {elapsed:?}"
        );
        assert_eq!(*rows.lock().await, vec![2]);
    }

    #[tokio::test]
    async fn pending_cap_applies_backpressure_instead_of_unbounded_growth() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let buf = CoalesceBuf::new(60, semaphore_writer(calls.clone(), release.clone(), false));
        enq(&buf, vec![0]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }

        let chunk = ABSOLUTE_EAGER_PENDING_BYTES / 4;
        let fill_n = ABSOLUTE_MAX_PENDING_BYTES / chunk;
        for i in 0..fill_n {
            buf.enqueue(vec![i as u32], chunk).await.unwrap();
        }
        assert!(buf.pending_bytes.load(Ordering::Acquire) >= ABSOLUTE_MAX_PENDING_BYTES);

        let blocked = {
            let buf = buf.clone();
            tokio::spawn(async move {
                buf.enqueue(vec![999], chunk).await.unwrap();
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "enqueue must block once pending hits ABSOLUTE_MAX_PENDING_BYTES"
        );

        release.add_permits(64);
        tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("backpressured enqueue did not complete")
            .unwrap();
        flush.await.unwrap().unwrap();
        buf.force_flush().await.unwrap();
    }

    #[tokio::test]
    async fn capacity_unblocks_at_drain_before_write_finishes() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let buf = CoalesceBuf::new(60, semaphore_writer(calls.clone(), release.clone(), false));
        enq(&buf, vec![0]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }

        let chunk = ABSOLUTE_MAX_PENDING_BYTES / 4;
        for i in 0..4u32 {
            buf.enqueue(vec![i], chunk).await.unwrap();
        }
        let blocked = {
            let buf = buf.clone();
            tokio::spawn(async move {
                buf.enqueue(vec![99], 1).await.unwrap();
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocked.is_finished(), "enqueue should wait on capacity");

        release.add_permits(1);
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("capacity not released at drain (still waiting on write)")
            .unwrap();

        release.add_permits(8);
        flush.await.unwrap().unwrap();
        buf.force_flush().await.unwrap();
    }

    #[tokio::test]
    async fn blocked_enqueue_errors_when_worker_dies() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let buf = CoalesceBuf::new(60, semaphore_writer(calls.clone(), release.clone(), true));
        enq(&buf, vec![0]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let chunk = ABSOLUTE_MAX_PENDING_BYTES / 4;
        for i in 0..4u32 {
            buf.enqueue(vec![i], chunk).await.unwrap();
        }
        let blocked = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.enqueue(vec![99], 1).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocked.is_finished());
        release.add_permits(1);
        let err = tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("waiter hung after worker death")
            .expect("join");
        assert!(err.is_err(), "expected worker-gone error, got {err:?}");
        let _ = flush.await;
    }

    #[tokio::test]
    async fn force_flush_drains_after_write_error() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
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
        enq(&buf, vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        enq(&buf, vec![2]).await.unwrap();
        release.add_permits(2);
        let err = flush.await.unwrap();
        assert!(err.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn force_flush_waits_until_write_completes() {
        let started = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let write: WriteFn<u32> = {
            let started = started.clone();
            let release = release.clone();
            Arc::new(move |_batches| {
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _p = release.acquire().await.unwrap();
                    Ok(())
                })
            })
        };
        let buf = CoalesceBuf::new(60, write);
        enq(&buf, vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        assert!(
            !flush.is_finished(),
            "force_flush must not return while write is in flight"
        );
        release.add_permits(1);
        flush.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn drop_discards_pending_without_write() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(3600, counting_writer(calls.clone(), rows, false));
        enq(&buf, vec![1]).await.unwrap();
        enq(&buf, vec![2, 3]).await.unwrap();
        drop(buf);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "drop must not flush discarded pending batches"
        );
    }

    #[tokio::test]
    async fn drop_during_coalesce_never_writes_even_after_deadline() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(1, counting_writer(calls.clone(), rows, false));
        enq(&buf, vec![1]).await.unwrap();
        drop(buf);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "shutdown must discard, not flush on timer"
        );
    }

    #[tokio::test]
    async fn drop_with_interval_zero_discards_inflight_channel_messages() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let gate = StdArc::new(TokioMutex::new(()));
        let hold = gate.clone().lock_owned().await;
        let buf = CoalesceBuf::new(0, gated_writer(calls.clone(), rows.clone(), gate));
        enq(&buf, vec![0]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        for i in 1..20u32 {
            enq(&buf, vec![i]).await.unwrap();
        }
        drop(buf);
        drop(hold);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "only the in-flight write may complete; queued messages must be discarded"
        );
        assert_eq!(*rows.lock().await, vec![1]);
    }

    #[tokio::test]
    async fn empty_enqueue_is_noop() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(0, counting_writer(calls.clone(), rows, false));
        buf.enqueue(Vec::<u32>::new(), 0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn soft_overshoot_allows_one_request_when_under_max() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let buf = CoalesceBuf::new(60, semaphore_writer(calls.clone(), release.clone(), false));
        let big = ABSOLUTE_MAX_PENDING_BYTES + 1024 * 1024;
        tokio::time::timeout(Duration::from_millis(500), buf.enqueue(vec![1], big))
            .await
            .expect("soft overshoot blocked")
            .unwrap();
        assert!(buf.pending_bytes.load(Ordering::Acquire) > ABSOLUTE_MAX_PENDING_BYTES);
        release.add_permits(4);
        buf.force_flush().await.unwrap();
    }

    #[tokio::test]
    async fn eager_threshold_flushes_before_interval() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows.clone(), false));
        let chunk = ABSOLUTE_EAGER_PENDING_BYTES / 2;
        buf.enqueue(vec![1], chunk).await.unwrap();
        buf.enqueue(vec![2], chunk).await.unwrap();
        wait_calls(&calls, 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let n: usize = rows.lock().await.iter().sum();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn force_flush_on_empty_is_ok() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let buf = CoalesceBuf::new(60, counting_writer(calls.clone(), rows, false));
        buf.force_flush().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mid_flush_arrivals_start_new_coalesce_window() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let rows = StdArc::new(TokioMutex::new(Vec::new()));
        let write: WriteFn<u32> = {
            let calls = calls.clone();
            let release = release.clone();
            let rows = rows.clone();
            Arc::new(move |batches| {
                let calls = calls.clone();
                let release = release.clone();
                let rows = rows.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let n: usize = batches.iter().map(|b| b.len()).sum();
                    rows.lock().await.push(n);
                    let _p = release.acquire().await.unwrap();
                    Ok(())
                })
            })
        };
        let buf = CoalesceBuf::new(60, write);
        enq(&buf, vec![1]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        enq(&buf, vec![2, 3]).await.unwrap();
        release.add_permits(2);
        flush.await.unwrap().unwrap();
        assert_eq!(*rows.lock().await, vec![1, 2]);
    }

    #[test]
    fn buffer_byte_thresholds_are_ordered() {
        const {
            assert!(ABSOLUTE_MAX_PENDING_BYTES > ABSOLUTE_EAGER_PENDING_BYTES);
            assert!(ABSOLUTE_EAGER_PENDING_BYTES > 0);
        };
    }

    #[test]
    fn resolve_byte_limits_clamps_and_halves_eager() {
        let (max, eager) = resolve_byte_limits(1);
        assert_eq!(max, 1024 * 1024);
        assert_eq!(eager, 512 * 1024);

        let (max, eager) = resolve_byte_limits(512); // above absolute
        assert_eq!(max, ABSOLUTE_MAX_PENDING_BYTES);
        assert_eq!(eager, ABSOLUTE_EAGER_PENDING_BYTES);

        let (max, eager) = resolve_byte_limits(0); // treat as default absolute
        assert_eq!(max, ABSOLUTE_MAX_PENDING_BYTES);
        assert_eq!(eager, ABSOLUTE_EAGER_PENDING_BYTES);
    }

    /// Soft `buffer_size_mb: 1` must backpressure at 1 MiB, not the absolute 256.
    #[tokio::test]
    async fn soft_one_mib_buffer_applies_backpressure() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let (max, eager) = resolve_byte_limits(1);
        assert_eq!(max, 1024 * 1024);
        let buf = CoalesceBuf::with_limits(
            60,
            max,
            eager,
            semaphore_writer(calls.clone(), release.clone(), false),
        );
        enq(&buf, vec![0]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let chunk = max / 4;
        for i in 0..4u32 {
            buf.enqueue(vec![i], chunk).await.unwrap();
        }
        assert!(buf.pending_bytes.load(Ordering::Acquire) >= max);
        let blocked = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.enqueue(vec![99], 1).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            !blocked.is_finished(),
            "soft 1 MiB cap must block before absolute 256 MiB"
        );
        release.add_permits(16);
        tokio::time::timeout(Duration::from_secs(3), blocked)
            .await
            .expect("blocked enqueue")
            .unwrap();
        flush.await.unwrap().unwrap();
        buf.force_flush().await.unwrap();
    }

    /// Hung write: after drain frees budget, another soft-max can enqueue, then
    /// the next enqueue blocks — bounded degradation, not unbounded growth.
    #[tokio::test]
    async fn hung_write_allows_one_more_soft_max_then_blocks() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let release = StdArc::new(Semaphore::new(0));
        let (max, eager) = resolve_byte_limits(1);
        let buf = CoalesceBuf::with_limits(
            60,
            max,
            eager,
            semaphore_writer(calls.clone(), release.clone(), false),
        );
        enq(&buf, vec![0]).await.unwrap();
        let flush = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.force_flush().await })
        };
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        // Write #1 held. Fill one soft max into the channel.
        let chunk = max / 4;
        for i in 0..4u32 {
            buf.enqueue(vec![i], chunk).await.unwrap();
        }
        let blocked = {
            let buf = buf.clone();
            tokio::spawn(async move { buf.enqueue(vec![42], chunk).await })
        };
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            !blocked.is_finished(),
            "second soft-max must wait on capacity"
        );
        assert!(
            buf.pending_bytes.load(Ordering::Acquire) <= max + chunk,
            "must not grow unbounded while write is stuck"
        );
        release.add_permits(32);
        let _ = tokio::time::timeout(Duration::from_secs(3), blocked)
            .await
            .expect("eventually unblocks")
            .expect("join");
        flush.await.unwrap().unwrap();
        buf.force_flush().await.unwrap();
    }
}
