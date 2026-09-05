//! Soft coalesce buffer: ack on enqueue; background flush after N seconds.
//!
//! When the last `Arc` is dropped, in-flight timer tasks fail `Weak::upgrade`
//! and leave pending rows discarded (no WAL). An already-running flush may still
//! complete and WARN on write error.

use anyhow::{anyhow, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tracing::warn;

type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type WriteFn<T> = Arc<dyn Fn(Vec<Vec<T>>) -> BoxFuture + Send + Sync>;

struct State<T> {
    pending: Vec<Vec<T>>,
    timer_armed: bool,
    flushing: bool,
    /// `force_flush` waiters for the current in-flight write.
    flight_waiters: Vec<oneshot::Sender<Result<()>>>,
}

/// Per-signal soft coalesce queue (logs / spans / metrics).
pub struct CoalesceBuf<T: Send + 'static> {
    interval: Duration,
    state: Arc<Mutex<State<T>>>,
    write: WriteFn<T>,
}

impl<T: Send + 'static> CoalesceBuf<T> {
    pub fn new(interval_secs: u64, write: WriteFn<T>) -> Arc<Self> {
        Arc::new(Self {
            interval: Duration::from_secs(interval_secs.max(1)),
            state: Arc::new(Mutex::new(State {
                pending: Vec::new(),
                timer_armed: false,
                flushing: false,
                flight_waiters: Vec::new(),
            })),
            write,
        })
    }

    /// Push a batch and return immediately (OTLP ack-on-enqueue).
    pub async fn enqueue(self: &Arc<Self>, items: Vec<T>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut g = self.state.lock().await;
        g.pending.push(items);
        if !g.timer_armed && !g.flushing {
            g.timer_armed = true;
            drop(g);
            self.arm_timer();
        }
        Ok(())
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
            let _ = this.flush_once(true).await;
        });
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
            std::mem::take(&mut g.pending)
        };

        let result = (self.write)(batches).await;

        let (waiters, rearm) = {
            let mut g = self.state.lock().await;
            g.flushing = false;
            let waiters = std::mem::take(&mut g.flight_waiters);
            let has_pending = !g.pending.is_empty();
            let rearm = from_timer && has_pending && !g.timer_armed;
            if rearm {
                g.timer_armed = true;
            }
            (waiters, rearm)
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
            if let Err(e) = &result {
                warn!("coalesce background flush failed after OTLP ack: {e}");
            }
            if rearm {
                self.arm_timer();
            }
            return Ok(());
        }

        result
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
}
