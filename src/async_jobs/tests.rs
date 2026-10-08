use crate::async_jobs::{
    spawn_runner, Job, LeaseStore, LeaseToken, MemoryLeaseStore, PostgresLeaseStore,
};
use crate::config::AsyncJobsConfig;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Shared lease semantics — run against Memory and Postgres.
async fn lease_contract(store: &dyn LeaseStore, prefix: &str) {
    let job = format!("{prefix}-j");
    let scope = format!("{prefix}-s");

    let first = store
        .acquire_lease(&job, &scope, "a", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("empty acquire wins");
    assert_eq!(first.epoch, 1);
    assert!(
        !store
            .try_acquire(&job, &scope, "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "second holder loses while valid"
    );
    let fenced_scope = format!("{prefix}-fenced");
    let old = store
        .acquire_lease(&job, &fenced_scope, "a", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let current = store
        .acquire_lease(&job, &fenced_scope, "b", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(
        current.epoch > old.epoch,
        "steal must advance fencing epoch"
    );
    assert!(store
        .heartbeat_lease(&job, &fenced_scope, &old, Duration::from_secs(60))
        .await
        .is_err());
    store
        .release_lease(&job, &fenced_scope, &old)
        .await
        .unwrap();
    assert!(store
        .heartbeat_lease(&job, &fenced_scope, &current, Duration::from_secs(60))
        .await
        .is_ok());

    // Same holder renew must extend TTL (survive past original short lease).
    let renew_scope = format!("{prefix}-renew");
    let initial_renew = store
        .acquire_lease(&job, &renew_scope, "a", Duration::from_secs(1))
        .await
        .unwrap()
        .expect("initial acquire");
    let renewed = store
        .acquire_lease(&job, &renew_scope, "a", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("same-holder renewal");
    assert_eq!(renewed.epoch, initial_renew.epoch);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        !store
            .try_acquire(&job, &renew_scope, "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "renew-via-acquire must extend lease_until"
    );

    // Heartbeat extends.
    let hb_scope = format!("{prefix}-hb");
    let hb_token = store
        .acquire_lease(&job, &hb_scope, "a", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        store
            .heartbeat_lease(&job, &hb_scope, &hb_token, Duration::from_secs(1))
            .await
            .unwrap();
    }
    assert!(!store
        .try_acquire(&job, &hb_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());
    assert!(store
        .heartbeat_lease(
            &job,
            &hb_scope,
            &LeaseToken {
                holder_id: "b".into(),
                epoch: 0
            },
            Duration::from_secs(60)
        )
        .await
        .is_err());

    // Release then other acquires; non-holder release is a no-op.
    let rel_scope = format!("{prefix}-rel");
    let before_release = store
        .acquire_lease(&job, &rel_scope, "a", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("acquire before release");
    store
        .release_lease(
            &job,
            &rel_scope,
            &LeaseToken {
                holder_id: "b".into(),
                epoch: 0,
            },
        )
        .await
        .unwrap();
    assert!(!store
        .try_acquire(&job, &rel_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());
    store
        .release_lease(&job, &rel_scope, &before_release)
        .await
        .unwrap();
    let next = store
        .acquire_lease(&job, &rel_scope, "b", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("acquire after release");
    assert!(
        next.epoch > before_release.epoch,
        "release must preserve the fence counter"
    );

    // Steal after TTL expiry (ttl floor is 1s on Postgres).
    let steal_scope = format!("{prefix}-steal");
    assert!(store
        .try_acquire(&job, &steal_scope, "a", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(store
        .try_acquire(&job, &steal_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());

    // Same holder re-acquires after expiry (steal-path WHERE, not a foreign steal).
    let self_scope = format!("{prefix}-self-reacq");
    assert!(store
        .try_acquire(&job, &self_scope, "a", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        store
            .try_acquire(&job, &self_scope, "a", Duration::from_secs(60))
            .await
            .unwrap(),
        "same holder must reclaim own expired lease"
    );
    assert!(
        !store
            .try_acquire(&job, &self_scope, "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "reclaim must leave a valid hold"
    );

    // Renew near expiry: extend before TTL elapses, then prove old deadline no longer applies.
    let near_scope = format!("{prefix}-near-exp");
    assert!(store
        .try_acquire(&job, &near_scope, "a", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        store
            .try_acquire(&job, &near_scope, "a", Duration::from_secs(60))
            .await
            .unwrap(),
        "renewal near expiry must succeed"
    );
    tokio::time::sleep(Duration::from_millis(300)).await; // past original 1s deadline
    assert!(
        !store
            .try_acquire(&job, &near_scope, "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "near-expiry renew must extend past the original lease_until"
    );

    // Heartbeat fails for missing / released / stolen keys.
    assert!(store
        .heartbeat_lease(
            &job,
            &format!("{prefix}-missing"),
            &LeaseToken {
                holder_id: "a".into(),
                epoch: 1
            },
            Duration::from_secs(60)
        )
        .await
        .is_err());
    let hb_gone = format!("{prefix}-hb-gone");
    let gone_token = store
        .acquire_lease(&job, &hb_gone, "a", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    store
        .release_lease(&job, &hb_gone, &gone_token)
        .await
        .unwrap();
    assert!(store
        .heartbeat_lease(&job, &hb_gone, &gone_token, Duration::from_secs(60))
        .await
        .is_err());
    let hb_stolen = format!("{prefix}-hb-stolen");
    let stolen_old = store
        .acquire_lease(&job, &hb_stolen, "a", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let stolen_new = store
        .acquire_lease(&job, &hb_stolen, "b", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(store
        .heartbeat_lease(&job, &hb_stolen, &stolen_old, Duration::from_secs(60))
        .await
        .is_err());
    assert!(store
        .heartbeat_lease(&job, &hb_stolen, &stolen_new, Duration::from_secs(60))
        .await
        .is_ok());

    // Former holder release after steal must not drop the new holder's lease.
    let post_loss = format!("{prefix}-post-loss");
    let post_loss_old = store
        .acquire_lease(&job, &post_loss, "a", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let post_loss_new = store
        .acquire_lease(&job, &post_loss, "b", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    store
        .release_lease(&job, &post_loss, &post_loss_old)
        .await
        .unwrap();
    assert!(store
        .heartbeat_lease(&job, &post_loss, &post_loss_new, Duration::from_secs(60))
        .await
        .is_ok());
    assert!(
        !store
            .try_acquire(&job, &post_loss, "c", Duration::from_secs(60))
            .await
            .unwrap(),
        "b must still hold after a's post-loss release"
    );

    // Zero TTL still acquires (Postgres floors to 1s; Memory Instant+0 may expire fast).
    let zero_scope = format!("{prefix}-ttl0");
    assert!(
        store
            .try_acquire(&job, &zero_scope, "a", Duration::ZERO)
            .await
            .unwrap(),
        "zero TTL must still acquire (clamped or Instant+0)"
    );
    assert!(
        store
            .try_acquire(&job, &zero_scope, "a", Duration::from_secs(60))
            .await
            .unwrap(),
        "same holder must be able to renew/reclaim after zero-TTL acquire"
    );
}

#[tokio::test]
async fn memory_lease_contract() {
    lease_contract(&MemoryLeaseStore::new(), "mem").await;
}

/// After expiry, concurrent reclaim among **distinct** holders is a fair race —
/// delayed renewal does not prefer the former holder once `lease_until` passed.
/// (Same holder may renew while held; that path is covered elsewhere.)
#[tokio::test]
async fn expired_lease_concurrent_reclaim_exactly_one_winner() {
    let store = Arc::new(MemoryLeaseStore::new());
    assert!(store
        .try_acquire("j", "expired-race", "former", Duration::from_millis(30))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut handles = Vec::new();
    for holder in ["a", "b", "c", "d", "e"] {
        let s = Arc::clone(&store);
        let h = holder.to_string();
        handles.push(tokio::spawn(async move {
            s.try_acquire("j", "expired-race", &h, Duration::from_secs(60))
                .await
                .unwrap()
        }));
    }
    let wins: usize = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap() as usize)
        .sum();
    assert_eq!(wins, 1, "exactly one winner after expiry race");
}

#[tokio::test]
async fn concurrent_acquire_exactly_one_winner() {
    let store = Arc::new(MemoryLeaseStore::new());
    let mut handles = Vec::new();
    for i in 0..16 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            s.try_acquire("j", "race", &format!("h{i}"), Duration::from_secs(60))
                .await
                .unwrap()
        }));
    }
    let mut wins = 0;
    for h in handles {
        if h.await.unwrap() {
            wins += 1;
        }
    }
    assert_eq!(wins, 1);
}

struct CountingJob {
    runs: AtomicUsize,
    scopes: Vec<String>,
    interval: Duration,
}

#[async_trait]
impl Job for CountingJob {
    fn name(&self) -> &'static str {
        "count"
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.scopes.clone())
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn runner_skips_run_when_lease_lost() {
    let leases = Arc::new(MemoryLeaseStore::new());
    assert!(leases
        .try_acquire("count", "t1", "other", Duration::from_secs(120))
        .await
        .unwrap());

    let job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["t1".into()],
        interval: Duration::from_millis(10),
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-a".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 120,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.abort();
    assert_eq!(runs.runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_due_gates_long_interval_job_under_fast_wake() {
    struct NamedJob {
        name: &'static str,
        runs: AtomicUsize,
        interval: Duration,
    }
    #[async_trait]
    impl Job for NamedJob {
        fn name(&self) -> &'static str {
            self.name
        }
        fn interval(&self) -> Duration {
            self.interval
        }
        async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(vec!["t1".into()])
        }
        async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let leases = Arc::new(MemoryLeaseStore::new());
    let fast = Arc::new(NamedJob {
        name: "fast",
        runs: AtomicUsize::new(0),
        interval: Duration::from_millis(50),
    });
    let slow = Arc::new(NamedJob {
        name: "slow",
        runs: AtomicUsize::new(0),
        interval: Duration::from_secs(3600),
    });
    let fast_runs = Arc::clone(&fast);
    let slow_runs = Arc::clone(&slow);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-due".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        leases,
        vec![fast as Arc<dyn Job>, slow as Arc<dyn Job>],
    )
    .expect("runner");
    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.abort();
    assert!(
        fast_runs.runs.load(Ordering::SeqCst) >= 3,
        "fast job should run many times under 50ms interval"
    );
    assert_eq!(
        slow_runs.runs.load(Ordering::SeqCst),
        1,
        "slow job must run once then due-gate despite fast wake"
    );
}

struct TimedIntervalJob {
    name: &'static str,
    runs: AtomicUsize,
    sleep_ms: u64,
    interval: Duration,
    scopes: Vec<String>,
}

#[async_trait]
impl Job for TimedIntervalJob {
    fn name(&self) -> &'static str {
        self.name
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.scopes.clone())
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn runner_wake_equals_interval_does_not_double_cadence() {
    // Staging used maintenance.interval_seconds == async wake. Completion stamps
    // (and Instant::now() after lease setup) saw elapsed < interval → ~2× cadence.
    // Wake-start stamp + skew must run every wake when work ≪ interval.
    let leases = Arc::new(MemoryLeaseStore::new());
    let interval = Duration::from_millis(200);
    let job = Arc::new(TimedIntervalJob {
        name: "timed",
        runs: AtomicUsize::new(0),
        sleep_ms: 80,
        interval,
        scopes: vec!["t1".into()],
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-wake-eq-interval".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(650)).await;
    handle.abort();
    let n = runs.runs.load(Ordering::SeqCst);
    assert!(
        n >= 3,
        "wake==interval must run every wake after wake-start stamp; got {n} (2× cadence would be ~2)"
    );
}

#[tokio::test]
async fn runner_overrun_pass_does_not_busy_loop_on_delay_catchup() {
    // When run > interval, Tokio Delay returns an immediate catch-up tick.
    // Overrun success must re-stamp so that tick is not_due (no back-to-back).
    let leases = Arc::new(MemoryLeaseStore::new());
    let interval = Duration::from_millis(100);
    let job = Arc::new(TimedIntervalJob {
        name: "overrun",
        runs: AtomicUsize::new(0),
        sleep_ms: 250,
        interval,
        scopes: vec!["t1".into()],
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-overrun".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(320)).await;
    let mid = runs.runs.load(Ordering::SeqCst);
    handle.abort();
    assert_eq!(
        mid, 1,
        "Delay catch-up after overrun must not start a second pass immediately; got {mid}"
    );
}

#[tokio::test]
async fn runner_multi_scope_overrun_does_not_rerun_early_scopes_on_catchup() {
    // Cumulative scope work crosses wake; earlier scopes must be restamped.
    let leases = Arc::new(MemoryLeaseStore::new());
    let interval = Duration::from_millis(100);
    let job = Arc::new(TimedIntervalJob {
        name: "multi_overrun",
        runs: AtomicUsize::new(0),
        sleep_ms: 60, // 60+60 > 100
        interval,
        scopes: vec!["a".into(), "b".into()],
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-multi-overrun".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(180)).await;
    let mid = runs.runs.load(Ordering::SeqCst);
    handle.abort();
    assert_eq!(
        mid, 2,
        "catch-up must not re-run early scopes after multi-scope overrun; got {mid}"
    );
}

#[tokio::test]
async fn runner_ok_then_err_overrun_does_not_rerun_ok_scope_on_catchup() {
    struct MixedJob {
        ok_runs: AtomicUsize,
        bad_runs: AtomicUsize,
        interval: Duration,
    }
    #[async_trait]
    impl Job for MixedJob {
        fn name(&self) -> &'static str {
            "mixed"
        }
        fn interval(&self) -> Duration {
            self.interval
        }
        async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(vec!["ok".into(), "bad".into()])
        }
        async fn run(&self, scope_key: &str) -> anyhow::Result<()> {
            tokio::time::sleep(Duration::from_millis(60)).await;
            if scope_key == "bad" {
                self.bad_runs.fetch_add(1, Ordering::SeqCst);
                return Err(anyhow::anyhow!("injected"));
            }
            self.ok_runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(MixedJob {
        ok_runs: AtomicUsize::new(0),
        bad_runs: AtomicUsize::new(0),
        interval: Duration::from_millis(100),
    });
    let ok_runs = Arc::clone(&job);
    let bad_runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-ok-err-overrun".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    // Wait until cleared `bad` has retried on Delay catch-up, but before the
    // next full interval can legitimately re-run `ok`.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    loop {
        if bad_runs.bad_runs.load(Ordering::SeqCst) >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ok = ok_runs.ok_runs.load(Ordering::SeqCst);
    let bad = bad_runs.bad_runs.load(Ordering::SeqCst);
    handle.abort();
    assert_eq!(
        ok, 1,
        "Ok scope must not re-run after overrun restamp; got {ok}"
    );
    assert!(
        bad >= 2,
        "cleared Err scope should retry on catch-up; got {bad}"
    );
}

#[tokio::test]
async fn runner_cross_job_overrun_does_not_rerun_earlier_job_on_catchup() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let interval = Duration::from_millis(100);
    let first = Arc::new(TimedIntervalJob {
        name: "first",
        runs: AtomicUsize::new(0),
        sleep_ms: 40,
        interval,
        scopes: vec!["t1".into()],
    });
    let second = Arc::new(TimedIntervalJob {
        name: "second",
        runs: AtomicUsize::new(0),
        sleep_ms: 80,
        interval,
        scopes: vec!["t1".into()],
    });
    let first_runs = Arc::clone(&first);
    let second_runs = Arc::clone(&second);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-cross-job-overrun".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        leases,
        vec![first as Arc<dyn Job>, second as Arc<dyn Job>],
    )
    .expect("runner");
    // first 40ms + second 80ms = 120ms > wake 100ms → catch-up must not re-run first.
    tokio::time::sleep(Duration::from_millis(180)).await;
    let a = first_runs.runs.load(Ordering::SeqCst);
    let b = second_runs.runs.load(Ordering::SeqCst);
    handle.abort();
    assert_eq!(
        a, 1,
        "earlier job must not re-run on Delay catch-up; got {a}"
    );
    assert_eq!(b, 1, "later job must run once; got {b}");
}

#[tokio::test]
async fn runner_retries_after_panic() {
    struct AlwaysPanicJob {
        runs: AtomicUsize,
        interval: Duration,
    }
    #[async_trait]
    impl Job for AlwaysPanicJob {
        fn name(&self) -> &'static str {
            "always_panic"
        }
        fn interval(&self) -> Duration {
            self.interval
        }
        async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(vec!["panic-scope".into()])
        }
        async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            panic!("injected panic");
        }
    }

    let leases = Arc::new(MemoryLeaseStore::new());
    let panic_job = Arc::new(AlwaysPanicJob {
        runs: AtomicUsize::new(0),
        interval: Duration::from_millis(40),
    });
    let runs = Arc::clone(&panic_job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-retry-panic".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![panic_job as Arc<dyn Job>]).expect("runner");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while runs.runs.load(Ordering::SeqCst) < 2 {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 2,
        "panic must clear due stamp so next wake retries; got {}",
        runs.runs.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn runner_runs_when_lease_won() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["t1".into()],
        interval: Duration::from_millis(10),
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-b".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "expected at least one run"
    );
}

/// Heartbeat errors are logged/counted and cancel the current run.
struct HeartbeatFailStore {
    inner: MemoryLeaseStore,
    heartbeat_calls: AtomicUsize,
}

#[async_trait]
impl LeaseStore for HeartbeatFailStore {
    async fn acquire_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> anyhow::Result<Option<LeaseToken>> {
        self.inner
            .acquire_lease(job_name, scope_key, holder_id, ttl)
            .await
    }

    async fn heartbeat_lease(
        &self,
        _job_name: &str,
        _scope_key: &str,
        _token: &LeaseToken,
        _ttl: Duration,
    ) -> anyhow::Result<()> {
        self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("injected heartbeat failure"))
    }

    async fn release_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
    ) -> anyhow::Result<()> {
        self.inner.release_lease(job_name, scope_key, token).await
    }
}

struct SlowJob {
    runs: AtomicUsize,
    sleep_ms: u64,
}

#[async_trait]
impl Job for SlowJob {
    fn name(&self) -> &'static str {
        "slow"
    }
    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["t1".into()])
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn runner_aborts_run_after_heartbeat_failure() {
    let leases = Arc::new(HeartbeatFailStore {
        inner: MemoryLeaseStore::new(),
        heartbeat_calls: AtomicUsize::new(0),
    });
    let hb = Arc::clone(&leases);
    let job = Arc::new(SlowJob {
        runs: AtomicUsize::new(0),
        sleep_ms: 1200, // > heartbeat_seconds so HB fires after skipped first tick
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-hb-fail".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(1600)).await;
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) == 0,
        "job future must be cancelled when heartbeat fails"
    );
    assert!(
        hb.heartbeat_calls.load(Ordering::SeqCst) >= 1,
        "heartbeat must have been attempted"
    );
}

/// try_acquire DB errors skip the scope and continue the wake.
struct AcquireFailStore {
    fail_scopes: std::sync::Mutex<std::collections::HashSet<String>>,
    inner: MemoryLeaseStore,
}

#[async_trait]
impl LeaseStore for AcquireFailStore {
    async fn acquire_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> anyhow::Result<Option<LeaseToken>> {
        if self.fail_scopes.lock().unwrap().contains(scope_key) {
            return Err(anyhow::anyhow!("injected acquire failure"));
        }
        self.inner
            .acquire_lease(job_name, scope_key, holder_id, ttl)
            .await
    }

    async fn heartbeat_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
        ttl: Duration,
    ) -> anyhow::Result<()> {
        self.inner
            .heartbeat_lease(job_name, scope_key, token, ttl)
            .await
    }

    async fn release_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
    ) -> anyhow::Result<()> {
        self.inner.release_lease(job_name, scope_key, token).await
    }
}

#[tokio::test]
async fn runner_skips_scope_on_acquire_error_and_continues() {
    let mut fail = std::collections::HashSet::new();
    fail.insert("bad".into());
    let leases = Arc::new(AcquireFailStore {
        fail_scopes: std::sync::Mutex::new(fail),
        inner: MemoryLeaseStore::new(),
    });
    let job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["bad".into(), "good".into()],
        // One pass in the test window — avoid a second cycle counting as dual-run.
        interval: Duration::from_secs(3600),
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-acq-fail".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    // Wait for the first (and only) wake to finish the good scope.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while runs.runs.load(Ordering::SeqCst) < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Long job interval (3600s) — a short settle must not see a second wake.
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.abort();
    assert_eq!(
        runs.runs.load(Ordering::SeqCst),
        1,
        "only the good scope should run"
    );
}

struct SlowPanicJob {
    ran: AtomicUsize,
    sleep_ms: u64,
}

#[async_trait]
impl Job for SlowPanicJob {
    fn name(&self) -> &'static str {
        "panic_job"
    }
    fn interval(&self) -> Duration {
        // Once per test window — avoid a second acquire/HB after unwind.
        Duration::from_secs(3600)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["t1".into()])
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        self.ran.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        panic!("injected job panic");
    }
}

/// Job panic must be caught (runner continues) and stop the heartbeat via RAII.
#[tokio::test]
async fn runner_survives_job_panic_and_stops_heartbeat() {
    let leases = Arc::new(HeartbeatFailStore {
        inner: MemoryLeaseStore::new(),
        heartbeat_calls: AtomicUsize::new(0),
    });
    let hb_calls = Arc::clone(&leases);
    let panic_job = Arc::new(SlowPanicJob {
        ran: AtomicUsize::new(0),
        sleep_ms: 1100, // past first HB tick (skip + 1s)
    });
    let ran = Arc::clone(&panic_job);
    let count_job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["t2".into()],
        interval: Duration::from_millis(10),
    });
    let runs = Arc::clone(&count_job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-panic".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        leases,
        vec![panic_job as Arc<dyn Job>, count_job as Arc<dyn Job>],
    )
    .expect("runner");
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(
        ran.ran.load(Ordering::SeqCst) >= 1,
        "panic job must have run"
    );
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "sibling job must still run after peer panic"
    );
    let hb_at_stop = hb_calls.heartbeat_calls.load(Ordering::SeqCst);
    assert!(hb_at_stop >= 1, "heartbeat should have fired before panic");
    handle.abort();
    // Leaked HB tasks would keep ticking after the runner is aborted.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        hb_calls.heartbeat_calls.load(Ordering::SeqCst),
        hb_at_stop,
        "heartbeat must stop after panic (RAII guard + join)"
    );
}

struct FailOneScopeJob {
    ran: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl Job for FailOneScopeJob {
    fn name(&self) -> &'static str {
        "fail_one"
    }
    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["bad".into(), "good".into()])
    }
    async fn run(&self, scope_key: &str) -> anyhow::Result<()> {
        self.ran.lock().unwrap().push(scope_key.to_string());
        if scope_key == "bad" {
            return Err(anyhow::anyhow!("injected scope failure"));
        }
        Ok(())
    }
}

/// A scope `Err` must not skip later scopes in the same wake.
#[tokio::test]
async fn runner_continues_after_scope_run_error() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(FailOneScopeJob {
        ran: std::sync::Mutex::new(Vec::new()),
    });
    let ran = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-scope-err".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    // Poll rather than a fixed sleep: under suite load the first wake can take
    // longer than 100ms (HB spawn/join around each scope).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let seen = loop {
        let snapshot = ran.ran.lock().unwrap().clone();
        if snapshot.iter().any(|s| s == "bad") && snapshot.iter().any(|s| s == "good") {
            break snapshot;
        }
        if tokio::time::Instant::now() >= deadline {
            break snapshot;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    handle.abort();
    assert!(
        seen.iter().any(|s| s == "bad"),
        "failing scope must still be attempted: {seen:?}"
    );
    assert!(
        seen.iter().any(|s| s == "good"),
        "later scope must run after peer Err: {seen:?}"
    );
}

/// Memory store honors sub-second TTLs; Postgres floors to 1s (see lease.rs).
#[tokio::test]
async fn memory_honors_subsecond_ttl() {
    let store = MemoryLeaseStore::new();
    assert!(store
        .try_acquire("j", "subsec", "a", Duration::from_millis(80))
        .await
        .unwrap());
    assert!(
        !store
            .try_acquire("j", "subsec", "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "must still be held before expiry"
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        store
            .try_acquire("j", "subsec", "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "sub-second TTL must expire on Memory"
    );
}

/// Connect to local ducklake-postgres when up (make setup / make test).
async fn try_postgres_store(schema: &str) -> Option<PostgresLeaseStore> {
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    use tokio_postgres::NoTls;

    let mut pg = tokio_postgres::Config::new();
    pg.host("localhost");
    pg.port(5432);
    pg.dbname("ducklake");
    pg.user("ducklake");
    pg.password("ducklake");
    let mgr = Manager::from_config(
        pg,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr).max_size(4).build().ok()?;
    let client = match tokio::time::timeout(Duration::from_secs(2), pool.get()).await {
        Ok(Ok(c)) => c,
        _ => return None,
    };
    let q = crate::workspace::quote_pg_ident(schema);
    client
        .execute(&format!("CREATE SCHEMA IF NOT EXISTS {q}"), &[])
        .await
        .ok()?;
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS {q}.thelake_job_lease (
  job_name TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  holder_id TEXT NOT NULL,
  epoch BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_name, scope_key)
)"#
            ),
            &[],
        )
        .await
        .ok()?;
    client
        .execute(
            &format!("ALTER TABLE {q}.thelake_job_lease ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 1"),
            &[],
        )
        .await
        .ok()?;
    // Isolate this test run.
    client
        .execute(&format!("TRUNCATE {q}.thelake_job_lease"), &[])
        .await
        .ok()?;
    Some(PostgresLeaseStore::new(pool, schema))
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_lease_contract() {
    let store = try_postgres_store("thelake_lease_ut")
        .await
        .expect("ducklake-postgres required (make setup)");
    lease_contract(&store, "pg").await;
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_concurrent_acquire_exactly_one_winner() {
    let store = try_postgres_store("thelake_lease_race")
        .await
        .expect("ducklake-postgres required (make setup)");
    let store = Arc::new(store);
    let mut handles = Vec::new();
    for i in 0..16 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            s.try_acquire("j", "race", &format!("h{i}"), Duration::from_secs(60))
                .await
                .unwrap()
        }));
    }
    let mut wins = 0;
    for h in handles {
        if h.await.unwrap() {
            wins += 1;
        }
    }
    assert_eq!(wins, 1);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_expired_lease_concurrent_reclaim_exactly_one_winner() {
    let store = try_postgres_store("thelake_lease_expired_race")
        .await
        .expect("ducklake-postgres required (make setup)");
    let store = Arc::new(store);
    assert!(store
        .try_acquire("j", "expired-race", "former", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut handles = Vec::new();
    for holder in ["a", "b", "c", "d", "e"] {
        let s = Arc::clone(&store);
        let h = holder.to_string();
        handles.push(tokio::spawn(async move {
            s.try_acquire("j", "expired-race", &h, Duration::from_secs(60))
                .await
                .unwrap()
        }));
    }
    let wins: usize = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap() as usize)
        .sum();
    assert_eq!(wins, 1, "exactly one winner after PG expiry race");
}

struct PostgresLeaseLossJob {
    started: tokio::sync::Notify,
    after_loss_boundary: AtomicUsize,
}

#[async_trait]
impl Job for PostgresLeaseLossJob {
    fn name(&self) -> &'static str {
        "pg_lease_loss_fence"
    }
    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["maintenance".into()])
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn run_fenced(
        &self,
        _scope_key: &str,
        _token: &LeaseToken,
        mut lease_lost: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.started.notify_one();
        // Represents an in-flight maintenance action. The next destructive
        // boundary must never execute after the runner observes lease loss.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            changed = lease_lost.changed() => { let _ = changed; anyhow::bail!("lease lost") },
        }
        if *lease_lost.borrow() {
            anyhow::bail!("lease lost before next boundary");
        }
        self.after_loss_boundary.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_runner_cancels_maintenance_after_lease_is_stolen() {
    let store = try_postgres_store("thelake_lease_loss")
        .await
        .expect("ducklake-postgres required (make setup)");
    let mut pg = tokio_postgres::Config::new();
    pg.host("localhost");
    pg.port(5432);
    pg.dbname("ducklake");
    pg.user("ducklake");
    pg.password("ducklake");
    let (client, connection) = pg
        .connect(tokio_postgres::NoTls)
        .await
        .expect("control connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let job = Arc::new(PostgresLeaseLossJob {
        started: tokio::sync::Notify::new(),
        after_loss_boundary: AtomicUsize::new(0),
    });
    let cfg = AsyncJobsConfig {
        instance_id: Some("pg-maintenance-old-holder".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 10,
    };
    let runner = spawn_runner(
        &cfg,
        Arc::new(store.clone()),
        vec![job.clone() as Arc<dyn Job>],
    )
    .expect("runner");
    tokio::time::timeout(Duration::from_secs(3), job.started.notified())
        .await
        .expect("maintenance started");

    let q = crate::workspace::quote_pg_ident("thelake_lease_loss");
    client.execute(
        &format!("UPDATE {q}.thelake_job_lease SET lease_until = now() - interval '1 second' WHERE job_name = 'pg_lease_loss_fence' AND scope_key = 'maintenance'"),
        &[],
    ).await.expect("expire old lease");
    let peer = store
        .acquire_lease(
            "pg_lease_loss_fence",
            "maintenance",
            "pg-maintenance-new-holder",
            Duration::from_secs(10),
        )
        .await
        .expect("peer acquire query")
        .expect("peer steals expired lease");
    tokio::time::sleep(Duration::from_millis(1300)).await;
    runner.abort();
    assert_eq!(
        job.after_loss_boundary.load(Ordering::SeqCst),
        0,
        "no later maintenance boundary may run under a stale fencing token"
    );
    assert!(
        store
            .heartbeat_lease(
                "pg_lease_loss_fence",
                "maintenance",
                &peer,
                Duration::from_secs(10)
            )
            .await
            .is_ok(),
        "stale runner cleanup must not release the peer's lease"
    );
}

#[test]
fn spawn_runner_returns_none_for_empty_jobs() {
    let cfg = AsyncJobsConfig::default();
    let leases = Arc::new(MemoryLeaseStore::new());
    assert!(spawn_runner(&cfg, leases, vec![]).is_none());
}

#[test]
fn async_jobs_config_rejects_heartbeat_ge_ttl() {
    let bad = AsyncJobsConfig {
        instance_id: None,
        lease_ttl_seconds: 30,
        heartbeat_seconds: 30,
    };
    assert!(bad.validate().is_err());
    let also_bad = AsyncJobsConfig {
        instance_id: None,
        lease_ttl_seconds: 30,
        heartbeat_seconds: 60,
    };
    assert!(also_bad.validate().is_err());
    assert!(AsyncJobsConfig::default().validate().is_ok());
}

#[test]
fn resolved_instance_id_default_and_explicit() {
    let empty = AsyncJobsConfig {
        instance_id: Some("  ".into()),
        ..AsyncJobsConfig::default()
    };
    assert!(empty.resolved_instance_id().starts_with("thelake-"));
    let named = AsyncJobsConfig {
        instance_id: Some("node-1".into()),
        ..AsyncJobsConfig::default()
    };
    assert_eq!(named.resolved_instance_id(), "node-1");
}

struct ScopeKeysFailJob;

#[async_trait]
impl Job for ScopeKeysFailJob {
    fn name(&self) -> &'static str {
        "scope_fail"
    }
    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Err(anyhow::anyhow!("injected scope_keys failure"))
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn runner_continues_when_scope_keys_fails() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let fail = Arc::new(ScopeKeysFailJob);
    let ok = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["t1".into()],
        interval: Duration::from_millis(10),
    });
    let runs = Arc::clone(&ok);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-scope-keys".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle =
        spawn_runner(&cfg, leases, vec![fail as Arc<dyn Job>, ok as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "sibling job must run after scope_keys Err"
    );
}

struct AlwaysFailJob {
    runs: AtomicUsize,
    interval: Duration,
}

#[async_trait]
impl Job for AlwaysFailJob {
    fn name(&self) -> &'static str {
        "always_fail"
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["fail-scope".into()])
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("injected failure"))
    }
}

#[tokio::test]
async fn runner_retries_after_failed_run() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let fail = Arc::new(AlwaysFailJob {
        runs: AtomicUsize::new(0),
        interval: Duration::from_millis(40),
    });
    let runs = Arc::clone(&fail);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-retry-fail".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![fail as Arc<dyn Job>]).expect("runner");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while runs.runs.load(Ordering::SeqCst) < 2 {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 2,
        "each wake retries after Err; got {}",
        runs.runs.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn release_on_ok_lets_peer_acquire() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["released".into()],
        // One pass then idle — peer acquire must not race a re-hold.
        interval: Duration::from_secs(3600),
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("holder-a".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![job as Arc<dyn Job>],
    )
    .expect("runner");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut peer_won = false;
    while tokio::time::Instant::now() < deadline {
        if runs.runs.load(Ordering::SeqCst) >= 1
            && leases
                .try_acquire("count", "released", "peer-b", Duration::from_secs(60))
                .await
                .unwrap()
        {
            peer_won = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "holder must complete one Ok run before peer acquire"
    );
    assert!(peer_won, "release after Ok must free the lease for peers");
}

#[tokio::test]
async fn release_on_err_lets_peer_acquire() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let fail = Arc::new(AlwaysFailJob {
        runs: AtomicUsize::new(0),
        // One pass then idle — peer acquire must not race a re-hold.
        interval: Duration::from_secs(3600),
    });
    let runs = Arc::clone(&fail);
    let cfg = AsyncJobsConfig {
        instance_id: Some("holder-fail".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![fail as Arc<dyn Job>],
    )
    .expect("runner");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut peer_won = false;
    while tokio::time::Instant::now() < deadline {
        if runs.runs.load(Ordering::SeqCst) >= 1
            && leases
                .try_acquire(
                    "always_fail",
                    "fail-scope",
                    "peer-b",
                    Duration::from_secs(60),
                )
                .await
                .unwrap()
        {
            peer_won = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "holder must complete one Err run before peer acquire"
    );
    assert!(peer_won, "release on Err must free the lease for peers");
}

#[tokio::test]
async fn two_runners_shared_store_no_dual_run() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job_a = Arc::new(HoldLeaseJob {
        runs: AtomicUsize::new(0),
        sleep_ms: 300,
    });
    let job_b = Arc::new(HoldLeaseJob {
        runs: AtomicUsize::new(0),
        sleep_ms: 300,
    });
    let runs_a = Arc::clone(&job_a);
    let runs_b = Arc::clone(&job_b);
    let cfg_a = AsyncJobsConfig {
        instance_id: Some("replica-a".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let cfg_b = AsyncJobsConfig {
        instance_id: Some("replica-b".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let h_a = spawn_runner(
        &cfg_a,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![job_a as Arc<dyn Job>],
    )
    .expect("a");
    let h_b = spawn_runner(
        &cfg_b,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![job_b as Arc<dyn Job>],
    )
    .expect("b");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let a = runs_a.runs.load(Ordering::SeqCst);
    let b = runs_b.runs.load(Ordering::SeqCst);
    h_a.abort();
    h_b.abort();
    assert_eq!(
        a + b,
        1,
        "exactly one in-flight holder under a shared lease; got a={a} b={b}"
    );
}

struct HoldLeaseJob {
    runs: AtomicUsize,
    sleep_ms: u64,
}

#[async_trait]
impl Job for HoldLeaseJob {
    fn name(&self) -> &'static str {
        "hold"
    }
    fn interval(&self) -> Duration {
        Duration::from_secs(3600)
    }
    async fn scope_keys(&self) -> anyhow::Result<Vec<String>> {
        Ok(vec!["hb-hold".into()])
    }
    async fn run(&self, _scope_key: &str) -> anyhow::Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        Ok(())
    }
}

#[tokio::test]
async fn heartbeat_extends_so_peer_cannot_steal_mid_run() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(HoldLeaseJob {
        runs: AtomicUsize::new(0),
        sleep_ms: 2500,
    });
    let cfg = AsyncJobsConfig {
        instance_id: Some("hb-holder".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 2,
    };
    let handle = spawn_runner(
        &cfg,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![job as Arc<dyn Job>],
    )
    .expect("runner");
    // Wait past original TTL but within HB-extended window.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        !leases
            .try_acquire("hold", "hb-hold", "thief", Duration::from_secs(60))
            .await
            .unwrap(),
        "heartbeat must extend lease past original TTL"
    );
    handle.abort();
}

#[tokio::test]
async fn panic_releases_lease_for_peer() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let panic_job = Arc::new(SlowPanicJob {
        ran: AtomicUsize::new(0),
        sleep_ms: 50,
    });
    let cfg = AsyncJobsConfig {
        instance_id: Some("panic-holder".into()),
        heartbeat_seconds: 1,
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(
        &cfg,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![panic_job as Arc<dyn Job>],
    )
    .expect("runner");
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.abort();
    assert!(
        leases
            .try_acquire(
                "panic_job",
                "t1",
                "peer-after-panic",
                Duration::from_secs(60)
            )
            .await
            .unwrap(),
        "panic must release so peer can acquire"
    );
}

#[tokio::test]
async fn memory_steal_records_without_panic() {
    // Smoke: steal path + metric hook must not panic when instruments unset.
    let store = MemoryLeaseStore::new();
    assert!(store
        .try_acquire("j", "steal-metric", "a", Duration::from_millis(40))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(store
        .try_acquire("j", "steal-metric", "b", Duration::from_secs(60))
        .await
        .unwrap());
    crate::self_monitoring::record_lease_acquire("j", "steal-metric", "win");
    crate::self_monitoring::record_lease_acquire("j", "steal-metric", "lose");
    crate::self_monitoring::record_job_error("j", "steal-metric");
}
