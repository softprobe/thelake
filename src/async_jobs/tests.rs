use crate::async_jobs::{spawn_runner, Job, LeaseStore, MemoryLeaseStore, PostgresLeaseStore};
use crate::config::AsyncJobsConfig;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Shared lease semantics — run against Memory and Postgres.
async fn lease_contract(store: &dyn LeaseStore, prefix: &str) {
    let job = format!("{prefix}-j");
    let scope = format!("{prefix}-s");

    assert!(
        store
            .try_acquire(&job, &scope, "a", Duration::from_secs(60))
            .await
            .unwrap(),
        "empty acquire wins"
    );
    assert!(
        !store
            .try_acquire(&job, &scope, "b", Duration::from_secs(60))
            .await
            .unwrap(),
        "second holder loses while valid"
    );

    // Same holder renew must extend TTL (survive past original short lease).
    let renew_scope = format!("{prefix}-renew");
    assert!(store
        .try_acquire(&job, &renew_scope, "a", Duration::from_secs(1))
        .await
        .unwrap());
    assert!(store
        .try_acquire(&job, &renew_scope, "a", Duration::from_secs(60))
        .await
        .unwrap());
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
    assert!(store
        .try_acquire(&job, &hb_scope, "a", Duration::from_secs(1))
        .await
        .unwrap());
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        store
            .heartbeat(&job, &hb_scope, "a", Duration::from_secs(1))
            .await
            .unwrap();
    }
    assert!(!store
        .try_acquire(&job, &hb_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());
    assert!(store
        .heartbeat(&job, &hb_scope, "b", Duration::from_secs(60))
        .await
        .is_err());

    // Release then other acquires; non-holder release is a no-op.
    let rel_scope = format!("{prefix}-rel");
    assert!(store
        .try_acquire(&job, &rel_scope, "a", Duration::from_secs(60))
        .await
        .unwrap());
    store.release(&job, &rel_scope, "b").await.unwrap();
    assert!(!store
        .try_acquire(&job, &rel_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());
    store.release(&job, &rel_scope, "a").await.unwrap();
    assert!(store
        .try_acquire(&job, &rel_scope, "b", Duration::from_secs(60))
        .await
        .unwrap());

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
        .heartbeat(
            &job,
            &format!("{prefix}-missing"),
            "a",
            Duration::from_secs(60)
        )
        .await
        .is_err());
    let hb_gone = format!("{prefix}-hb-gone");
    assert!(store
        .try_acquire(&job, &hb_gone, "a", Duration::from_secs(60))
        .await
        .unwrap());
    store.release(&job, &hb_gone, "a").await.unwrap();
    assert!(store
        .heartbeat(&job, &hb_gone, "a", Duration::from_secs(60))
        .await
        .is_err());
    let hb_stolen = format!("{prefix}-hb-stolen");
    assert!(store
        .try_acquire(&job, &hb_stolen, "a", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(store
        .try_acquire(&job, &hb_stolen, "b", Duration::from_secs(60))
        .await
        .unwrap());
    assert!(store
        .heartbeat(&job, &hb_stolen, "a", Duration::from_secs(60))
        .await
        .is_err());

    // Former holder release after steal must not drop the new holder's lease.
    let post_loss = format!("{prefix}-post-loss");
    assert!(store
        .try_acquire(&job, &post_loss, "a", Duration::from_secs(1))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(store
        .try_acquire(&job, &post_loss, "b", Duration::from_secs(60))
        .await
        .unwrap());
    store.release(&job, &post_loss, "a").await.unwrap();
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
}

#[async_trait]
impl Job for CountingJob {
    fn name(&self) -> &'static str {
        "count"
    }
    fn interval(&self) -> Duration {
        Duration::from_millis(10)
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
async fn runner_runs_when_lease_won() {
    let leases = Arc::new(MemoryLeaseStore::new());
    let job = Arc::new(CountingJob {
        runs: AtomicUsize::new(0),
        scopes: vec!["t1".into()],
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

/// Heartbeat errors are logged/counted but must not abort `job.run`.
struct HeartbeatFailStore {
    inner: MemoryLeaseStore,
    heartbeat_calls: AtomicUsize,
}

#[async_trait]
impl LeaseStore for HeartbeatFailStore {
    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> anyhow::Result<bool> {
        self.inner
            .try_acquire(job_name, scope_key, holder_id, ttl)
            .await
    }

    async fn heartbeat(
        &self,
        _job_name: &str,
        _scope_key: &str,
        _holder_id: &str,
        _ttl: Duration,
    ) -> anyhow::Result<()> {
        self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("injected heartbeat failure"))
    }

    async fn release(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
    ) -> anyhow::Result<()> {
        self.inner.release(job_name, scope_key, holder_id).await
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
async fn runner_completes_run_despite_heartbeat_failures() {
    let leases = Arc::new(HeartbeatFailStore {
        inner: MemoryLeaseStore::new(),
        heartbeat_calls: AtomicUsize::new(0),
    });
    let hb = Arc::clone(&leases);
    let job = Arc::new(SlowJob {
        runs: AtomicUsize::new(0),
        sleep_ms: 80,
    });
    let runs = Arc::clone(&job);
    let cfg = AsyncJobsConfig {
        instance_id: Some("runner-hb-fail".into()),
        heartbeat_seconds: 1, // first interval tick is immediate → fails during run
        lease_ttl_seconds: 60,
    };
    let handle = spawn_runner(&cfg, leases, vec![job as Arc<dyn Job>]).expect("runner");
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.abort();
    assert!(
        runs.runs.load(Ordering::SeqCst) >= 1,
        "job must finish even when heartbeat fails"
    );
    assert!(
        hb.heartbeat_calls.load(Ordering::SeqCst) >= 1,
        "heartbeat must have been attempted"
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
    let q = crate::runtime_engine::quote_pg_ident(schema);
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
  lease_until TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_name, scope_key)
)"#
            ),
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
