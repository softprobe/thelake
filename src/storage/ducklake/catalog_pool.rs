//! One attached DuckLake catalog and every DuckDB session that serves it.
//!
//! A [`CatalogPool`] owns three private lanes sized once from config: writer
//! sessions, query slots, and one maintenance session. Workspaces are filters
//! over the catalog, never owners of sessions. Query slots follow
//! [`SlotBinding`]: user SQL runs only after a slot serves the calling
//! workspace.

use super::{
    DuckLakeAccess, DuckLakeSessionFactory, DuckLakeSessionKind, DuckLakeWriter, PhysicalScope,
    WorkspaceScopeMode,
};
use crate::config::Config;
use crate::promotion::{BusinessTableManifest, TelemetryColumnsManifest};
use crate::storage::duckdb::cache::{
    cache_httpfs_disabled_by_env, wrap_one, CacheSettings, WrapAttempt,
};
use anyhow::{anyhow, bail, Result};
use duckdb::Connection;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

pub(crate) static CACHE_HTTPFS_CONFIG_WARNED: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(false));

/// Bounded wait. Pool creation runs on a blocking thread and workspace builds
/// hold a per-scope lock across it, so a slot wedged inside ATTACH (a Postgres
/// host that completes the TCP handshake and then goes silent) must surface as
/// a failure instead of hanging startup.
const SLOT_READY_TIMEOUT: Duration = Duration::from_secs(30);

fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Manifests replayed after the dev reset drops the lake tables.
pub(crate) struct DevReset {
    pub(crate) telemetry: Vec<TelemetryColumnsManifest>,
    pub(crate) business: Vec<BusinessTableManifest>,
}

pub(crate) struct CatalogPool {
    config: Config,
    access: DuckLakeAccess,
    mode: WorkspaceScopeMode,
    writers: WriterPool,
    queries: QueryLane,
    maintenance: MaintenanceLane,
}

impl CatalogPool {
    pub(crate) fn dev_reset_requested() -> bool {
        std::env::var("SPLAKE_RESET_DUCKLAKE").ok().as_deref() == Some("1")
    }

    /// Attach the catalog, run schema compatibility (and the dev reset when
    /// one is supplied) exactly once, then open the query and maintenance
    /// lanes.
    pub(crate) async fn open(
        config: &Config,
        scope: &PhysicalScope,
        reset: Option<DevReset>,
    ) -> Result<Arc<Self>> {
        let config = config.clone();
        let scope = scope.clone();
        tokio::task::spawn_blocking(move || Self::open_blocking(&config, scope, reset))
            .await
            .map_err(|error| anyhow!("catalog pool open join: {error}"))?
    }

    fn open_blocking(
        config: &Config,
        scope: PhysicalScope,
        reset: Option<DevReset>,
    ) -> Result<Arc<Self>> {
        let access = DuckLakeAccess::Physical(scope.clone());
        let mode = config.ducklake.workspace_scope_mode;
        let writers = WriterPool::open(config, &access)?;
        if let Some(reset) = &reset {
            DuckLakeWriter::reset_lake(&writers, &scope, reset)?;
        }
        DuckLakeWriter::ensure_shared_schema(&writers, &scope)?;
        let maintenance = MaintenanceLane::open(config, &access)?;
        let queries = QueryLane::open(config, &access, mode)?;
        info!(
            "DuckLake catalog pool opened (writer_slots={}, query_slots={})",
            writers.conns.len(),
            queries.workers.len()
        );
        Ok(Arc::new(Self {
            config: config.clone(),
            access,
            mode,
            writers,
            queries,
            maintenance,
        }))
    }

    pub(crate) fn scope(&self) -> &PhysicalScope {
        self.access.physical_scope()
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn writers(&self) -> &WriterPool {
        &self.writers
    }

    pub(crate) fn with_writer<R>(&self, f: impl FnOnce(&Connection) -> Result<R>) -> Result<R> {
        self.writers.with_conn(f)
    }

    /// Run `job` on a query slot that serves `workspace_id`.
    ///
    /// The slot is leased until the job finishes on its thread, even when the
    /// awaiting caller is cancelled.
    pub(crate) async fn with_query<R: Send + 'static>(
        &self,
        workspace_id: &str,
        job: impl FnOnce(&mut QuerySlot, &QueryCaller) -> R + Send + 'static,
    ) -> Result<R> {
        let caller = match self.mode {
            WorkspaceScopeMode::Shared => QueryCaller::Workspace(workspace_id.to_string()),
            WorkspaceScopeMode::Isolated => QueryCaller::Catalog,
        };
        self.queries.run(caller, job).await
    }

    /// Borrow the maintenance session.
    ///
    /// Returns `(result, cold_open, checkout_elapsed)`. On a catalog or
    /// connection failure the session is dropped so the next borrow reopens it.
    pub(crate) fn with_maintenance<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<(R, bool, Duration)> {
        self.maintenance.with_conn(&self.config, &self.access, f)
    }

    /// Opened DuckDB sessions across all lanes.
    pub(crate) fn session_count_for_test(&self) -> usize {
        self.writers.conns.len()
            + self.queries.shared.sessions.load(Ordering::Relaxed)
            + usize::from(self.maintenance.is_open())
    }
}

impl Drop for CatalogPool {
    fn drop(&mut self) {
        // Extension connections must not outlive their threads while the
        // process or test binary exits: close every channel, then join.
        self.queries.stop_workers(true);
    }
}

pub(super) struct TableReadinessRegistry {
    ready_tables: std::sync::RwLock<std::collections::HashSet<String>>,
}

impl TableReadinessRegistry {
    fn new() -> Self {
        Self {
            ready_tables: std::sync::RwLock::new(std::collections::HashSet::new()),
        }
    }

    fn is_ready(&self, table_name: &str) -> bool {
        self.ready_tables
            .read()
            .map(|ready| ready.contains(table_name))
            .unwrap_or(false)
    }

    fn mark_ready(&self, table_name: &str) {
        if let Ok(mut ready) = self.ready_tables.write() {
            ready.insert(table_name.to_string());
        }
    }

    fn clear(&self) {
        if let Ok(mut ready) = self.ready_tables.write() {
            ready.clear();
        }
    }
}

pub(super) struct WriterPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
    registry: TableReadinessRegistry,
    table_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl WriterPool {
    /// Attach sequentially: the first session initializes DuckLake metadata
    /// tables; later ones ATTACH the already-initialized Postgres schema.
    fn open(config: &Config, access: &DuckLakeAccess) -> Result<Self> {
        let factory = DuckLakeSessionFactory::new(config);
        let size = config.ducklake.effective_writer_pool_size();
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            let conn = factory.open(access, DuckLakeSessionKind::Writer)?;
            factory.attach(&conn, access)?;
            DuckLakeWriter::ensure_schema_for(&conn, access.physical_scope())?;
            conns.push(Mutex::new(conn));
        }
        Ok(Self {
            conns,
            next: AtomicUsize::new(0),
            registry: TableReadinessRegistry::new(),
            table_locks: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn with_conn<R>(&self, f: impl FnOnce(&Connection) -> Result<R>) -> Result<R> {
        let n = self.conns.len();
        if n == 0 {
            return Err(anyhow!("DuckLake writer pool is empty"));
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(guard) = self.conns[idx].try_lock() {
                return f(&guard);
            }
        }
        let guard = self.conns[start]
            .lock()
            .map_err(|_| anyhow!("DuckLake writer connection lock poisoned"))?;
        f(&guard)
    }

    pub(super) fn is_table_ready(&self, table_name: &str) -> bool {
        self.registry.is_ready(table_name)
    }

    pub(super) fn mark_table_ready(&self, table_name: &str) {
        self.registry.mark_ready(table_name);
    }

    pub(super) fn clear_ready(&self) {
        self.registry.clear();
    }

    pub(super) fn table_lock(&self, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock_ignoring_poison(&self.table_locks)
            .entry(table_name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

struct MaintenanceSession {
    conn: Option<Connection>,
    cold: bool,
}

struct MaintenanceLane {
    session: Mutex<MaintenanceSession>,
}

impl MaintenanceLane {
    fn open(config: &Config, access: &DuckLakeAccess) -> Result<Self> {
        Ok(Self {
            session: Mutex::new(MaintenanceSession {
                conn: Some(Self::open_attached(config, access)?),
                cold: true,
            }),
        })
    }

    fn open_attached(config: &Config, access: &DuckLakeAccess) -> Result<Connection> {
        let factory = DuckLakeSessionFactory::new(config);
        let conn = factory
            .open(access, DuckLakeSessionKind::Maintenance)
            .map_err(|e| anyhow!("maintenance DuckDB open: {e}"))?;
        factory
            .attach(&conn, access)
            .map_err(|e| anyhow!("maintenance DuckDB attach: {e}"))?;
        let registry_dsn = config
            .ducklake
            .metadata_path
            .strip_prefix("postgres:")
            .unwrap_or(&config.ducklake.metadata_path);
        let registry_target = crate::sql::literal::sql_string_literal(registry_dsn);
        conn.execute_batch(&format!(
            "ATTACH {registry_target} AS __thelake_registry (TYPE postgres);"
        ))
        .map_err(|e| anyhow!("maintenance registry attach: {e}"))?;
        Ok(conn)
    }

    fn is_open(&self) -> bool {
        lock_ignoring_poison(&self.session).conn.is_some()
    }

    fn with_conn<R>(
        &self,
        config: &Config,
        access: &DuckLakeAccess,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<(R, bool, Duration)> {
        let started = Instant::now();
        let mut session = self
            .session
            .lock()
            .map_err(|_| anyhow!("maintenance connection lock poisoned"))?;
        if session.conn.is_none() {
            session.conn = Some(Self::open_attached(config, access)?);
            session.cold = true;
        }
        let cold = std::mem::take(&mut session.cold);
        let checkout = started.elapsed();
        let conn = session.conn.as_ref().expect("opened above");
        match f(conn) {
            Ok(value) => Ok((value, cold, checkout)),
            Err(err) => {
                let msg = err.to_string().to_lowercase();
                if msg.contains("connection")
                    || msg.contains("catalog")
                    || msg.contains("detach")
                    || msg.contains("not open")
                {
                    session.conn = None;
                }
                Err(err)
            }
        }
    }
}

/// What a query slot is currently pointed at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SlotBinding {
    /// Fresh or rebuilt session: no workspace views installed.
    Unbound,
    /// The four logical views filter to this workspace.
    Shared(String),
    /// The catalog belongs to one workspace; bare names are qualified in SQL.
    IsolatedCatalog,
}

/// Who a query slot must serve before user SQL runs on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueryCaller {
    Workspace(String),
    Catalog,
}

impl SlotBinding {
    fn initial(mode: WorkspaceScopeMode) -> Self {
        match mode {
            WorkspaceScopeMode::Shared => Self::Unbound,
            WorkspaceScopeMode::Isolated => Self::IsolatedCatalog,
        }
    }

    fn serves(&self, caller: &QueryCaller) -> bool {
        match (self, caller) {
            (Self::Shared(bound), QueryCaller::Workspace(workspace)) => bound == workspace,
            (Self::IsolatedCatalog, QueryCaller::Catalog) => true,
            _ => false,
        }
    }
}

/// One query-lane DuckDB session plus its cache_httpfs wrap progress.
pub(crate) struct QuerySession {
    pub(crate) conn: Connection,
    cache_httpfs_wrap_supported: bool,
    cache_httpfs_wrapped_s3: bool,
    cache_httpfs_wrapped_httpfs: bool,
}

impl QuerySession {
    pub(crate) fn new(conn: Connection) -> Self {
        Self {
            conn,
            cache_httpfs_wrap_supported: true,
            cache_httpfs_wrapped_s3: false,
            cache_httpfs_wrapped_httpfs: false,
        }
    }

    /// Wrap lazily and best-effort: the s3/httpfs filesystems only register
    /// after the first real I/O.
    pub(crate) fn try_wrap_cache_httpfs_filesystems(&mut self, cache: &CacheSettings) {
        if cache.cache_dir.is_none() || cache_httpfs_disabled_by_env() {
            return;
        }
        if !self.cache_httpfs_wrap_supported {
            return;
        }

        if !self.cache_httpfs_wrapped_s3 {
            match wrap_one(&self.conn, "s3") {
                Ok(WrapAttempt::Wrapped) => {
                    self.cache_httpfs_wrapped_s3 = true;
                    info!("cache_httpfs wrapped filesystem: s3");
                }
                Ok(WrapAttempt::NotReady) => {}
                Ok(WrapAttempt::Unsupported) => {
                    self.cache_httpfs_wrap_supported = false;
                    warn!(
                        "cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused"
                    );
                }
                Err(err) => {
                    if !CACHE_HTTPFS_CONFIG_WARNED.swap(true, Ordering::Relaxed) {
                        warn!("Failed to wrap cache_httpfs filesystem s3: {err}");
                    }
                }
            }
        }

        if !self.cache_httpfs_wrapped_httpfs && self.cache_httpfs_wrap_supported {
            match wrap_one(&self.conn, "httpfs") {
                Ok(WrapAttempt::Wrapped) => {
                    self.cache_httpfs_wrapped_httpfs = true;
                    info!("cache_httpfs wrapped filesystem: httpfs");
                }
                Ok(WrapAttempt::NotReady) => {}
                Ok(WrapAttempt::Unsupported) => {
                    self.cache_httpfs_wrap_supported = false;
                    warn!(
                        "cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused"
                    );
                }
                Err(err) => {
                    if !CACHE_HTTPFS_CONFIG_WARNED.swap(true, Ordering::Relaxed) {
                        warn!("Failed to wrap cache_httpfs filesystem httpfs: {err}");
                    }
                }
            }
        }
    }
}

struct SlotCell {
    busy: bool,
    binding: SlotBinding,
}

struct QueryLaneShared {
    cells: Mutex<Vec<SlotCell>>,
    permits: Arc<Semaphore>,
    sessions: AtomicUsize,
}

impl QueryLaneShared {
    /// A permit guarantees a free cell. Prefer one already serving the caller,
    /// then an unbound one, so a rebind never evicts a bound slot needlessly.
    fn take_free(&self, caller: &QueryCaller) -> Option<usize> {
        let mut cells = lock_ignoring_poison(&self.cells);
        let index = cells
            .iter()
            .position(|cell| !cell.busy && cell.binding.serves(caller))
            .or_else(|| {
                cells
                    .iter()
                    .position(|cell| !cell.busy && cell.binding == SlotBinding::Unbound)
            })
            .or_else(|| cells.iter().position(|cell| !cell.busy))?;
        cells[index].busy = true;
        Some(index)
    }
}

/// Exclusive hold on one slot. Dropping it frees the cell, then returns the
/// permit.
struct QueryLease {
    shared: Arc<QueryLaneShared>,
    index: usize,
    _permit: OwnedSemaphorePermit,
}

impl Drop for QueryLease {
    fn drop(&mut self) {
        lock_ignoring_poison(&self.shared.cells)[self.index].busy = false;
    }
}

struct SlotEnv {
    config: Config,
    access: DuckLakeAccess,
    mode: WorkspaceScopeMode,
    cache: CacheSettings,
}

/// The thread-owned half of a query slot. The binding lives in the lane's
/// cell table so the scheduler and the thread never disagree.
pub(crate) struct QuerySlot {
    index: usize,
    env: Arc<SlotEnv>,
    lane: Arc<QueryLaneShared>,
    session: Option<QuerySession>,
}

impl QuerySlot {
    pub(crate) fn session(&mut self) -> Result<&mut QuerySession> {
        self.session
            .as_mut()
            .ok_or_else(|| anyhow!("DuckDB query slot has no open session"))
    }

    fn set_binding(&self, binding: SlotBinding) {
        lock_ignoring_poison(&self.lane.cells)[self.index].binding = binding;
    }

    fn serves(&self, caller: &QueryCaller) -> bool {
        lock_ignoring_poison(&self.lane.cells)[self.index]
            .binding
            .serves(caller)
    }

    fn install_session(&mut self, session: Option<QuerySession>) {
        match (self.session.is_some(), session.is_some()) {
            (false, true) => {
                self.lane.sessions.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.lane.sessions.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.session = session;
        self.set_binding(SlotBinding::initial(self.env.mode));
    }

    fn open_session(&self) -> Result<QuerySession> {
        let factory = DuckLakeSessionFactory::new(&self.env.config);
        let conn = factory.open(&self.env.access, DuckLakeSessionKind::Query)?;
        if let Err(err) = self.env.cache.wrap_filesystems(&conn) {
            if !CACHE_HTTPFS_CONFIG_WARNED.swap(true, Ordering::Relaxed) {
                warn!("Failed to wrap cache_httpfs filesystems: {}", err);
            }
        }
        factory.attach(&conn, &self.env.access)?;
        Ok(QuerySession::new(conn))
    }

    /// Drop the current session and install a fresh one in its initial
    /// binding. When the open fails the slot stays sessionless and the next
    /// checkout retries.
    fn replace_session(&mut self) -> Result<()> {
        self.install_session(None);
        let fresh = self.open_session()?;
        self.install_session(Some(fresh));
        Ok(())
    }

    /// Make this slot serve `caller`. No user SQL may run before this
    /// returns Ok. A failed view replace discards the connection so a
    /// half-rebound session is never reused.
    pub(crate) fn bind(&mut self, caller: &QueryCaller) -> Result<()> {
        if self.session.is_none() {
            self.replace_session()?;
        }
        if self.serves(caller) {
            return Ok(());
        }
        let QueryCaller::Workspace(workspace_id) = caller else {
            bail!("isolated catalog caller reached a shared query slot");
        };
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| anyhow!("DuckDB query slot has no open session"))?;
        let installed = super::workspace_views::install(
            &session.conn,
            self.env.access.physical_scope(),
            workspace_id,
        );
        match installed {
            Ok(()) => {
                self.set_binding(SlotBinding::Shared(workspace_id.clone()));
                Ok(())
            }
            Err(error) => {
                if let Err(reopen) = self.replace_session() {
                    warn!("DuckDB query slot could not replace its connection: {reopen}");
                }
                Err(error)
            }
        }
    }

    /// Rebuild a poisoned session and rebind it to `caller`, so a collateral
    /// retry sees the same workspace the failed attempt did.
    pub(crate) fn recover(&mut self, caller: &QueryCaller) -> Result<()> {
        self.replace_session()?;
        self.bind(caller)
    }
}

type SlotJob = Box<dyn FnOnce(&mut QuerySlot) + Send>;

struct SlotWorker {
    sender: Option<mpsc::UnboundedSender<SlotJob>>,
    join: Option<std::thread::JoinHandle<()>>,
}

struct QueryLane {
    shared: Arc<QueryLaneShared>,
    workers: Vec<SlotWorker>,
}

impl QueryLane {
    fn open(config: &Config, access: &DuckLakeAccess, mode: WorkspaceScopeMode) -> Result<Self> {
        let slot_count = std::cmp::max(1, config.query.max_connections);
        crate::self_monitoring::gauge_store::QUERY_WORKERS.store(slot_count, Ordering::Relaxed);
        let shared = Arc::new(QueryLaneShared {
            cells: Mutex::new(
                (0..slot_count)
                    .map(|_| SlotCell {
                        busy: false,
                        binding: SlotBinding::initial(mode),
                    })
                    .collect(),
            ),
            permits: Arc::new(Semaphore::new(slot_count)),
            sessions: AtomicUsize::new(0),
        });
        let env = Arc::new(SlotEnv {
            config: config.clone(),
            access: access.clone(),
            mode,
            cache: CacheSettings::new(config),
        });
        let mut lane = Self {
            shared,
            workers: Vec::with_capacity(slot_count),
        };

        // A slot that cannot open must not stay in the lane: round-robin onto
        // a dead slot used to fail a fixed fraction of requests as a bare 503.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<usize, String>>();
        for index in 0..slot_count {
            let (tx, rx) = mpsc::unbounded_channel::<SlotJob>();
            let slot = QuerySlot {
                index,
                env: Arc::clone(&env),
                lane: Arc::clone(&lane.shared),
                session: None,
            };
            let ready_tx = ready_tx.clone();
            let spawned = std::thread::Builder::new()
                .name("softprobe-duckdb-query-worker".to_string())
                .spawn(move || run_slot(slot, rx, ready_tx));
            match spawned {
                Ok(join) => lane.workers.push(SlotWorker {
                    sender: Some(tx),
                    join: Some(join),
                }),
                Err(err) => {
                    lane.stop_workers(true);
                    return Err(anyhow!("DuckDB worker spawn failed: {err}"));
                }
            }
        }
        drop(ready_tx);

        let mut failures = Vec::new();
        let mut wedged = false;
        for _ in 0..slot_count {
            match ready_rx.recv_timeout(SLOT_READY_TIMEOUT) {
                Ok(Ok(_)) => {}
                Ok(Err(msg)) => failures.push(msg),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    failures.push(format!(
                        "worker did not report readiness within {}s",
                        SLOT_READY_TIMEOUT.as_secs()
                    ));
                    wedged = true;
                    break;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    failures.push("worker exited before reporting readiness".to_string());
                    break;
                }
            }
        }
        if failures.is_empty() {
            return Ok(lane);
        }
        warn!(
            "DuckDB query lane: {}/{} slots failed to start",
            failures.len(),
            slot_count
        );
        // Joining a slot wedged inside ATTACH would block forever, trading a
        // thread leak for a hang. Leak deliberately; the timeout above is
        // what makes the failure visible.
        lane.stop_workers(!wedged);
        if wedged {
            warn!(
                "leaving {} DuckDB worker thread(s) detached: at least one is wedged in startup",
                lane.workers.len()
            );
        }
        Err(anyhow!(
            "DuckDB query lane failed to start {} of {} slots: {}",
            failures.len(),
            slot_count,
            failures.join("; ")
        ))
    }

    fn stop_workers(&mut self, join: bool) {
        for worker in &mut self.workers {
            worker.sender.take();
        }
        for worker in &mut self.workers {
            let Some(handle) = worker.join.take() else {
                continue;
            };
            if join {
                if let Err(err) = handle.join() {
                    warn!("DuckDB query worker panicked during shutdown: {:?}", err);
                }
            }
        }
    }

    async fn checkout(&self, caller: &QueryCaller) -> Result<QueryLease> {
        let permit = Arc::clone(&self.shared.permits)
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("DuckDB query lane closed"))?;
        let index = self
            .shared
            .take_free(caller)
            .ok_or_else(|| anyhow!("DuckDB query lane has no free slot despite a permit"))?;
        Ok(QueryLease {
            shared: Arc::clone(&self.shared),
            index,
            _permit: permit,
        })
    }

    async fn run<R: Send + 'static>(
        &self,
        caller: QueryCaller,
        job: impl FnOnce(&mut QuerySlot, &QueryCaller) -> R + Send + 'static,
    ) -> Result<R> {
        let lease = self.checkout(&caller).await?;
        let sender = self.workers[lease.index]
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("DuckDB worker channel closed"))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Box::new(move |slot| {
                let _lease = lease;
                let outcome = slot.bind(&caller).map(|()| job(slot, &caller));
                let _ = tx.send(outcome);
            }))
            .map_err(|_| anyhow!("DuckDB worker channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("DuckDB worker dropped response"))?
    }
}

fn run_slot(
    mut slot: QuerySlot,
    mut jobs: mpsc::UnboundedReceiver<SlotJob>,
    ready: std::sync::mpsc::Sender<Result<usize, String>>,
) {
    let index = slot.index;
    if let Err(err) = slot.replace_session() {
        let _ = ready.send(Err(format!("worker {index} open: {err}")));
        return;
    }
    if ready.send(Ok(index)).is_err() {
        return;
    }
    drop(ready);
    while let Some(job) = jobs.blocking_recv() {
        job(&mut slot);
    }
}
