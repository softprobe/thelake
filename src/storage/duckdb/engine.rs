use crate::config::Config;
use crate::query::workspace_views;
use crate::storage::duckdb::cache::CacheSettings;
use crate::storage::ducklake::{
    ducklake_qualified_table_name, DuckLakeSessionFactory, DuckLakeSessionKind,
};
use crate::workspace_scope::DuckLakeAccess;
use anyhow::{anyhow, Result};
use base64::Engine;
use duckdb::types::Value as DuckValue;
use duckdb::Connection;
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Query result containing columns and rows
#[derive(Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
}

type InflightWaiters = Vec<oneshot::Sender<Result<QueryResult>>>;
type InflightMap = HashMap<u64, InflightWaiters>;

pub struct DuckDBQueryEngine {
    _shared_connection: Arc<Mutex<Connection>>,
    workers: Vec<WorkerHandle>,
    next_worker: AtomicUsize,
    config: Config,
    access: DuckLakeAccess,
    /// In-flight identical SQL shares one worker (Grafana panel stampede), not a result TTL.
    inflight: Arc<Mutex<InflightMap>>,
    tenant_id: String,
    counts_toward_liveness: bool,
}

fn is_sql_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// True when `pos` lies inside a SQL string/identifier literal or comment.
///
/// Recognizes single-quoted strings, double-quoted identifiers, `--` line comments,
/// and `/* */` block comments so qualify does not treat apostrophes/`--` inside them
/// as string/comment toggles.
fn inside_sql_string_or_comment(s: &str, pos: usize) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < pos && i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if in_single {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b'\'' => {
                in_single = true;
                i += 1;
            }
            b'"' => {
                in_double = true;
                i += 1;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                in_line_comment = true;
                i += 2;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                in_block_comment = true;
                i += 2;
            }
            _ => i += 1,
        }
    }
    in_single || in_double || in_line_comment || in_block_comment
}

fn replace_standalone_ident(s: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(s.len().saturating_add(32));
    let mut last = 0;
    for (i, _) in s.match_indices(from) {
        if inside_sql_string_or_comment(s, i) {
            continue;
        }
        // `.` before the match means a qualified suffix (`catalog.traces`) — do not rewrite.
        // `.` after the match means `traces.col` — still rewrite the table segment.
        let before_ok = match s[..i].chars().next_back() {
            None => true,
            Some(c) => !is_sql_ident_char(c) && c != '.',
        };
        let end = i + from.len();
        let after_ok = s[end..]
            .chars()
            .next()
            .map(|c| !is_sql_ident_char(c))
            .unwrap_or(true);
        if before_ok && after_ok {
            out.push_str(&s[last..i]);
            out.push_str(to);
            last = end;
        }
    }
    out.push_str(&s[last..]);
    out
}

/// Expand bare public telemetry names to catalog-qualified DuckLake tables.
///
/// Tenant-scoped catalogs expose tables under `catalog.schema.table`; bare
/// `FROM traces` would miss the attachment and return empty (masked as 0 rows).
/// Historical Iceberg/buffer aliases are intentionally not rewritten.
fn qualify_public_telemetry_tables(
    sql: &str,
    traces: &str,
    logs: &str,
    scores: &str,
    score_configs: &str,
) -> String {
    let mut s = sql.to_string();
    s = replace_standalone_ident(&s, "score_configs", score_configs);
    s = replace_standalone_ident(&s, "scores", scores);
    s = replace_standalone_ident(&s, "traces", traces);
    s = replace_standalone_ident(&s, "logs", logs);
    s
}

/// True when SQL mutates a DuckLake catalog and must be wrapped in BEGIN…COMMIT.
///
/// Without an explicit COMMIT, INSERT…SELECT can write parquet under DATA_PATH while
/// leaving the catalog snapshot unchanged — Prom workers then see empty 5m/1h/collapse.
fn sql_is_ducklake_mutating(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let head: String = trimmed
        .chars()
        .take(12)
        .collect::<String>()
        .to_ascii_uppercase();
    head.starts_with("INSERT")
        || head.starts_with("UPDATE")
        || head.starts_with("DELETE")
        || head.starts_with("CREATE")
        || head.starts_with("ALTER")
        || head.starts_with("DROP")
        || head.starts_with("COPY")
        || head.starts_with("CALL")
        || head.starts_with("MERGE")
}

/// INSERT/UPDATE/DELETE need Softprobe BEGIN…COMMIT so Prom workers see
/// catalog snapshots. DuckLake `CALL` procedures (expire/merge/cleanup) manage
/// their own transactions — wrapping them can no-op metadata changes (AC-N3).
fn sql_needs_softprobe_txn_wrap(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let head: String = trimmed
        .chars()
        .take(12)
        .collect::<String>()
        .to_ascii_uppercase();
    head.starts_with("INSERT")
        || head.starts_with("UPDATE")
        || head.starts_with("DELETE")
        || head.starts_with("CREATE")
        || head.starts_with("ALTER")
        || head.starts_with("DROP")
        || head.starts_with("COPY")
        || head.starts_with("MERGE")
}

use once_cell::sync::Lazy;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;

static CACHE_HTTPFS_CONFIG_WARNED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

/// Self-heal bookkeeping for poisoned worker connections. Process-global on
/// purpose: /health must answer "is self-heal failing" across every tenant
/// engine without holding a reference to each.
///
/// Because it is global, `consecutive_failures` is cleared by ANY successful
/// query, not just by a successful rebuild. Otherwise a single tenant whose
/// DuckLake scope is permanently unattachable (schema dropped, bucket deleted,
/// per-scope credentials rotated) would drive the counter past the /health
/// threshold and crashloop a pod that is serving every other tenant fine --
/// and a restart cannot fix a broken tenant scope, so it would never stop.
/// Reaching the threshold now requires that nothing anywhere is succeeding,
/// which is the process-level state a restart can actually resolve.
#[derive(Default)]
struct SelfHealCounters {
    rebuilds: AtomicU64,
    consecutive_failures: AtomicU64,
}

static SELF_HEAL: Lazy<SelfHealCounters> = Lazy::new(SelfHealCounters::default);

#[derive(Debug, Clone)]
pub struct SelfHealSnapshot {
    pub rebuilds: u64,
    pub consecutive_failures: u64,
}

pub fn self_heal_snapshot() -> SelfHealSnapshot {
    SelfHealSnapshot {
        rebuilds: SELF_HEAL.rebuilds.load(Ordering::Relaxed),
        consecutive_failures: SELF_HEAL.consecutive_failures.load(Ordering::Relaxed),
    }
}

/// Test-only: the counters are global, so the /health unhealthy branch is
/// otherwise unreachable from tests.
pub fn set_self_heal_failures_for_test(value: u64) {
    SELF_HEAL
        .consecutive_failures
        .store(value, Ordering::Relaxed);
}

/// How a query error relates to DuckDB's "database has been invalidated" state.
///
/// After an internal assertion failure (e.g. the ducklake extension's inlined
/// data reader crashing with "Attempted to access index 0 within vector of
/// size 0"), DuckDB invalidates the whole database object. Each worker owns an
/// independent in-memory database, so one bad query kills that worker's
/// connection permanently -- and round-robin dispatch keeps feeding requests
/// to the corpse. Under concurrent load every worker gets poisoned within
/// seconds and each query returns 503 until a human restarts the process.
/// That is the 2026-08-03 production outage; this classification is what lets
/// workers rebuild instead of staying dead.
///
/// Classification is anchored to the leading marker of the FIRST line, for the
/// same reason [`crate::api::llm::query::classify_storage_error`] is: DuckDB
/// echoes the offending statement (`LINE 1: ...`) into the message, and that
/// statement embeds caller-supplied literals. A `contains` check here would let
/// a filter value like `model_name = "database has been invalidated"` force two
/// full connection rebuilds -- each one a fresh in-memory database plus a
/// DuckLake ATTACH round-trip to Postgres, synchronously on the worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Poison {
    /// Not a poison-related failure.
    None,
    /// This query tripped the fatal error itself. Deterministic -- retrying
    /// would just poison the fresh connection too, so only rebuild.
    Triggered,
    /// This query landed on a connection an earlier query had poisoned.
    /// Innocent -- retry it once on the rebuilt connection.
    Collateral,
}

fn poison_kind(message: &str) -> Poison {
    // Only the first line: everything after it may contain echoed SQL.
    let head = message.trim_start().lines().next().unwrap_or("");
    // Order matters. The collateral message quotes the original internal error
    // in a trailing `Original error: "..."`, but that lives on a later line, so
    // first-line anchoring already separates the two. Checked first anyway to
    // keep the intent explicit.
    if head.starts_with("FATAL Error") {
        return Poison::Collateral;
    }
    if head.starts_with("INTERNAL Error") {
        return Poison::Triggered;
    }
    // Stale ATTACH after inlined catalog table rename (e.g. optional external
    // flush). Rebuild + retry picks up the new name — required now that default
    // inlining is 500 (was 10_000 under #55).
    if head.starts_with("Catalog Error: Failed to read inlined data from DuckLake") {
        return Poison::Collateral;
    }
    Poison::None
}

fn rebuild_worker_state(core: &DuckDBCore, index: usize) -> Option<ConnectionState> {
    match core
        .open_connection()
        .and_then(|conn| core.init_connection_state_with(conn))
    {
        Ok(state) => {
            if core.counts_toward_liveness {
                SELF_HEAL.rebuilds.fetch_add(1, Ordering::Relaxed);
                SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
            }
            info!("DuckDB query worker {index} rebuilt its connection after a fatal engine error");
            Some(state)
        }
        Err(err) => {
            // Counted so /health can turn "self-heal keeps failing" into an
            // unhealthy signal; nothing recovers from this state on its own.
            // Ops engines set counts_toward_liveness=false so a broken ops
            // catalog cannot crashloop the customer plane.
            if core.counts_toward_liveness {
                SELF_HEAL
                    .consecutive_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            warn!("DuckDB query worker {index} failed to rebuild its poisoned connection: {err}");
            None
        }
    }
}

struct WorkerHandle {
    sender: Option<mpsc::Sender<QueryRequest>>,
    join: Option<std::thread::JoinHandle<()>>,
}

struct QueryRequest {
    sql: String,
    enqueued_at: std::time::Instant,
    respond_to: oneshot::Sender<QueryWorkerResponse>,
}

struct QueryWorkerResponse {
    result: Result<QueryResult>,
    queue_wait: std::time::Duration,
    exec_elapsed: std::time::Duration,
}

struct ConnectionState {
    conn: Connection,
    cache_httpfs_wrap_supported: bool,
    cache_httpfs_wrapped_s3: bool,
    cache_httpfs_wrapped_httpfs: bool,
}

#[derive(Clone)]
struct DuckDBCore {
    config: Config,
    access: DuckLakeAccess,
    cache: CacheSettings,
    /// When false, rebuild failures do not increment process-global SelfHeal
    /// counters used by `/health` liveness (ops engines).
    counts_toward_liveness: bool,
    tenant_id: String,
}

fn sql_coalesce_key(sql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

/// Drops the in-flight key if the leader is cancelled (compat backends wrap
/// `execute_query` in `tokio::time::timeout`). Without this, waiters hang until restart.
struct InflightLease {
    inflight: Arc<Mutex<InflightMap>>,
    key: u64,
    armed: bool,
}

impl InflightLease {
    fn lock_map(inflight: &Mutex<InflightMap>) -> std::sync::MutexGuard<'_, InflightMap> {
        inflight.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn take_waiters(&mut self) -> InflightWaiters {
        self.armed = false;
        Self::lock_map(&self.inflight)
            .remove(&self.key)
            .unwrap_or_default()
    }

    fn complete(mut self, result: &Result<QueryResult>) {
        let waiters = self.take_waiters();
        match result {
            Ok(rows) => {
                for waiter in waiters {
                    let _ = waiter.send(Ok(rows.clone()));
                }
            }
            Err(err) => {
                let msg = err.to_string();
                for waiter in waiters {
                    let _ = waiter.send(Err(anyhow!(msg.clone())));
                }
            }
        }
    }
}

impl Drop for InflightLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let waiters = self.take_waiters();
        for waiter in waiters {
            let _ = waiter.send(Err(anyhow!("DuckDB in-flight leader cancelled")));
        }
    }
}

impl DuckDBQueryEngine {
    /// `counts_toward_liveness=false` for ops/self-monitoring engines so rebuild
    /// failures never trip process `/health` liveness.
    pub(crate) async fn new_with_liveness(
        config: &Config,
        access: DuckLakeAccess,
        counts_toward_liveness: bool,
        tenant_id: &str,
    ) -> Result<Self> {
        let core = DuckDBCore {
            config: config.clone(),
            access: access.clone(),
            cache: CacheSettings::new(config),
            counts_toward_liveness,
            tenant_id: tenant_id.to_string(),
        };
        crate::self_monitoring::gauge_store::QUERY_WORKERS.store(
            std::cmp::max(1, config.query.max_connections),
            Ordering::Relaxed,
        );
        let worker_count = std::cmp::max(1, config.query.max_connections);
        let mut workers = Vec::with_capacity(worker_count);
        // Workers report startup outcome so a failed one cannot stay in the pool.
        // Previously a worker that could not open its connection just logged a
        // warning and returned; its channel closed, but round-robin dispatch kept
        // handing queries to that dead slot. Every request landing there failed
        // with "worker channel closed", which the API layer flattened into a bare
        // 503 -- a fixed fraction of requests failing while looking like a random
        // outage.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<usize, String>>();
        for index in 0..worker_count {
            let (tx, mut rx) = mpsc::channel::<QueryRequest>(32);
            let core = core.clone();
            let ready_tx = ready_tx.clone();
            let join = std::thread::Builder::new()
                .name("softprobe-duckdb-query-worker".to_string())
                .spawn(move || {
                    // Each worker gets its own independent connection (not cloned)
                    let connection = match core.open_connection() {
                        Ok(conn) => conn,
                        Err(err) => {
                            let _ = ready_tx
                                .send(Err(format!("worker {index} open_connection: {err}")));
                            return;
                        }
                    };
                    let mut state = match core.init_connection_state_with(connection) {
                        Ok(state) => state,
                        Err(err) => {
                            let _ = ready_tx
                                .send(Err(format!("worker {index} init_connection: {err}")));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(index)).is_err() {
                        return; // engine construction already aborted
                    }
                    drop(ready_tx);
                    while let Some(request) = rx.blocking_recv() {
                        let queue_wait = request.enqueued_at.elapsed();
                        let sql_kind = crate::self_monitoring::classify_sql_kind(&request.sql);
                        if core.counts_toward_liveness {
                            crate::self_monitoring::record_query_queue_wait(
                                &core.tenant_id,
                                sql_kind,
                                queue_wait,
                            );
                        }
                        crate::self_monitoring::gauge_store::QUERY_WORKERS_BUSY
                            .fetch_add(1, Ordering::Relaxed);
                        let exec_start = std::time::Instant::now();
                        let mut result = core.execute_query_on_state(&mut state, &request.sql);
                        if let Err(err) = &result {
                            let kind = poison_kind(&err.to_string());
                            if kind != Poison::None {
                                // This worker's in-memory database is dead; every
                                // future statement would fail with the same FATAL
                                // error. Rebuild before touching the next request.
                                if let Some(fresh) = rebuild_worker_state(&core, index) {
                                    state = fresh;
                                    if kind == Poison::Collateral {
                                        result =
                                            core.execute_query_on_state(&mut state, &request.sql);
                                        if let Err(retry_err) = &result {
                                            if poison_kind(&retry_err.to_string()) != Poison::None {
                                                // The retry poisoned the fresh connection
                                                // too; rebuild again so the next request
                                                // does not inherit a dead one.
                                                if let Some(fresh) =
                                                    rebuild_worker_state(&core, index)
                                                {
                                                    state = fresh;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        let exec_elapsed = exec_start.elapsed();
                        crate::self_monitoring::gauge_store::QUERY_WORKERS_BUSY
                            .fetch_sub(1, Ordering::Relaxed);
                        if core.counts_toward_liveness {
                            crate::self_monitoring::record_query(
                                &core.tenant_id,
                                sql_kind,
                                exec_elapsed,
                            );
                        }
                        if result.is_ok() {
                            // Any *customer* success clears the global streak --
                            // see SelfHealCounters. Ops engines must not clear
                            // (or they can hide a dead customer plane behind
                            // healthy Grafana traffic).
                            if core.counts_toward_liveness {
                                SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
                            }
                        }
                        let _ = request.respond_to.send(QueryWorkerResponse {
                            result,
                            queue_wait,
                            exec_elapsed,
                        });
                    }
                })
                .map_err(|err| anyhow!("DuckDB worker spawn failed: {}", err))?;
            workers.push(WorkerHandle {
                sender: Some(tx),
                join: Some(join),
            });
        }
        drop(ready_tx);

        // Fail fast: one unusable worker means a fixed slice of every future
        // request would fail, which is far harder to diagnose than not starting.
        //
        // Bounded wait. This is a blocking recv on a Tokio worker thread, and
        // `engine_for` holds the per-tenant build mutex across it, so an
        // unbounded wait would park a runtime thread and the tenant lock
        // forever whenever a worker wedges inside ATTACH -- a Postgres host
        // that completes the TCP handshake and then goes silent (firewall
        // blackhole, saturated pooler) does exactly that. On the startup path
        // that hangs before `axum::serve` binds: no /health, never ready, and
        // nothing in the logs. A timeout turns that into a clear failure.
        const WORKER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let mut failures = Vec::new();
        let mut wedged = false;
        for _ in 0..worker_count {
            match ready_rx.recv_timeout(WORKER_READY_TIMEOUT) {
                Ok(Ok(_)) => {}
                Ok(Err(msg)) => failures.push(msg),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    failures.push(format!(
                        "worker did not report readiness within {}s",
                        WORKER_READY_TIMEOUT.as_secs()
                    ));
                    // Remaining workers cannot be waited on either: a wedged
                    // worker holds the channel open, so every further recv
                    // would burn another full timeout.
                    wedged = true;
                    break;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    failures.push("worker exited before reporting readiness".to_string());
                    break;
                }
            }
        }
        if !failures.is_empty() {
            warn!(
                "DuckDB query engine: {}/{} workers failed to start",
                failures.len(),
                worker_count
            );
            // `Drop for DuckDBQueryEngine` is the only place workers are
            // joined, and it cannot run here because `Self` was never
            // constructed -- so simply dropping `workers` detaches threads
            // holding live DuckDB connections, which is what that Drop impl
            // exists to prevent. Since `engine_for` does not cache failures,
            // every client retry would leak another full pool.
            //
            // Only safe to join when no worker is wedged: a worker stuck
            // inside ATTACH never reaches `blocking_recv`, so closing its
            // channel does not release it and `join` would block forever --
            // trading a leak for a hang. In that case leak deliberately and
            // say so; the timeout above is what makes the failure visible.
            for worker in &mut workers {
                worker.sender.take();
            }
            if wedged {
                warn!(
                    "leaving {} DuckDB worker thread(s) detached: at least one is wedged in \
                     startup and would never observe a closed channel",
                    workers.len()
                );
            } else {
                for worker in &mut workers {
                    if let Some(join) = worker.join.take() {
                        if let Err(err) = join.join() {
                            warn!(
                                "DuckDB query worker panicked during startup abort: {:?}",
                                err
                            );
                        }
                    }
                }
            }
            return Err(anyhow!(
                "DuckDB query engine failed to start {} of {} workers: {}",
                failures.len(),
                worker_count,
                failures.join("; ")
            ));
        }

        // Keep a dummy connection for the _shared_connection field (for compatibility)
        let dummy_conn = core.open_connection()?;

        Ok(Self {
            _shared_connection: Arc::new(Mutex::new(dummy_conn)),
            workers,
            next_worker: AtomicUsize::new(0),
            config: config.clone(),
            access,
            inflight: Arc::new(Mutex::new(HashMap::new())),
            tenant_id: core.tenant_id.clone(),
            counts_toward_liveness: core.counts_toward_liveness,
        })
    }

    /// Catalog alias for `__ducklake_metadata_<alias>` / `{alias}.promotion_specs`.
    pub(crate) fn catalog_alias(&self) -> &str {
        self.access.physical_scope().attach_alias()
    }

    pub(crate) fn workspace_scope_mode(&self) -> crate::workspace_scope::WorkspaceScopeMode {
        self.config.ducklake.workspace_scope_mode
    }

    pub(crate) async fn execute_trusted(
        &self,
        query: crate::sql::trusted::TrustedSql,
    ) -> Result<QueryResult> {
        let scope = self.access.physical_scope();
        query
            .validate_for_scope(scope)
            .map_err(|error| anyhow!(error))?;
        self.execute_query(query.as_str()).await
    }

    /// Execute arbitrary SQL query and return results as JSON
    /// Used by Grafana SQL API endpoint
    pub(crate) async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let key = sql_coalesce_key(query);
        // Guard must drop before any `.await` — `std::sync::MutexGuard` is `!Send`.
        let waiter = {
            let mut pending = InflightLease::lock_map(&self.inflight);
            if let Some(waiters) = pending.get_mut(&key) {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                Some(rx)
            } else {
                pending.insert(key, Vec::new());
                None
            }
        };
        if let Some(rx) = waiter {
            return rx
                .await
                .map_err(|_| anyhow!("DuckDB in-flight coalesced waiter dropped"))?;
        }
        let lease = InflightLease {
            inflight: Arc::clone(&self.inflight),
            key,
            armed: true,
        };
        let result = self.dispatch_query(query).await;
        lease.complete(&result);
        result
    }

    async fn dispatch_query(&self, query: &str) -> Result<QueryResult> {
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker = &self.workers[index % self.workers.len()];
        let sender = worker
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("DuckDB worker channel closed"))?;
        let (tx, rx) = oneshot::channel();
        let request = QueryRequest {
            sql: query.to_string(),
            enqueued_at: std::time::Instant::now(),
            respond_to: tx,
        };
        sender
            .send(request)
            .await
            .map_err(|_| anyhow!("DuckDB worker channel closed"))?;
        let response = rx
            .await
            .map_err(|_| anyhow!("DuckDB worker dropped response"))?;
        let elapsed = response.queue_wait + response.exec_elapsed;
        if elapsed >= std::time::Duration::from_millis(200) {
            let preview: String = query.chars().take(160).collect();
            let sql_kind = crate::self_monitoring::classify_sql_kind(query);
            warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                queue_wait_ms = response.queue_wait.as_millis() as u64,
                sql = %preview,
                "slow DuckDB query (queue + execute)"
            );
            if self.counts_toward_liveness {
                crate::self_monitoring::record_slow_query(&self.tenant_id, sql_kind);
            }
        }
        response.result
    }

    /// One-shot metadata SQL on a dedicated connection (no worker pool, no
    /// self-monitoring instruments). Used by inventory scrapes.
    pub(crate) async fn execute_query_uninstrumented(&self, query: &str) -> Result<QueryResult> {
        let mut rows = self.execute_queries_uninstrumented(vec![query]).await?;
        rows.pop()
            .ok_or_else(|| anyhow!("inventory query returned no result"))?
    }

    /// Run several metadata SQLs on one open+attach connection (inventory).
    pub(crate) async fn execute_queries_uninstrumented(
        &self,
        queries: Vec<&str>,
    ) -> Result<Vec<Result<QueryResult>>> {
        let core = DuckDBCore {
            config: self.config.clone(),
            access: self.access.clone(),
            cache: CacheSettings::new(&self.config),
            counts_toward_liveness: false,
            tenant_id: self.tenant_id.clone(),
        };
        let sqls: Vec<String> = queries.iter().map(|s| (*s).to_string()).collect();
        tokio::task::spawn_blocking(move || {
            let conn = core.open_connection()?;
            let mut state = core.init_connection_state_with(conn)?;
            let mut out = Vec::with_capacity(sqls.len());
            for sql in sqls {
                out.push(core.execute_query_on_state(&mut state, &sql));
            }
            Ok(out)
        })
        .await
        .map_err(|e| anyhow!("inventory query join: {e}"))?
    }
}

impl Drop for DuckDBQueryEngine {
    fn drop(&mut self) {
        // DuckDB/extension connections are not safe to leave on detached threads while the process
        // or test binary is exiting. Close every worker channel first so all workers can break out
        // of `blocking_recv`, then join them before this engine releases its final shared state.
        for worker in &mut self.workers {
            worker.sender.take();
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                if let Err(err) = join.join() {
                    warn!("DuckDB query worker panicked during shutdown: {:?}", err);
                }
            }
        }
    }
}

impl DuckDBCore {
    fn open_connection(&self) -> Result<Connection> {
        let access = self.session_access();
        DuckLakeSessionFactory::new(&self.config).open(&access, DuckLakeSessionKind::Query)
    }

    fn init_connection_state_with(&self, conn: Connection) -> Result<ConnectionState> {
        self.init_connection_state_with_options(conn, true)
    }

    fn init_connection_state_with_options(
        &self,
        conn: Connection,
        attach_catalog: bool,
    ) -> Result<ConnectionState> {
        self.configure_connection(&conn)?;
        if attach_catalog {
            self.attach_catalog_if_needed(&conn)?;
        }
        if self.config.ducklake.workspace_scope_mode
            == crate::workspace_scope::WorkspaceScopeMode::Shared
        {
            workspace_views::install(&conn, self.access.physical_scope(), &self.tenant_id)?;
        }
        Ok(ConnectionState {
            conn,
            cache_httpfs_wrap_supported: true,
            cache_httpfs_wrapped_s3: false,
            cache_httpfs_wrapped_httpfs: false,
        })
    }

    #[cfg(test)]
    fn init_connection_state_for_prepared_catalog(
        &self,
        conn: Connection,
    ) -> Result<ConnectionState> {
        self.init_connection_state_with_options(conn, false)
    }

    fn execute_query_on_state(
        &self,
        state: &mut ConnectionState,
        query: &str,
    ) -> Result<QueryResult> {
        // Catalog visibility: Postgres metadata is visible without reconnect.
        // is handled by DuckLake (WAL + busy timeout / ATTACH behavior). Softprobe does not
        // reattach or mem::forget connections after writes.

        let query_run = self.ducklake_inline_sql(query);
        // D12 runs on the final SQL after bare traces/logs/scores have been
        // expanded to qualified DuckLake table names.
        crate::sql::ensure_fact_scan_bound(&query_run).map_err(|e| anyhow!("SQL gate: {e}"))?;
        if std::env::var("SOFTPROBE_LOG_SQL").ok().as_deref() == Some("1") {
            eprintln!("SOFTPROBE_LOG_SQL run={query_run}");
        }
        let diag = std::env::var("PERF_DIAG").ok().as_deref() == Some("1");

        // DuckLake publishes snapshots only on COMMIT. SQL-API DML (harness materialize,
        // ad-hoc INSERT) must not leave orphan parquet invisible to Prom workers.
        // CALL expire/merge/cleanup must NOT be txn-wrapped (AC-N3).
        if sql_needs_softprobe_txn_wrap(&query_run) {
            let trimmed = query_run.trim().trim_end_matches(';');
            let batch = format!("BEGIN TRANSACTION;\n{trimmed};\nCOMMIT;");
            let query_start = std::time::Instant::now();
            self.try_wrap_cache_httpfs_filesystems(state);
            state
                .conn
                .execute_batch(&batch)
                .map_err(|e| anyhow!("DuckLake mutating SQL failed: {e}"))?;
            if diag {
                println!("DIAG execute_query(dml): {:?}", query_start.elapsed());
            }
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
            });
        }
        if sql_is_ducklake_mutating(&query_run) {
            // CALL / other mutating non-wrap path (expire, merge, cleanup, set_option).
            let trimmed = query_run.trim().trim_end_matches(';');
            let query_start = std::time::Instant::now();
            self.try_wrap_cache_httpfs_filesystems(state);
            state
                .conn
                .execute_batch(trimmed)
                .map_err(|e| anyhow!("DuckLake CALL/mutating SQL failed: {e}"))?;
            if diag {
                println!("DIAG execute_query(call): {:?}", query_start.elapsed());
            }
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
            });
        }

        let run_once = |state: &mut ConnectionState| -> Result<QueryResult> {
            let query_start = std::time::Instant::now();
            self.try_wrap_cache_httpfs_filesystems(state);
            let mut stmt = state.conn.prepare(query_run.as_str())?;
            let mut query_rows = stmt.query([])?;
            let column_names = query_rows
                .as_ref()
                .map(|stmt_ref| {
                    (0..stmt_ref.column_count())
                        .filter_map(|idx| {
                            stmt_ref.column_name(idx).ok().map(|name| name.to_string())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let mut rows = Vec::new();
            while let Some(row) = query_rows.next()? {
                let mut values = Vec::with_capacity(column_names.len());
                for idx in 0..column_names.len() {
                    let value: DuckValue = row.get(idx)?;
                    values.push(duck_value_to_json(value));
                }
                rows.push(values);
            }

            let result = QueryResult {
                columns: column_names,
                row_count: rows.len(),
                rows,
            };
            if diag {
                println!("DIAG execute_query: {:?}", query_start.elapsed());
            }
            Ok(result)
        };

        match run_once(state) {
            Ok(result) => Ok(result),
            Err(err) => {
                let message = err.to_string();
                if message.contains("No snapshot found in DuckLake") {
                    warn!(
                        "DuckLake snapshot not visible yet; retrying query once: {}",
                        message
                    );
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    return run_once(state);
                }
                Err(err)
            }
        }
    }

    fn try_wrap_cache_httpfs_filesystems(&self, state: &mut ConnectionState) {
        if self.cache.cache_dir.is_none() {
            return;
        }
        if std::env::var("PERF_DISABLE_CACHE_HTTPFS").ok().as_deref() == Some("1") {
            return;
        }
        if !state.cache_httpfs_wrap_supported {
            return;
        }

        if !state.cache_httpfs_wrapped_s3 {
            match state
                .conn
                .execute("SELECT cache_httpfs_wrap_cache_filesystem('s3');", [])
            {
                Ok(_) => {
                    state.cache_httpfs_wrapped_s3 = true;
                    info!("cache_httpfs wrapped filesystem: s3");
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("already wrapped") {
                        state.cache_httpfs_wrapped_s3 = true;
                        info!("cache_httpfs wrapped filesystem: s3 (already wrapped)");
                    } else if message.contains("hasn't been registered yet") {
                        // Will retry later once filesystem is registered by real usage.
                    } else if message.contains("does not exist")
                        || message.contains("Catalog Error")
                            && message.contains("cache_httpfs_wrap_cache_filesystem")
                    {
                        state.cache_httpfs_wrap_supported = false;
                        warn!("cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused");
                    }
                }
            }
        }

        if !state.cache_httpfs_wrapped_httpfs && state.cache_httpfs_wrap_supported {
            match state
                .conn
                .execute("SELECT cache_httpfs_wrap_cache_filesystem('httpfs');", [])
            {
                Ok(_) => {
                    state.cache_httpfs_wrapped_httpfs = true;
                    info!("cache_httpfs wrapped filesystem: httpfs");
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("already wrapped") {
                        state.cache_httpfs_wrapped_httpfs = true;
                        info!("cache_httpfs wrapped filesystem: httpfs (already wrapped)");
                    } else if message.contains("hasn't been registered yet") {
                        // Will retry later once filesystem is registered by real usage.
                    } else if message.contains("does not exist")
                        || message.contains("Catalog Error")
                            && message.contains("cache_httpfs_wrap_cache_filesystem")
                    {
                        state.cache_httpfs_wrap_supported = false;
                        warn!("cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused");
                    }
                }
            }
        }
    }

    fn configure_connection(&self, conn: &Connection) -> Result<()> {
        // Extension INSTALL/LOAD, resource caps, version guessing, query tuning, and
        // cache_httpfs SETs live in duckdb_init.sql (applied by SessionFactory).
        // Only filesystem wrapping remains here — it is lazy/best-effort until S3/httpfs
        // registers after first real I/O.
        if self.cache.cache_dir.is_some()
            && std::env::var("PERF_DISABLE_CACHE_HTTPFS").ok().as_deref() == Some("1")
        {
            return Ok(());
        }

        if let Err(err) = self.cache.wrap_filesystems(conn) {
            if !CACHE_HTTPFS_CONFIG_WARNED.swap(true, Ordering::Relaxed) {
                warn!("Failed to wrap cache_httpfs filesystems: {}", err);
            }
        }

        Ok(())
    }

    /// Must match [`crate::storage::ducklake::ducklake_qualified_table_name`] (writer DDL uses
    /// `catalog.table` when `metadata_schema` is `main`, not `catalog.main.table`).
    fn ducklake_qualified_table(&self, table: &str) -> String {
        ducklake_qualified_table_name(self.access.physical_scope(), table)
    }

    /// Replace bare telemetry table names with qualified DuckLake table refs.
    fn ducklake_inline_sql(&self, sql: &str) -> String {
        if self.config.ducklake.workspace_scope_mode
            == crate::workspace_scope::WorkspaceScopeMode::Shared
        {
            // Shared-mode workers expose filtered logical views. Keeping the
            // SQL logical prevents callers from bypassing those views with the
            // physical catalog qualification used by isolated mode.
            return sql.to_string();
        }
        qualify_public_telemetry_tables(
            sql,
            &self.ducklake_qualified_table("traces"),
            &self.ducklake_qualified_table("logs"),
            &self.ducklake_qualified_table("scores"),
            &self.ducklake_qualified_table("score_configs"),
        )
    }

    fn attach_catalog_if_needed(&self, conn: &Connection) -> Result<()> {
        let access = self.session_access();
        DuckLakeSessionFactory::new(&self.config)
            .attach(conn, &access)
            .map(|_| ())
    }

    fn session_access(&self) -> DuckLakeAccess {
        self.access.clone()
    }
}

/// Preserve NaN/Inf through JSON (serde_json::Number rejects non-finite → Null otherwise).
fn finite_or_special_float(v: f64) -> Value {
    if v.is_nan() {
        Value::String("NaN".into())
    } else if v.is_infinite() {
        Value::String(if v.is_sign_negative() {
            "-Inf".into()
        } else {
            "+Inf".into()
        })
    } else {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn duck_value_to_json(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(v) => Value::Bool(v),
        DuckValue::TinyInt(v) => Value::Number(v.into()),
        DuckValue::SmallInt(v) => Value::Number(v.into()),
        DuckValue::Int(v) => Value::Number(v.into()),
        DuckValue::BigInt(v) => Value::Number(v.into()),
        DuckValue::HugeInt(v) => Value::String(v.to_string()),
        DuckValue::UTinyInt(v) => Value::Number(v.into()),
        DuckValue::USmallInt(v) => Value::Number(v.into()),
        DuckValue::UInt(v) => Value::Number(v.into()),
        DuckValue::UBigInt(v) => Value::Number(v.into()),
        DuckValue::Float(v) => finite_or_special_float(v as f64),
        DuckValue::Double(v) => finite_or_special_float(v),
        DuckValue::Decimal(v) => Value::String(v.to_string()),
        DuckValue::Timestamp(unit, value) => Value::String(format!("{:?}:{}", unit, value)),
        // Keep VARCHAR/JSON-as-text as strings. Call sites that need objects
        // (VARIANT `CAST(... AS JSON)` attributes) parse in map_string_map.
        DuckValue::Text(v) => Value::String(v),
        DuckValue::Blob(v) => Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
        DuckValue::Date32(v) => Value::String(v.to_string()),
        DuckValue::Time64(unit, value) => Value::String(format!("{:?}:{}", unit, value)),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => Value::String(format!("months={months},days={days},nanos={nanos}")),
        DuckValue::List(v) => Value::Array(v.into_iter().map(duck_value_to_json).collect()),
        DuckValue::Enum(v) => Value::String(v),
        DuckValue::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, field) in fields.iter() {
                map.insert(name.clone(), duck_value_to_json(field.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Array(v) => Value::Array(v.into_iter().map(duck_value_to_json).collect()),
        DuckValue::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries.iter() {
                // `format!("{:?}", key)` emits Rust Debug output (`Text("k")`) instead of
                // the actual string value. Extract the logical key so JSON MAP objects
                // have correct keys regardless of the DuckValue variant.
                let key_str = match key {
                    DuckValue::Text(s) => s.clone(),
                    other => match duck_value_to_json(other.clone()) {
                        Value::String(s) => s,
                        v => v.to_string(),
                    },
                };
                map.insert(key_str, duck_value_to_json(value.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Union(value) => duck_value_to_json(*value),
        // duckdb::types::Value is #[non_exhaustive]; keep forward-compatible.
        other => Value::String(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_scope::{PhysicalScope, WorkspaceBinding};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    #[test]
    fn replace_standalone_ident_rewrites_bare_traces() {
        let s = "SELECT count(*) AS c FROM traces";
        let out = replace_standalone_ident(s, "traces", "softprobe.softprobe.traces");
        assert!(out.contains("softprobe.softprobe.traces"), "got {out}");
        assert!(!out.contains("FROM traces"));
    }

    #[test]
    fn replace_standalone_ident_skips_string_literals() {
        let s = "SELECT count(*) FROM traces WHERE message_type = 'sp.logs.ingest.requests'";
        let out = replace_standalone_ident(s, "logs", "softprobe.ducklake_softprobe_local.logs");
        assert_eq!(
            s, out,
            "must not rewrite logs inside quoted string literals"
        );
    }

    #[test]
    fn replace_standalone_ident_skips_line_and_block_comments() {
        let line = "-- user's query\nSELECT * FROM traces";
        let line_out =
            replace_standalone_ident(line, "traces", "softprobe.ducklake_softprobe_local.traces");
        assert!(
            line_out.contains("softprobe.ducklake_softprobe_local.traces"),
            "line-comment apostrophe must not block rewrite: {line_out}"
        );
        assert!(!line_out.contains("FROM traces"));

        let block = "SELECT * FROM /* user's table traces */ logs";
        let block_out =
            replace_standalone_ident(block, "logs", "softprobe.ducklake_softprobe_local.logs");
        assert!(
            block_out.contains("softprobe.ducklake_softprobe_local.logs"),
            "block-comment apostrophe must not block rewrite: {block_out}"
        );
        assert!(
            block_out.contains("/* user's table traces */"),
            "must not rewrite idents inside block comments: {block_out}"
        );
    }

    #[test]
    fn replace_standalone_ident_does_not_requalify_dotted_suffix() {
        let qualified = "SELECT * FROM softprobe.ducklake_softprobe_local.traces";
        let out = replace_standalone_ident(
            qualified,
            "traces",
            "softprobe.ducklake_softprobe_local.traces",
        );
        assert_eq!(
            out, qualified,
            "qualified trailing segment must not be rewritten again"
        );
    }

    #[test]
    fn replace_standalone_ident_rewrites_table_column_form() {
        let traces = "softprobe.ducklake_softprobe_local.traces";
        let out = replace_standalone_ident("SELECT traces.app_id FROM traces", "traces", traces);
        assert_eq!(
            out,
            format!("SELECT {traces}.app_id FROM {traces}"),
            "table.column must still qualify the table segment"
        );
        let logs = "softprobe.ducklake_softprobe_local.logs";
        let logs_out = replace_standalone_ident("SELECT logs.body FROM logs", "logs", logs);
        assert_eq!(logs_out, format!("SELECT {logs}.body FROM {logs}"));
    }

    #[test]
    fn replace_standalone_ident_skips_double_quoted_idents_with_dashes() {
        let s = r#"SELECT "col--name", count(*) FROM traces"#;
        let out =
            replace_standalone_ident(s, "traces", "softprobe.ducklake_softprobe_local.traces");
        assert!(
            out.contains("softprobe.ducklake_softprobe_local.traces"),
            "double-quoted -- must not start a line comment: {out}"
        );
        assert!(out.contains(r#""col--name""#), "got {out}");
    }

    #[test]
    fn ducklake_inline_pipeline_does_not_double_qualify_traces() {
        let traces = "softprobe.ducklake_softprobe_local.traces";
        let logs = "softprobe.ducklake_softprobe_local.logs";
        let scores = "softprobe.ducklake_softprobe_local.scores";
        let score_configs = "softprobe.ducklake_softprobe_local.score_configs";
        let after_bare = qualify_public_telemetry_tables(
            "SELECT * FROM traces LIMIT 1",
            traces,
            logs,
            scores,
            score_configs,
        );
        assert_eq!(after_bare, format!("SELECT * FROM {traces} LIMIT 1"));
        let after_again =
            qualify_public_telemetry_tables(&after_bare, traces, logs, scores, score_configs);
        assert_eq!(
            after_again, after_bare,
            "bare traces rewrite must not double-qualify already expanded names"
        );
    }

    #[test]
    fn qualify_public_telemetry_tables_expands_scores() {
        let traces = "softprobe.ducklake_softprobe_local.traces";
        let logs = "softprobe.ducklake_softprobe_local.logs";
        let scores = "softprobe.ducklake_softprobe_local.scores";
        let score_configs = "softprobe.ducklake_softprobe_local.score_configs";
        let out = qualify_public_telemetry_tables(
            "SELECT 1 FROM scores WHERE timestamp >= '2026-01-01'",
            traces,
            logs,
            scores,
            score_configs,
        );
        assert_eq!(
            out,
            format!("SELECT 1 FROM {scores} WHERE timestamp >= '2026-01-01'")
        );
    }

    #[test]
    fn qualify_public_telemetry_tables_does_not_rewrite_legacy_aliases() {
        let traces = "softprobe.ducklake_softprobe_local.traces";
        let logs = "softprobe.ducklake_softprobe_local.logs";
        let scores = "softprobe.ducklake_softprobe_local.scores";
        let score_configs = "softprobe.ducklake_softprobe_local.score_configs";
        for legacy in [
            "union_spans",
            "union_logs",
            "committed_spans",
            "buffer_logs",
            "staged_spans",
            "iceberg_logs",
            "tm_all_span",
            "tm_cq_log",
        ] {
            let sql = format!("SELECT 1 FROM {legacy} LIMIT 1");
            let out = qualify_public_telemetry_tables(&sql, traces, logs, scores, score_configs);
            assert_eq!(
                out, sql,
                "legacy alias {legacy} must not be rewritten to a DuckLake table"
            );
        }
    }

    #[test]
    fn ducklake_inline_sql_qualifies_bare_traces_and_logs() {
        use crate::storage::ducklake::ducklake_qualified_table_name;

        let scope = PhysicalScope::new(
            "host=localhost dbname=ducklake",
            "s3://warehouse/tenant/",
            "softprobe",
            "ducklake_softprobe_local",
        );
        let traces = ducklake_qualified_table_name(&scope, "traces");
        let logs = ducklake_qualified_table_name(&scope, "logs");
        let scores = ducklake_qualified_table_name(&scope, "scores");
        let score_configs = ducklake_qualified_table_name(&scope, "score_configs");
        assert_eq!(traces, "softprobe.ducklake_softprobe_local.traces");
        assert_eq!(logs, "softprobe.ducklake_softprobe_local.logs");
        assert_eq!(scores, "softprobe.ducklake_softprobe_local.scores");

        let out = qualify_public_telemetry_tables(
            "SELECT * FROM traces WHERE record_category = 'Servlet'",
            &traces,
            &logs,
            &scores,
            &score_configs,
        );
        assert!(
            out.contains("softprobe.ducklake_softprobe_local.traces"),
            "got {out}"
        );
        let logs_out = qualify_public_telemetry_tables(
            "SELECT 1 FROM logs LIMIT 1",
            &traces,
            &logs,
            &scores,
            &score_configs,
        );
        assert!(
            logs_out.contains("softprobe.ducklake_softprobe_local.logs"),
            "got {logs_out}"
        );
    }

    #[test]
    fn shared_query_keeps_logical_table_names_for_filtered_views() {
        let mut config = Config::default();
        config.ducklake.workspace_scope_mode = crate::workspace_scope::WorkspaceScopeMode::Shared;
        let scope = PhysicalScope::from_ducklake(&config.ducklake);
        let access = DuckLakeAccess::Workspace(
            WorkspaceBinding::new("workspace-a", scope, config.ducklake.workspace_scope_mode)
                .expect("binding"),
        );
        let core = DuckDBCore {
            cache: CacheSettings::new(&config),
            config,
            access,
            counts_toward_liveness: true,
            tenant_id: "workspace-a".to_string(),
        };

        let sql =
            core.ducklake_inline_sql("SELECT * FROM traces JOIN score_configs USING (config_id)");

        assert_eq!(
            sql,
            "SELECT * FROM traces JOIN score_configs USING (config_id)"
        );
    }

    #[test]
    fn shared_connection_initializer_recreates_filtered_views_for_each_connection() {
        for tenant_id in ["workspace-a", "workspace-b"] {
            let mut config = Config::default();
            config.ducklake.catalog_alias = "softprobe".to_string();
            config.ducklake.metadata_schema = "main".to_string();
            config.ducklake.workspace_scope_mode =
                crate::workspace_scope::WorkspaceScopeMode::Shared;
            let scope = PhysicalScope::from_ducklake(&config.ducklake);
            let access = DuckLakeAccess::Workspace(
                WorkspaceBinding::new(tenant_id, scope, config.ducklake.workspace_scope_mode)
                    .expect("binding"),
            );
            let core = DuckDBCore {
                cache: CacheSettings::new(&config),
                config,
                access,
                counts_toward_liveness: false,
                tenant_id: tenant_id.to_string(),
            };
            let state = core
                .init_connection_state_for_prepared_catalog(prepared_catalog_connection())
                .unwrap();

            let visible_id: String = state
                .conn
                .query_row("SELECT id FROM traces", [], |row| row.get(0))
                .unwrap();
            assert_eq!(visible_id, format!("{tenant_id}-row"));
        }
    }

    fn prepared_catalog_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("open prepared catalog connection");
        conn.execute_batch(
            "ATTACH ':memory:' AS softprobe;
             CREATE TABLE softprobe.traces (tenant_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.logs (tenant_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.scores (tenant_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.score_configs (tenant_id VARCHAR, id VARCHAR);
             INSERT INTO softprobe.traces VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.logs VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.scores VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.score_configs VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');",
        )
        .expect("seed prepared catalog");
        conn
    }

    #[test]
    fn public_logs_alias_cannot_bypass_timestamp_gate() {
        assert!(
            crate::sql::ensure_fact_scan_bound("SELECT * FROM logs").is_err(),
            "the public logs alias must be subject to the fact-scan gate"
        );
    }

    #[test]
    fn duck_value_map_keys_are_plain_strings() {
        let entries = vec![
            (
                DuckValue::Text("logger_name".into()),
                DuckValue::Text("com.example".into()),
            ),
            (
                DuckValue::Text("sp.source".into()),
                DuckValue::Text("backend".into()),
            ),
        ];
        let json = duck_value_to_json(DuckValue::Map(entries.into()));
        let obj = json.as_object().expect("should be object");
        assert!(
            obj.contains_key("logger_name"),
            "key should be plain string, got: {json}"
        );
        assert!(
            obj.contains_key("sp.source"),
            "key should be plain string, got: {json}"
        );
        assert!(
            !json.to_string().contains("Text("),
            "keys must not contain Debug wrapper"
        );
    }

    #[test]
    fn sql_is_ducklake_mutating_detects_dml() {
        assert!(sql_is_ducklake_mutating(
            "INSERT INTO softprobe.traces SELECT 1"
        ));
        assert!(sql_is_ducklake_mutating("  create table t(i int)"));
        assert!(sql_is_ducklake_mutating(
            "CALL softprobe.ducklake_merge_adjacent_files('t')"
        ));
        assert!(!sql_is_ducklake_mutating(
            "SELECT count(*) FROM softprobe.traces"
        ));
        assert!(!sql_is_ducklake_mutating("EXPLAIN SELECT 1"));
        assert!(sql_needs_softprobe_txn_wrap(
            "INSERT INTO softprobe.traces SELECT 1"
        ));
        assert!(!sql_needs_softprobe_txn_wrap(
            "CALL ducklake_expire_snapshots('softprobe', older_than => now() - INTERVAL '60 seconds')"
        ));
        assert!(!sql_needs_softprobe_txn_wrap(
            "CALL ducklake_merge_adjacent_files('softprobe', 'traces')"
        ));
    }

    #[test]
    fn duck_value_keeps_json_looking_text_as_string() {
        let value = duck_value_to_json(DuckValue::Text(
            r#"{"Content-Type":"application/json"}"#.to_string(),
        ));
        assert_eq!(
            value,
            Value::String(r#"{"Content-Type":"application/json"}"#.to_string()),
            "VARCHAR JSON blobs (e.g. http headers) must remain strings"
        );
    }

    #[test]
    fn duck_value_keeps_plain_text() {
        let value = duck_value_to_json(DuckValue::Text("plain".to_string()));
        assert_eq!(value, Value::String("plain".to_string()));
    }

    #[test]
    fn duck_value_preserves_nan_and_inf_as_strings() {
        assert_eq!(
            duck_value_to_json(DuckValue::Double(f64::NAN)),
            Value::String("NaN".into())
        );
        assert_eq!(
            duck_value_to_json(DuckValue::Double(f64::INFINITY)),
            Value::String("+Inf".into())
        );
        assert_eq!(
            duck_value_to_json(DuckValue::Double(f64::NEG_INFINITY)),
            Value::String("-Inf".into())
        );
    }

    #[test]
    fn poison_kind_matches_production_outage_messages() {
        // Verbatim from the 2026-08-03 incident logs.
        assert_eq!(
            poison_kind(
                "FATAL Error: Failed: database has been invalidated because of a previous \
                 fatal error. The database must be restarted prior to being used again.\n\
                 Original error: \"Attempted to access index 0 within vector of size 0\""
            ),
            Poison::Collateral
        );
        assert_eq!(
            poison_kind("INTERNAL Error: Attempted to access index 0 within vector of size 0"),
            Poison::Triggered
        );
        assert_eq!(
            poison_kind(
                "Catalog Error: Failed to read inlined data from DuckLake: Table with name \
                 ducklake_inlined_data_28_28 does not exist!\n\
                 Did you mean \"ducklake_inlined_data_28_29\"?\n\n\
                 LINE 3: FROM \"__ducklake_metadata_softprobe\".\"main\".ducklake_inlined_dat..."
            ),
            Poison::Collateral
        );
    }

    #[test]
    fn poison_kind_ignores_ordinary_failures() {
        assert_eq!(
            poison_kind("Catalog Error: Table with name logs does not exist!"),
            Poison::None
        );
        assert_eq!(
            poison_kind("Connection Error: could not reach object store"),
            Poison::None
        );
        assert_eq!(
            poison_kind("Binder Error: column x not found"),
            Poison::None
        );
    }

    #[test]
    fn poison_kind_ignores_markers_in_echoed_sql() {
        // DuckDB echoes the offending statement, and that statement embeds
        // caller-supplied filter values. A `contains` check here let a filter
        // like model_name = "database has been invalidated" force two full
        // connection rebuilds (fresh database + DuckLake ATTACH) per request.
        assert_eq!(
            poison_kind(
                "Catalog Error: Table with name traces does not exist!\n\
                 LINE 1: ... WHERE model_name = 'database has been invalidated'"
            ),
            Poison::None
        );
        assert_eq!(
            poison_kind(
                "Binder Error: no such column\n\
                 LINE 1: ... WHERE user_id = 'INTERNAL Error'"
            ),
            Poison::None
        );
        // The collateral message quotes the internal error on a later line;
        // first-line anchoring must still classify it as collateral, not as
        // the triggering query.
        assert_eq!(
            poison_kind(
                "FATAL Error: Failed: database has been invalidated because of a previous \
                 fatal error.\nOriginal error: \"Attempted to access index 0 within vector \
                 of size 0\""
            ),
            Poison::Collateral
        );
    }

    #[test]
    fn identical_sql_shares_coalesce_key() {
        let a = sql_coalesce_key("SELECT 1");
        let b = sql_coalesce_key("SELECT 1");
        let c = sql_coalesce_key("SELECT 2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[tokio::test]
    async fn cancelled_leader_releases_inflight_waiters() {
        let inflight = Arc::new(Mutex::new(HashMap::new()));
        let key = 7u64;
        {
            let mut pending = InflightLease::lock_map(&inflight);
            pending.insert(key, Vec::new());
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = InflightLease::lock_map(&inflight);
            pending.get_mut(&key).expect("leader").push(tx);
        }
        drop(InflightLease {
            inflight: inflight.clone(),
            key,
            armed: true,
        });
        let notified = rx.await.expect("waiter notified");
        assert!(
            notified
                .err()
                .map(|e| e.to_string().contains("cancelled"))
                .unwrap_or(false),
            "waiter must see leader cancellation"
        );
        assert!(
            InflightLease::lock_map(&inflight).get(&key).is_none(),
            "cancelled leader must drop the inflight key"
        );
        // A later identical query can become the new leader.
        InflightLease::lock_map(&inflight).insert(key, Vec::new());
        let mut lease = InflightLease {
            inflight: inflight.clone(),
            key,
            armed: true,
        };
        assert!(lease.take_waiters().is_empty());
        assert!(InflightLease::lock_map(&inflight).get(&key).is_none());
    }
}
