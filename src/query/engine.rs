use crate::config::Config;
use crate::models::ScoreConfig;
use crate::sql::lake_reads::{LogCountFilter, TraceCountFilter};
use crate::sql::trusted::TrustedSql;
use crate::sql::QueryWindow;
use crate::storage::duckdb::cache::CacheSettings;
#[cfg(test)]
use crate::storage::ducklake::PhysicalScope;
use crate::storage::ducklake::{
    ducklake_qualified_table_name, CatalogPool, DuckLakeAccess, QueryCaller, QueryCancelToken,
    QuerySession, QuerySlot, WorkspaceBinding, WorkspaceScopeMode, QUERY_CANCELLED_MESSAGE,
};
use crate::workspace_scope::{SharedScopeError, SharedScopeErrorCode};
use anyhow::{anyhow, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use duckdb::types::Value as DuckValue;
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::{info, warn};

/// Query result containing columns and rows
#[derive(Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpSpan {
    pub request_method: Option<String>,
    pub request_path: Option<String>,
    pub request_headers: Option<String>,
    pub request_body: Option<String>,
    pub response_status_code: Option<i64>,
    pub response_headers: Option<String>,
    pub response_body: Option<String>,
}

#[derive(Clone)]
pub struct QueryEngine {
    inner: Arc<QueryEngineInner>,
}

type InflightWaiters = Vec<oneshot::Sender<Result<QueryResult>>>;
type InflightKey = (u64, String);

struct InflightEntry {
    subscribers: usize,
    completed: bool,
    waiters: InflightWaiters,
    cancel_token: QueryCancelToken,
}

type InflightMap = HashMap<InflightKey, Arc<Mutex<InflightEntry>>>;

struct QueryEngineInner {
    pool: Arc<CatalogPool>,
    core: Arc<DuckDBCore>,
    /// In-flight identical SQL shares one worker (Grafana panel stampede), not a result TTL.
    inflight: Arc<Mutex<InflightMap>>,
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
use std::sync::atomic::AtomicU64;

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
/// same reason [`crate::api::error::classify_storage_error`] is: DuckDB
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
    /// This query landed on an invalidated connection or a stale DuckLake
    /// catalog attachment. Retry it once on a rebuilt connection.
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
    // inlining is 500.
    if head.starts_with("Catalog Error: Failed to read inlined data from DuckLake")
        || head.starts_with("Invalid Error: Failed to read inlined data from DuckLake")
    {
        return Poison::Collateral;
    }
    Poison::None
}

fn recover_slot(core: &DuckDBCore, slot: &mut QuerySlot, caller: &QueryCaller) -> bool {
    match slot.recover(caller) {
        Ok(()) => {
            if core.counts_toward_liveness {
                SELF_HEAL.rebuilds.fetch_add(1, Ordering::Relaxed);
                SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
            }
            info!("DuckDB query slot rebuilt its connection after a fatal engine error or stale DuckLake catalog attachment");
            true
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
            warn!("DuckDB query slot failed to rebuild its poisoned connection: {err}");
            false
        }
    }
}

struct QueryWorkerResponse {
    result: Result<QueryResult>,
    queue_wait: std::time::Duration,
    exec_elapsed: std::time::Duration,
    gate_elapsed: std::time::Duration,
    run_elapsed: std::time::Duration,
}

struct DuckDBCore {
    config: Config,
    access: DuckLakeAccess,
    cache: CacheSettings,
    /// When false, rebuild failures do not increment process-global SelfHeal
    /// counters used by `/health` liveness (ops engines).
    counts_toward_liveness: bool,
    workspace_id: String,
}

struct TimedExecute {
    result: Result<QueryResult>,
    gate_elapsed: std::time::Duration,
    run_elapsed: std::time::Duration,
}

fn sql_coalesce_key(sql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn lock_inflight_map(inflight: &Mutex<InflightMap>) -> std::sync::MutexGuard<'_, InflightMap> {
    inflight.lock().unwrap_or_else(|p| p.into_inner())
}

fn lock_inflight_entry(entry: &Mutex<InflightEntry>) -> std::sync::MutexGuard<'_, InflightEntry> {
    entry.lock().unwrap_or_else(|p| p.into_inner())
}

struct CancelOnDrop {
    token: QueryCancelToken,
    armed: bool,
}

impl CancelOnDrop {
    fn new(token: QueryCancelToken) -> Self {
        Self { token, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

/// Decrements active subscriber count on caller drop; interrupts DuckDB only
/// when the last awaiting subscriber for this coalesced query disappears.
struct SubscriberGuard {
    inflight: Arc<Mutex<InflightMap>>,
    key: InflightKey,
    entry: Arc<Mutex<InflightEntry>>,
    cancel_token: QueryCancelToken,
    armed: bool,
}

impl SubscriberGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let should_cancel = {
            let mut st = lock_inflight_entry(&self.entry);
            if st.completed {
                false
            } else {
                st.subscribers = st.subscribers.saturating_sub(1);
                if st.subscribers == 0 {
                    st.completed = true;
                    st.waiters.clear();
                    true
                } else {
                    false
                }
            }
        };
        if should_cancel {
            {
                let mut map = lock_inflight_map(&self.inflight);
                if map
                    .get(&self.key)
                    .is_some_and(|cur| Arc::ptr_eq(cur, &self.entry))
                {
                    map.remove(&self.key);
                }
            }
            self.cancel_token.cancel();
        }
    }
}

struct InflightLeaderGuard {
    inflight: Arc<Mutex<InflightMap>>,
    key: InflightKey,
    entry: Arc<Mutex<InflightEntry>>,
    cancel_token: QueryCancelToken,
    armed: bool,
}

impl InflightLeaderGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InflightLeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        {
            let mut map = lock_inflight_map(&self.inflight);
            if map
                .get(&self.key)
                .is_some_and(|cur| Arc::ptr_eq(cur, &self.entry))
            {
                map.remove(&self.key);
            }
        }
        let waiters = {
            let mut st = lock_inflight_entry(&self.entry);
            st.completed = true;
            std::mem::take(&mut st.waiters)
        };
        self.cancel_token.cancel();
        for waiter in waiters {
            let _ = waiter.send(Err(anyhow!("query leader task terminated unexpectedly")));
        }
    }
}

fn clone_error_preserving_chain(err: &anyhow::Error) -> anyhow::Error {
    let chain: Vec<String> = err.chain().map(ToString::to_string).collect();
    let mut iter = chain.into_iter().rev();
    let mut out = anyhow!(iter
        .next()
        .unwrap_or_else(|| "query execution failed".to_string()));
    for ctx in iter {
        out = out.context(ctx);
    }
    out
}

fn fan_out_inflight_result(
    mut waiters: Vec<tokio::sync::oneshot::Sender<Result<QueryResult>>>,
    result: Result<QueryResult>,
) {
    match result {
        Ok(rows) => {
            if waiters.len() > 1 {
                for waiter in waiters.drain(..waiters.len() - 1) {
                    let _ = waiter.send(Ok(rows.clone()));
                }
            }
            if let Some(last) = waiters.pop() {
                let _ = last.send(Ok(rows));
            }
        }
        Err(err) => {
            if waiters.len() > 1 {
                for waiter in waiters.drain(..waiters.len() - 1) {
                    let _ = waiter.send(Err(clone_error_preserving_chain(&err)));
                }
            }
            if let Some(last) = waiters.pop() {
                let _ = last.send(Err(err));
            }
        }
    }
}

impl QueryEngineInner {
    /// Catalog alias for `__ducklake_metadata_<alias>` / `{alias}.promotion_specs`.
    pub(crate) fn catalog_alias(&self) -> &str {
        self.core.access.physical_scope().attach_alias()
    }

    pub(crate) fn workspace_scope_mode(&self) -> WorkspaceScopeMode {
        self.core.config.ducklake.workspace_scope_mode
    }

    pub(crate) async fn execute_trusted(
        self: &Arc<Self>,
        query: crate::sql::trusted::TrustedSql,
    ) -> Result<QueryResult> {
        let scope = self.core.access.physical_scope();
        query
            .validate_for_scope(scope)
            .map_err(|error| anyhow!(error))?;
        self.execute_query(query.as_str()).await
    }

    pub(crate) async fn execute_query(self: &Arc<Self>, query: &str) -> Result<QueryResult> {
        if sql_is_ducklake_mutating(query) {
            let cancel_token = QueryCancelToken::new();
            let mut drop_guard = CancelOnDrop::new(cancel_token.clone());
            let result = self.dispatch_query(query, cancel_token).await;
            drop_guard.disarm();
            return result;
        }

        let key = (sql_coalesce_key(query), query.to_string());
        let (entry, rx, cancel_token, is_leader) = {
            let mut pending = lock_inflight_map(&self.inflight);
            let mut joined = None;
            if let Some(existing) = pending.get(&key).cloned() {
                let mut st = lock_inflight_entry(&existing);
                if !st.completed {
                    let (tx, rx) = oneshot::channel();
                    st.subscribers += 1;
                    st.waiters.push(tx);
                    let tok = st.cancel_token.clone();
                    drop(st);
                    joined = Some((existing, rx, tok, false));
                }
            }
            if let Some(found) = joined {
                found
            } else {
                let (tx, rx) = oneshot::channel();
                let cancel_token = QueryCancelToken::new();
                let entry = Arc::new(Mutex::new(InflightEntry {
                    subscribers: 1,
                    completed: false,
                    waiters: vec![tx],
                    cancel_token: cancel_token.clone(),
                }));
                pending.insert(key.clone(), Arc::clone(&entry));
                (entry, rx, cancel_token, true)
            }
        };

        if is_leader {
            let inner = Arc::clone(self);
            let sql_owned = query.to_string();
            let leader_key = key.clone();
            let leader_entry = Arc::clone(&entry);
            let leader_token = cancel_token.clone();
            tokio::spawn(async move {
                let mut leader_guard = InflightLeaderGuard {
                    inflight: Arc::clone(&inner.inflight),
                    key: leader_key.clone(),
                    entry: Arc::clone(&leader_entry),
                    cancel_token: leader_token.clone(),
                    armed: true,
                };
                let result = inner.dispatch_query(&sql_owned, leader_token).await;
                {
                    let mut map = lock_inflight_map(&inner.inflight);
                    if map
                        .get(&leader_key)
                        .is_some_and(|cur| Arc::ptr_eq(cur, &leader_entry))
                    {
                        map.remove(&leader_key);
                    }
                }
                let waiters = {
                    let mut st = lock_inflight_entry(&leader_entry);
                    st.completed = true;
                    std::mem::take(&mut st.waiters)
                };
                leader_guard.disarm();
                fan_out_inflight_result(waiters, result);
            });
        }

        let mut sub_guard = SubscriberGuard {
            inflight: Arc::clone(&self.inflight),
            key,
            entry,
            cancel_token,
            armed: true,
        };
        let received = rx.await.map_err(|_| anyhow!(QUERY_CANCELLED_MESSAGE))?;
        sub_guard.disarm();
        received
    }

    async fn dispatch_query(
        &self,
        query: &str,
        cancel_token: QueryCancelToken,
    ) -> Result<QueryResult> {
        let enqueued_at = std::time::Instant::now();
        let core = Arc::clone(&self.core);
        let sql = query.to_string();
        let response = self
            .pool
            .with_query(
                &self.core.workspace_id,
                cancel_token,
                move |slot, caller, token| {
                    run_query_job(&core, slot, caller, &sql, enqueued_at, token)
                },
            )
            .await?;
        let elapsed = response.queue_wait + response.exec_elapsed;
        if elapsed >= std::time::Duration::from_millis(200) {
            let preview: String = query.chars().take(160).collect();
            let sql_kind = crate::self_monitoring::classify_sql_kind(query);
            warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                queue_wait_ms = response.queue_wait.as_millis() as u64,
                sql_gate_ms = response.gate_elapsed.as_millis() as u64,
                sql_exec_ms = response.run_elapsed.as_millis() as u64,
                sql = %preview,
                "slow DuckDB query (queue + execute)"
            );
            if self.core.counts_toward_liveness {
                crate::self_monitoring::record_slow_query(&self.core.workspace_id, sql_kind);
            }
        }
        response.result
    }

    /// One-shot metadata SQL on a borrowed query slot (no self-monitoring
    /// instruments). Used by inventory scrapes.
    pub(crate) async fn execute_query_uninstrumented(&self, query: &str) -> Result<QueryResult> {
        let mut rows = self.execute_queries_uninstrumented(vec![query]).await?;
        rows.pop()
            .ok_or_else(|| anyhow!("inventory query returned no result"))?
    }

    /// Run several metadata SQLs on one borrowed query slot (inventory).
    pub(crate) async fn execute_queries_uninstrumented(
        &self,
        queries: Vec<&str>,
    ) -> Result<Vec<Result<QueryResult>>> {
        let core = Arc::clone(&self.core);
        let sqls: Vec<String> = queries.iter().map(|s| (*s).to_string()).collect();
        let cancel_token = QueryCancelToken::new();
        let mut drop_guard = CancelOnDrop::new(cancel_token.clone());
        let out = self
            .pool
            .with_query(
                &self.core.workspace_id,
                cancel_token,
                move |slot, _, token| {
                    sqls.iter()
                        .map(|sql| core.execute_on_slot_cancellable(slot, sql, token).result)
                        .collect()
                },
            )
            .await;
        drop_guard.disarm();
        out
    }
}

/// Run one request on a slot already bound to `caller`, rebuilding the slot
/// when the engine reports a poisoned database.
fn run_query_job(
    core: &DuckDBCore,
    slot: &mut QuerySlot,
    caller: &QueryCaller,
    sql: &str,
    enqueued_at: std::time::Instant,
    cancel_token: &QueryCancelToken,
) -> QueryWorkerResponse {
    if cancel_token.is_cancelled() {
        return QueryWorkerResponse {
            result: Err(anyhow!(QUERY_CANCELLED_MESSAGE)),
            queue_wait: enqueued_at.elapsed(),
            exec_elapsed: std::time::Duration::ZERO,
            gate_elapsed: std::time::Duration::ZERO,
            run_elapsed: std::time::Duration::ZERO,
        };
    }
    let queue_wait = enqueued_at.elapsed();
    let sql_kind = crate::self_monitoring::classify_sql_kind(sql);
    if core.counts_toward_liveness {
        crate::self_monitoring::record_query_queue_wait(&core.workspace_id, sql_kind, queue_wait);
    }
    crate::self_monitoring::gauge_store::QUERY_WORKERS_BUSY.fetch_add(1, Ordering::Relaxed);
    let exec_start = std::time::Instant::now();
    let mut timed = core.execute_on_slot_cancellable(slot, sql, cancel_token);
    let mut result = timed.result;
    let mut gate_elapsed = timed.gate_elapsed;
    let mut run_elapsed = timed.run_elapsed;
    if let Err(err) = &result {
        let kind = poison_kind(&err.to_string());
        if kind != Poison::None {
            // This slot's in-memory database is dead; every future statement
            // would fail with the same FATAL error. Rebuild before touching
            // the next request.
            if recover_slot(core, slot, caller)
                && kind == Poison::Collateral
                && !cancel_token.is_cancelled()
            {
                timed = core.execute_on_slot_cancellable(slot, sql, cancel_token);
                result = timed.result;
                gate_elapsed = timed.gate_elapsed;
                run_elapsed = timed.run_elapsed;
                if let Err(retry_err) = &result {
                    if poison_kind(&retry_err.to_string()) != Poison::None {
                        // The retry poisoned the fresh connection too; rebuild
                        // again so the next request does not inherit a dead one.
                        recover_slot(core, slot, caller);
                    }
                }
            }
        }
    }
    let exec_elapsed = exec_start.elapsed();
    crate::self_monitoring::gauge_store::QUERY_WORKERS_BUSY.fetch_sub(1, Ordering::Relaxed);
    if core.counts_toward_liveness {
        crate::self_monitoring::record_query(&core.workspace_id, sql_kind, exec_elapsed);
        crate::self_monitoring::record_query_stage(
            &core.workspace_id,
            sql_kind,
            crate::self_monitoring::query_stage::SQL_GATE,
            gate_elapsed,
        );
        crate::self_monitoring::record_query_stage(
            &core.workspace_id,
            sql_kind,
            crate::self_monitoring::query_stage::SQL_EXEC,
            run_elapsed,
        );
        if result.is_ok() {
            // Any *customer* success clears the global streak -- see
            // SelfHealCounters. Ops engines must not clear (or they can hide
            // a dead customer plane behind healthy Grafana traffic).
            SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }
    QueryWorkerResponse {
        result,
        queue_wait,
        exec_elapsed,
        gate_elapsed,
        run_elapsed,
    }
}

impl QueryEngine {
    /// Bind a workspace-facing query surface to a shared catalog pool. The
    /// engine owns no sessions; it only carries the workspace identity and the
    /// in-flight collapse map.
    pub(crate) fn bind(
        pool: Arc<CatalogPool>,
        workspace_id: &str,
        counts_toward_liveness: bool,
    ) -> Result<Self> {
        let config = pool.config().clone();
        let binding = WorkspaceBinding::new(
            workspace_id,
            pool.scope().clone(),
            config.ducklake.workspace_scope_mode,
        )
        .map_err(|error| anyhow!(error))?;
        let core = Arc::new(DuckDBCore {
            cache: CacheSettings::new(&config),
            config,
            access: DuckLakeAccess::Workspace(binding),
            counts_toward_liveness,
            workspace_id: workspace_id.to_string(),
        });
        Ok(Self {
            inner: Arc::new(QueryEngineInner {
                pool,
                core,
                inflight: Arc::new(Mutex::new(HashMap::new())),
            }),
        })
    }

    pub(crate) fn catalog_alias(&self) -> &str {
        self.inner.catalog_alias()
    }

    pub fn workspace_id(&self) -> &str {
        &self.inner.core.workspace_id
    }

    pub fn workspace_scope_mode(&self) -> WorkspaceScopeMode {
        self.inner.workspace_scope_mode()
    }

    pub async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        self.ensure_raw_sql_allowed()?;
        self.inner.execute_query(query).await
    }

    pub async fn execute_query_uninstrumented(&self, query: &str) -> Result<QueryResult> {
        self.ensure_raw_sql_allowed()?;
        self.inner.execute_query_uninstrumented(query).await
    }

    pub(crate) async fn execute_queries_uninstrumented(
        &self,
        queries: Vec<&str>,
    ) -> Result<Vec<Result<QueryResult>>> {
        self.ensure_raw_sql_allowed()?;
        self.inner.execute_queries_uninstrumented(queries).await
    }

    pub(crate) async fn execute_trusted(&self, query: TrustedSql) -> Result<QueryResult> {
        self.inner.execute_trusted(query).await
    }

    pub async fn count_logs(&self, filter: LogCountFilter) -> Result<u64> {
        let query = crate::sql::lake_reads::count_logs(&filter).map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    pub async fn count_traces(&self, filter: TraceCountFilter) -> Result<u64> {
        let query = crate::sql::lake_reads::count_traces(&filter).map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    pub async fn find_http_span(
        &self,
        session_id: &str,
        time_window: QueryWindow,
    ) -> Result<Option<HttpSpan>> {
        let query = crate::sql::lake_reads::find_http_span(session_id, time_window)
            .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        let Some(row) = result.rows.first() else {
            return Ok(None);
        };
        Ok(Some(HttpSpan {
            request_method: row.first().and_then(|v| v.as_str()).map(str::to_owned),
            request_path: row.get(1).and_then(|v| v.as_str()).map(str::to_owned),
            request_headers: row.get(2).and_then(|v| v.as_str()).map(str::to_owned),
            request_body: row.get(3).and_then(|v| v.as_str()).map(str::to_owned),
            response_status_code: row.get(4).and_then(|v| v.as_i64()),
            response_headers: row.get(5).and_then(|v| v.as_str()).map(str::to_owned),
            response_body: row.get(6).and_then(|v| v.as_str()).map(str::to_owned),
        }))
    }

    pub async fn count_trace_days(
        &self,
        session_id: &str,
        time_window: QueryWindow,
    ) -> Result<u64> {
        let query = crate::sql::lake_reads::count_trace_days(session_id, time_window)
            .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    pub async fn count_traces_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
        time_window: QueryWindow,
    ) -> Result<u64> {
        let query =
            crate::sql::lake_reads::count_traces_by_attribute(session_id, key, value, time_window)
                .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    pub async fn count_logs_by_attribute(
        &self,
        key: &str,
        value: &str,
        time_window: QueryWindow,
    ) -> Result<u64> {
        let query = crate::sql::lake_reads::count_logs_by_attribute(key, value, time_window)
            .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(first_count(&result))
    }

    pub async fn trace_attributes_by_attribute(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
        time_window: QueryWindow,
    ) -> Result<Option<Value>> {
        let query = crate::sql::lake_reads::trace_attributes_by_attribute(
            session_id,
            key,
            value,
            time_window,
        )
        .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(map_attributes_row(&result))
    }

    pub async fn trace_attributes_for_span(
        &self,
        span_id: &str,
        time_window: QueryWindow,
    ) -> Result<Option<Value>> {
        let query = crate::sql::lake_reads::trace_attributes_for_span(span_id, time_window)
            .map_err(|e| anyhow!(e))?;
        let result = self.execute_trusted(query).await?;
        Ok(map_attributes_row(&result))
    }

    pub async fn search_spans(
        &self,
        request: &crate::sql::llm::search::SpanSearchRequest,
    ) -> Result<QueryResult> {
        let query = crate::sql::llm::search_spans(request).map_err(anyhow::Error::msg)?;
        self.execute_trusted(query).await
    }

    pub async fn telemetry_details_logs(
        &self,
        target: &crate::sql::telemetry::TelemetryDetailsTarget,
        time_range: &crate::sql::telemetry::TelemetryTimeRange,
        limit: usize,
    ) -> Result<QueryResult> {
        let query = crate::sql::telemetry::details_logs(target, time_range, limit)
            .map_err(anyhow::Error::msg)?;
        self.execute_trusted(query).await
    }

    pub async fn score_exists(&self, score_id: &str, timestamp: DateTime<Utc>) -> Result<bool> {
        let window = QueryWindow::try_new(timestamp, timestamp).map_err(|e| anyhow!(e))?;
        let query =
            crate::sql::lake_reads::score_exists(score_id, window).map_err(|e| anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    pub async fn score_config_exists(&self, config_id: &str) -> Result<bool> {
        let query =
            crate::sql::lake_reads::score_config_exists(config_id).map_err(|e| anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    pub async fn list_score_configs(&self) -> Result<Vec<ScoreConfig>> {
        let query = crate::sql::lake_reads::list_score_configs().map_err(|e| anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .iter()
            .filter_map(|r| ScoreConfig::from_json_row(r))
            .collect())
    }

    pub async fn get_score_config(&self, config_id: &str) -> Result<Option<ScoreConfig>> {
        let query = crate::sql::lake_reads::get_score_config(config_id).map_err(|e| anyhow!(e))?;
        let result = match self.execute_trusted(query).await {
            Ok(res) => res,
            Err(err) if err.to_string().contains("does not exist") => return Ok(None),
            Err(err) => return Err(err),
        };
        Ok(result
            .rows
            .first()
            .and_then(|r| ScoreConfig::from_json_row(r)))
    }

    fn ensure_raw_sql_allowed(&self) -> Result<()> {
        if self.workspace_scope_mode() == WorkspaceScopeMode::Shared {
            return Err(anyhow!(SharedScopeError::new(
                SharedScopeErrorCode::RawSqlForbidden,
                "raw SQL is disabled for shared workspace scope; use a typed query API",
            )));
        }
        Ok(())
    }
}

fn first_count(result: &QueryResult) -> u64 {
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| value.as_i64())
        .unwrap_or_default() as u64
}

fn map_attributes_row(result: &QueryResult) -> Option<Value> {
    let value = result.rows.first().and_then(|row| row.first())?;
    value
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .or_else(|| Some(value.clone()))
}

impl DuckDBCore {
    fn execute_on_slot_cancellable(
        &self,
        slot: &mut QuerySlot,
        query: &str,
        cancel_token: &QueryCancelToken,
    ) -> TimedExecute {
        match slot.session() {
            Ok(session) => self.execute_query_on_state_cancellable(session, query, cancel_token),
            Err(err) => TimedExecute {
                result: Err(err),
                gate_elapsed: std::time::Duration::ZERO,
                run_elapsed: std::time::Duration::ZERO,
            },
        }
    }

    #[cfg(test)]
    fn execute_query_on_state(&self, state: &mut QuerySession, query: &str) -> TimedExecute {
        let token = QueryCancelToken::new();
        self.execute_query_on_state_cancellable(state, query, &token)
    }

    fn execute_query_on_state_cancellable(
        &self,
        state: &mut QuerySession,
        query: &str,
        cancel_token: &QueryCancelToken,
    ) -> TimedExecute {
        // Catalog visibility: Postgres metadata is visible without reconnect.
        // is handled by DuckLake (WAL + busy timeout / ATTACH behavior). Softprobe does not
        // reattach or mem::forget connections after writes.

        let query_run = self.ducklake_inline_sql(query);
        let is_mutating =
            sql_needs_softprobe_txn_wrap(&query_run) || sql_is_ducklake_mutating(&query_run);
        let _interrupt_guard = if !is_mutating {
            match cancel_token.bind_interrupt(Arc::clone(&state.interrupt_handle)) {
                Ok(guard) => Some(guard),
                Err(err) => {
                    return TimedExecute {
                        result: Err(err),
                        gate_elapsed: std::time::Duration::ZERO,
                        run_elapsed: std::time::Duration::ZERO,
                    };
                }
            }
        } else {
            None
        };

        // Optional query-worker fact-scan gate (`query.sql_gate`, default off).
        let gate_start = std::time::Instant::now();
        if self.config.query.sql_gate {
            if let Err(e) =
                crate::sql::ensure_fact_scan_uses_timestamp_pruning(&state.conn, &query_run)
            {
                let err = if cancel_token.is_cancelled() {
                    anyhow!(QUERY_CANCELLED_MESSAGE)
                } else {
                    anyhow!("SQL gate: {e}")
                };
                return TimedExecute {
                    result: Err(err),
                    gate_elapsed: gate_start.elapsed(),
                    run_elapsed: std::time::Duration::ZERO,
                };
            }
        }
        let gate_elapsed = gate_start.elapsed();
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
            state.try_wrap_cache_httpfs_filesystems(&self.cache);
            let result = state
                .conn
                .execute_batch(&batch)
                .map_err(|e| anyhow!("DuckLake mutating SQL failed: {e}"))
                .map(|_| QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    row_count: 0,
                });
            if diag {
                println!("DIAG execute_query(dml): {:?}", query_start.elapsed());
            }
            return TimedExecute {
                result,
                gate_elapsed,
                run_elapsed: query_start.elapsed(),
            };
        }
        if sql_is_ducklake_mutating(&query_run) {
            // CALL / other mutating non-wrap path (expire, merge, cleanup, set_option).
            let trimmed = query_run.trim().trim_end_matches(';');
            let query_start = std::time::Instant::now();
            state.try_wrap_cache_httpfs_filesystems(&self.cache);
            let result = state
                .conn
                .execute_batch(trimmed)
                .map_err(|e| anyhow!("DuckLake CALL/mutating SQL failed: {e}"))
                .map(|_| QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    row_count: 0,
                });
            if diag {
                println!("DIAG execute_query(call): {:?}", query_start.elapsed());
            }
            return TimedExecute {
                result,
                gate_elapsed,
                run_elapsed: query_start.elapsed(),
            };
        }

        let run_once = |state: &mut QuerySession| -> Result<QueryResult> {
            if cancel_token.is_cancelled() {
                return Err(anyhow!(QUERY_CANCELLED_MESSAGE));
            }
            let query_start = std::time::Instant::now();
            state.try_wrap_cache_httpfs_filesystems(&self.cache);
            // duckdb-rs requires Statement for multi-row fetch. Preparing the
            // full SQL every request with inlined literals gets no reuse.
            // TODO(#114): parameterized SQL + per-worker Statement cache.
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
                if rows.len() % 256 == 0 && cancel_token.is_cancelled() {
                    return Err(anyhow!(QUERY_CANCELLED_MESSAGE));
                }
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

        let run_start = std::time::Instant::now();
        let result = match run_once(state) {
            Ok(result) => Ok(result),
            Err(err) => {
                let message = err.to_string();
                if cancel_token.is_cancelled() || message.starts_with("Interrupt Error") {
                    Err(anyhow!(QUERY_CANCELLED_MESSAGE))
                } else if message.contains("No snapshot found in DuckLake") {
                    warn!(
                        "DuckLake snapshot not visible yet; retrying query once: {}",
                        message
                    );
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    run_once(state)
                } else {
                    Err(err)
                }
            }
        };
        TimedExecute {
            result,
            gate_elapsed,
            run_elapsed: run_start.elapsed(),
        }
    }

    /// Must match [`crate::storage::ducklake::ducklake_qualified_table_name`]
    /// (`catalog.schema.table`, including when schema is `main`).
    fn ducklake_qualified_table(&self, table: &str) -> String {
        ducklake_qualified_table_name(self.access.physical_scope(), table)
    }

    /// Replace bare telemetry table names with qualified DuckLake table refs.
    fn ducklake_inline_sql(&self, sql: &str) -> String {
        if self.config.ducklake.workspace_scope_mode == WorkspaceScopeMode::Shared {
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
        // (attribute-map `CAST(... AS JSON)` projections) parse in map_string_map.
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

    use crate::storage::ducklake::workspace_views;
    use duckdb::Connection;
    use std::sync::Arc;

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
        config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Shared;
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
            workspace_id: "workspace-a".to_string(),
        };

        let sql =
            core.ducklake_inline_sql("SELECT * FROM traces JOIN score_configs USING (config_id)");

        assert_eq!(
            sql,
            "SELECT * FROM traces JOIN score_configs USING (config_id)"
        );
    }

    fn prepared_catalog_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("open prepared catalog connection");
        conn.execute_batch(
            "ATTACH ':memory:' AS softprobe;
             CREATE SCHEMA IF NOT EXISTS softprobe.main;
             CREATE TABLE softprobe.main.traces (workspace_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.main.logs (workspace_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.main.scores (workspace_id VARCHAR, id VARCHAR);
             CREATE TABLE softprobe.main.score_configs (workspace_id VARCHAR, id VARCHAR);
             INSERT INTO softprobe.main.traces VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.main.logs VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.main.scores VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
             INSERT INTO softprobe.main.score_configs VALUES
               ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');",
        )
        .expect("seed prepared catalog");
        conn
    }

    fn prepared_session(core: &DuckDBCore) -> QuerySession {
        let session = QuerySession::new(prepared_catalog_connection());
        workspace_views::install(
            &session.conn,
            core.access.physical_scope(),
            &core.workspace_id,
        )
        .expect("install filtered views");
        session
    }

    fn test_core(sql_gate: bool) -> DuckDBCore {
        let mut config = Config::default();
        config.query.sql_gate = sql_gate;
        config.ducklake.catalog_alias = "softprobe".to_string();
        config.ducklake.metadata_schema = "main".to_string();
        config.ducklake.workspace_scope_mode = WorkspaceScopeMode::Shared;
        let scope = PhysicalScope::from_ducklake(&config.ducklake);
        let access = DuckLakeAccess::Workspace(
            WorkspaceBinding::new("workspace-a", scope, config.ducklake.workspace_scope_mode)
                .expect("binding"),
        );
        DuckDBCore {
            cache: CacheSettings::new(&config),
            config,
            access,
            counts_toward_liveness: false,
            workspace_id: "workspace-a".to_string(),
        }
    }

    #[test]
    fn sql_gate_false_skips_gate_and_runs() {
        let core = test_core(false);
        let mut state = prepared_session(&core);
        let timed = core.execute_query_on_state(&mut state, "SELECT id FROM traces LIMIT 1");
        let result = timed.result.expect("sql_gate false must skip gate and run");
        assert_eq!(result.columns, vec!["id".to_string()]);
    }

    #[test]
    fn sql_gate_false_still_runs_explain_result_sets() {
        let core = test_core(false);
        let mut state = prepared_session(&core);
        let timed =
            core.execute_query_on_state(&mut state, "EXPLAIN SELECT id FROM traces LIMIT 1");
        let result = timed
            .result
            .expect("EXPLAIN must work on query worker path");
        assert!(result.row_count > 0);
    }

    #[test]
    fn sql_gate_true_rejects_unbounded_fact_scan() {
        let core = test_core(true);
        let mut state = prepared_session(&core);
        let timed = core.execute_query_on_state(&mut state, "SELECT id FROM traces LIMIT 1");
        let err = timed
            .result
            .err()
            .expect("sql_gate true must reject unbounded fact scan")
            .to_string();
        assert!(
            err.contains("SQL gate"),
            "expected SQL gate prefix, got {err}"
        );
        assert!(timed.run_elapsed.is_zero());
    }

    #[test]
    fn public_logs_alias_cannot_bypass_timestamp_gate() {
        assert!(
            crate::sql::ensure_sql_has_bare_timestamp_predicate("SELECT * FROM logs").is_err(),
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
        assert_eq!(
            poison_kind(
                "Invalid Error: Failed to read inlined data from DuckLake: Failed to prepare COPY \
                 \"COPY (SELECT ... FROM \\\"__ducklake_metadata_softprobe\\\".\\\"ducklake_inlined_data_8_8\\\") TO STDOUT\""
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

    #[test]
    fn poison_kind_ignores_duckdb_interrupt_error() {
        assert_eq!(poison_kind("Interrupt Error: Interrupted!"), Poison::None);
    }

    async fn open_single_slot_test_engine(
        mode: WorkspaceScopeMode,
    ) -> (QueryEngine, Arc<CatalogPool>, tempfile::TempDir) {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = crate::test_support::file_backed_test_config(&temp);
        cfg.ducklake.workspace_scope_mode = mode;
        cfg.query.max_connections = 1;
        let scope = PhysicalScope::from_ducklake(&cfg.ducklake);
        let pool = CatalogPool::open(&cfg, &scope, None)
            .await
            .expect("open single-slot pool");
        let engine =
            QueryEngine::bind(Arc::clone(&pool), "ws-cancel-test", true).expect("bind engine");
        (engine, pool, temp)
    }

    #[tokio::test]
    async fn dropping_query_future_interrupts_active_duckdb_scan_and_preserves_slot_health() {
        for mode in [WorkspaceScopeMode::Isolated, WorkspaceScopeMode::Shared] {
            let (engine, _pool, _temp) = open_single_slot_test_engine(mode).await;
            let heal_before = self_heal_snapshot();

            let slow_sql = crate::sql::trusted::approved_query(
                "SELECT sum(hash(i, j)) AS s FROM range(25000) t1(i), range(25000) t2(j)",
            )
            .expect("trusted slow sql");
            let timed_out = tokio::time::timeout(
                std::time::Duration::from_millis(80),
                engine.execute_trusted(slow_sql),
            )
            .await;
            assert!(
                timed_out.is_err(),
                "625M-row hash cross join must still be running at 80ms"
            );

            let followup_start = std::time::Instant::now();
            let fast_sql =
                crate::sql::trusted::approved_query("SELECT 42 AS answer").expect("fast sql");
            let followup = engine
                .execute_trusted(fast_sql)
                .await
                .expect("follow-up query on interrupted slot must succeed");
            let followup_elapsed = followup_start.elapsed();
            assert_eq!(followup.rows, vec![vec![serde_json::json!(42)]]);
            assert!(
                followup_elapsed < std::time::Duration::from_millis(400),
                "interrupted slot must release promptly in {mode:?}, took {followup_elapsed:?}"
            );

            let heal_after = self_heal_snapshot();
            assert_eq!(
                heal_after.rebuilds, heal_before.rebuilds,
                "interrupt must not trigger self-heal rebuild in {mode:?}"
            );
            assert_eq!(heal_after.consecutive_failures, 0);
        }
    }

    #[tokio::test]
    async fn dropping_queued_query_skips_execution_when_slot_becomes_free() {
        let (engine, _pool, _temp) =
            open_single_slot_test_engine(WorkspaceScopeMode::Isolated).await;

        let holder_engine = engine.clone();
        let holder = tokio::spawn(async move {
            let medium_sql = crate::sql::trusted::approved_query(
                "SELECT sum(hash(i, j)) AS s FROM range(4000) t1(i), range(4000) t2(j)",
            )
            .expect("medium sql");
            holder_engine.execute_trusted(medium_sql).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let queued_slow = crate::sql::trusted::approved_query(
            "SELECT sum(hash(i, j)) AS s FROM range(25000) t1(i), range(25000) t2(j)",
        )
        .expect("queued slow sql");
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            engine.execute_trusted(queued_slow),
        )
        .await;

        holder.await.expect("join holder").expect("holder succeeds");

        let followup_start = std::time::Instant::now();
        let fast_sql = crate::sql::trusted::approved_query("SELECT 7 AS v").expect("fast sql");
        let res = engine.execute_trusted(fast_sql).await.expect("fast query");
        let followup_elapsed = followup_start.elapsed();
        assert_eq!(res.rows, vec![vec![serde_json::json!(7)]]);
        assert!(
            followup_elapsed < std::time::Duration::from_millis(250),
            "dropped queued query must not execute after holder finishes, took {followup_elapsed:?}"
        );
    }

    #[tokio::test]
    async fn coalesced_inflight_query_survives_single_subscriber_drop_and_cancels_when_all_drop() {
        let (engine, _pool, _temp) = open_single_slot_test_engine(WorkspaceScopeMode::Shared).await;

        let shared_medium =
            "SELECT count(*) AS c FROM range(3500) t1(i), range(3500) t2(j) WHERE hash(i, j) > 0";
        let e1 = engine.clone();
        let sub1 = tokio::spawn(async move {
            let q = crate::sql::trusted::approved_query(shared_medium).unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(25), e1.execute_trusted(q)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let e2 = engine.clone();
        let sub2 = tokio::spawn(async move {
            let q = crate::sql::trusted::approved_query(shared_medium).unwrap();
            e2.execute_trusted(q).await
        });

        let sub1_res = sub1.await.expect("sub1 join");
        assert!(sub1_res.is_err(), "sub1 should drop via timeout at 25ms");
        let sub2_res = sub2
            .await
            .expect("sub2 join")
            .expect("remaining subscriber must still receive query result when leader drops");
        assert_eq!(sub2_res.row_count, 1);

        let shared_slow = "SELECT sum(hash(i, j)) AS s FROM range(25000) t1(i), range(25000) t2(j)";
        let e3 = engine.clone();
        let s1 = tokio::spawn(async move {
            let q = crate::sql::trusted::approved_query(shared_slow).unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(35), e3.execute_trusted(q)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let e4 = engine.clone();
        let s2 = tokio::spawn(async move {
            let q = crate::sql::trusted::approved_query(shared_slow).unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(60), e4.execute_trusted(q)).await
        });
        assert!(s1.await.unwrap().is_err());
        assert!(s2.await.unwrap().is_err());

        let followup_start = std::time::Instant::now();
        let fast_sql = crate::sql::trusted::approved_query("SELECT 99 AS v").unwrap();
        let followup = engine.execute_trusted(fast_sql).await.expect("followup");
        let followup_elapsed = followup_start.elapsed();
        assert_eq!(followup.rows, vec![vec![serde_json::json!(99)]]);
        assert!(
            followup_elapsed < std::time::Duration::from_millis(400),
            "once all coalesced subscribers drop, DuckDB must interrupt promptly; took {followup_elapsed:?}"
        );
    }

    #[tokio::test]
    async fn queued_cancel_before_slot_bind_marks_token_done_on_drop() {
        let (_engine, pool, _temp) = open_single_slot_test_engine(WorkspaceScopeMode::Shared).await;

        let (slot_entered_tx, slot_entered_rx) = tokio::sync::oneshot::channel();
        let (release_slot_tx, release_slot_rx) = std::sync::mpsc::channel();
        let holder_pool = Arc::clone(&pool);
        let holder = tokio::spawn(async move {
            holder_pool
                .with_query(
                    "ws-holder",
                    QueryCancelToken::new(),
                    move |_slot, _caller, _tok| {
                        let _ = slot_entered_tx.send(());
                        let _ = release_slot_rx.recv();
                        Ok::<_, anyhow::Error>(())
                    },
                )
                .await
        });

        slot_entered_rx.await.expect("holder entered single slot");

        let queued_token = QueryCancelToken::new();
        let mut queued_fut = Box::pin(pool.with_query(
            "ws-queued",
            queued_token.clone(),
            |_slot, _caller, _tok| Ok::<_, anyhow::Error>(()),
        ));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(15), &mut queued_fut)
                .await
                .is_err(),
            "second query must wait while single slot is occupied"
        );

        queued_token.cancel();
        assert!(queued_token.is_cancelled());
        assert!(!queued_token.is_done());

        drop(queued_fut);
        assert!(
            queued_token.is_done(),
            "dropping a queued with_query future before checkout must mark its cancel token done"
        );

        let _ = release_slot_tx.send(());
        holder
            .await
            .expect("join holder")
            .expect("with_query")
            .expect("job");
    }

    #[test]
    fn inflight_leader_guard_cleans_up_map_and_notifies_waiters_on_abort() {
        let inflight: Arc<Mutex<InflightMap>> = Arc::new(Mutex::new(HashMap::new()));
        let key: InflightKey = (sql_coalesce_key("SELECT 1"), "SELECT 1".to_string());
        let (tx1, mut rx1) = oneshot::channel();
        let (tx2, mut rx2) = oneshot::channel();
        let cancel_token = QueryCancelToken::new();
        let entry = Arc::new(Mutex::new(InflightEntry {
            subscribers: 2,
            completed: false,
            waiters: vec![tx1, tx2],
            cancel_token: cancel_token.clone(),
        }));
        lock_inflight_map(&inflight).insert(key.clone(), Arc::clone(&entry));

        {
            let _guard = InflightLeaderGuard {
                inflight: Arc::clone(&inflight),
                key: key.clone(),
                entry: Arc::clone(&entry),
                cancel_token: cancel_token.clone(),
                armed: true,
            };
        }

        assert!(
            !lock_inflight_map(&inflight).contains_key(&key),
            "aborted leader guard must remove inflight map entry"
        );
        assert!(
            lock_inflight_entry(&entry).completed,
            "aborted leader guard must mark inflight entry completed"
        );
        assert!(
            cancel_token.is_cancelled(),
            "aborted leader guard must cancel the leader token"
        );
        let err1 = rx1
            .try_recv()
            .expect("waiter 1 notified")
            .err()
            .expect("waiter 1 receives error");
        let err2 = rx2
            .try_recv()
            .expect("waiter 2 notified")
            .err()
            .expect("waiter 2 receives error");
        assert!(err1.to_string().contains("terminated unexpectedly"));
        assert!(err2.to_string().contains("terminated unexpectedly"));
    }

    #[test]
    fn fan_out_inflight_error_preserves_retryable_root_cause_for_all_waiters() {
        let (tx1, mut rx1) = oneshot::channel();
        let (tx2, mut rx2) = oneshot::channel();
        let original = anyhow!("IO Error: simulated transient GCS read error")
            .context("inspect shared workspace table");

        fan_out_inflight_result(vec![tx1, tx2], Err(original));

        for rx in [&mut rx1, &mut rx2] {
            let err = rx
                .try_recv()
                .expect("waiter notified")
                .err()
                .expect("waiter receives error");
            assert_eq!(err.to_string(), "inspect shared workspace table");
            assert_eq!(
                err.root_cause().to_string(),
                "IO Error: simulated transient GCS read error"
            );
            let kind = crate::api::error::classify_storage_error_chain(&err);
            assert_eq!(kind.code, "query_unavailable");
            assert!(
                kind.retryable,
                "coalesced follower must preserve retryable 503 classification"
            );
        }
    }
}
