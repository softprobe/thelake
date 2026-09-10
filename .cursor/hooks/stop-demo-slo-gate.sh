#!/usr/bin/env bash
# Cursor stop hook: do not let the agent finish until the demo SLO gate is green.
# stdout must be JSON only. Diagnostics go to stderr and the state log.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

STATE_DIR="$ROOT/.cursor/hooks/.state"
LOG="$STATE_DIR/last-stop-gate.log"
FAILS="$STATE_DIR/failures.txt"
PY="$ROOT/.cursor/hooks/grafana_dashboard_slo.py"
GRAFANA_STATE="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
PID_FILE="$GRAFANA_STATE/softprobe.pid"
WRITE_PID_FILE="$GRAFANA_STATE/softprobe-write.pid"
READ_PID_FILE="$GRAFANA_STATE/softprobe-read.pid"
INGEST_URL="${SOFTPROBE_INGEST_URL:-http://127.0.0.1:8091}"
QUERY_URL="${SOFTPROBE_QUERY_URL:-http://127.0.0.1:8090}"
OTEL_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"
# Never honor skip env vars. The gate is the product stop condition.

mkdir -p "$STATE_DIR"
: >"$FAILS"

log() { printf '%s\n' "$*" | tee -a "$LOG" >&2; }
log "===== $(date -u +%Y-%m-%dT%H:%M:%SZ) stop-demo-slo-gate ====="

read_status() {
  python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    d={}
print(d.get("status") or "")
print(d.get("loop_count") if d.get("loop_count") is not None else 0)
'
}

input="$(cat || true)"
status="$(printf '%s' "$input" | read_status | sed -n '1p')"
loop_count="$(printf '%s' "$input" | read_status | sed -n '2p')"
log "status=${status:-?} loop_count=${loop_count:-0}"

if [[ "${status}" == "aborted" ]]; then
  echo '{}'
  exit 0
fi

fail() {
  printf '%s\n' "$1" >>"$FAILS"
  log "FAIL: $1"
}

# --- 1. working tree committed ---
dirty="$(git status --porcelain | grep -vE '^\?\? \.codegraph/|^\?\? \.agent/|^\?\? \.cursor/hooks/\.state/' || true)"
if [[ -n "$dirty" ]]; then
  preview="$(printf '%s\n' "$dirty" | head -n 40)"
  fail "Code is not committed. Dirty paths:
$preview"
else
  log "git: clean at $(git rev-parse --short HEAD)"
fi

# --- 2. OTEL demo running ---
grafana_up=0
if docker inspect -f '{{.State.Running}}' thelake-grafana-manual 2>/dev/null | grep -qx true; then
  grafana_up=1
fi
otel_n="$(docker ps --filter "label=com.docker.compose.project=${OTEL_PROJECT}" --filter status=running --format '{{.Names}}' 2>/dev/null | wc -l | tr -d ' ' || echo 0)"
otel_collector="$(docker ps --filter "label=com.docker.compose.project=${OTEL_PROJECT}" --filter status=running --format '{{.Names}}' 2>/dev/null | grep -Ei 'otel-collector|collector' || true)"

softprobe_ok=0
if curl -sf -m 2 "${QUERY_URL}/api/v1/status/buildinfo" >/dev/null 2>&1 \
  || curl -sf -m 2 -H "Authorization: Bearer ${SOFTPROBE_API_KEY:-local-dev-key}" \
       "${QUERY_URL}/api/v1/label/__name__/values" >/dev/null 2>&1; then
  softprobe_ok=1
fi
if curl -sf -m 2 "${INGEST_URL}/ready" >/dev/null 2>&1; then
  softprobe_ok=1
fi

if [[ "$grafana_up" != 1 && "$softprobe_ok" != 1 ]]; then
  log "demo not running; skip stop gate (workspace is not on a live Grafana session)"
  echo '{}'
  exit 0
fi

if [[ "$grafana_up" != 1 ]]; then
  fail "Grafana demo is not running (container thelake-grafana-manual). Start with: make grafana-up"
fi
if [[ "${otel_n:-0}" -lt 1 || -z "$otel_collector" ]]; then
  fail "OpenTelemetry Demo is not running (compose project ${OTEL_PROJECT}, need otel-collector). Start with: make grafana-up"
fi
if [[ "$softprobe_ok" != 1 ]]; then
  log "slo: Softprobe not detected on query :8090 / ingest :8091 yet (will retry after helper init)"
fi

# --- 3. full-fidelity live stack + PromQL SLO ---
# Success criteria: Softprobe %CPU <100 under concurrent full OTLP + Grafana
# refresh; ops online; PromQL ≤100ms may use a short isolated measure window,
# then full ingest must recover without Softprobe bounce on the happy path.

grafana_paused=0
collector_stopped=0

unpause_grafana() {
  if [[ "${grafana_paused}" == 1 ]]; then
    docker unpause thelake-grafana-manual >/dev/null 2>&1 || true
    grafana_paused=0
    log "slo: unpaused thelake-grafana-manual"
  fi
}

ensure_grafana_running() {
  local st
  st="$(docker inspect -f '{{.State.Status}}' thelake-grafana-manual 2>/dev/null || true)"
  if [[ "$st" == "paused" ]]; then
    docker unpause thelake-grafana-manual >/dev/null 2>&1 || true
    grafana_paused=0
  fi
}

force_recreate_collector() {
  local overlay demo_dir rc
  overlay="$ROOT/tests/compat/grafana/otel-demo"
  demo_dir="${OTEL_DEMO_DIR:-$HOME/.cache/thelake/otel-demo/${OTEL_DEMO_TAG:-3.0.0}}"
  if [[ -d "$demo_dir" && -f "$overlay/otelcol-config-extras.yml" && -f "$overlay/compose.softprobe.yaml" ]]; then
    log "slo: force-recreating otel-collector for clean OTLP recovery"
    rc=0
    (
      cd "$demo_dir" && \
        DEMO_VERSION="${OTEL_DEMO_TAG:-3.0.0}" IMAGE_VERSION="${OTEL_DEMO_TAG:-3.0.0}" \
        OTEL_COLLECTOR_CONFIG_EXTRAS="$overlay/otelcol-config-extras.yml" \
        docker compose -p "${OTEL_PROJECT:-thelake-otel-demo}" --env-file .env \
          -f compose.yaml -f "$overlay/compose.softprobe.yaml" \
          up -d --force-recreate otel-collector
    ) >>"$LOG" 2>&1 || rc=$?
    if [[ "$rc" -ne 0 ]]; then
      log "slo: compose recreate failed (rc=$rc); trying docker start"
      docker start otel-collector >/dev/null 2>&1 || true
    fi
    # Bare recreate without compose.softprobe.yaml drops host.docker.internal
    # and OTLP to Softprobe silently dies while debug exporter still looks busy.
    if ! docker inspect -f '{{json .HostConfig.ExtraHosts}}' otel-collector 2>/dev/null \
      | grep -q 'host.docker.internal'; then
      log "slo: otel-collector missing host.docker.internal; re-applying softprobe overlay"
      (
        cd "$demo_dir" && \
          DEMO_VERSION="${OTEL_DEMO_TAG:-3.0.0}" IMAGE_VERSION="${OTEL_DEMO_TAG:-3.0.0}" \
          OTEL_COLLECTOR_CONFIG_EXTRAS="$overlay/otelcol-config-extras.yml" \
          docker compose -p "${OTEL_PROJECT:-thelake-otel-demo}" --env-file .env \
            -f compose.yaml -f "$overlay/compose.softprobe.yaml" \
            up -d --force-recreate otel-collector
      ) >>"$LOG" 2>&1 || true
    fi
  else
    docker start otel-collector >/dev/null 2>&1 || true
  fi
  log "slo: otel-collector up"
}

restart_collector() {
  if [[ "${collector_stopped:-0}" != 1 ]]; then
    return 0
  fi
  force_recreate_collector
  collector_stopped=0
}

restart_softprobe_demo() {
  local bin cfg auth_url duck_lib
  bin="$GRAFANA_STATE/softprobe-runtime"
  if [[ ! -x "$bin" && -x "$ROOT/dist/softprobe-runtime" ]]; then
    cp -f "$ROOT/dist/softprobe-runtime" "$bin"
    chmod +x "$bin"
  fi
  cfg="$GRAFANA_STATE/config.yaml"
  cfg_write="$GRAFANA_STATE/config-write.yaml"
  cfg_query="$GRAFANA_STATE/config-query.yaml"
  if [[ ! -f "$cfg_write" ]]; then
    cfg_write="$cfg"
  fi
  if [[ ! -f "$cfg_query" ]]; then
    cfg_query="$cfg"
  fi
  if [[ ! -x "$bin" || ! -f "$cfg" ]]; then
    log "slo: softprobe restart skipped (missing $bin or $cfg)"
    return 1
  fi
  duck_lib=""
  if [[ -f "$GRAFANA_STATE/libduckdb.so" ]]; then
    duck_lib="$GRAFANA_STATE"
  elif [[ -f "$ROOT/dist/libduckdb.so" ]]; then
    cp -f "$ROOT/dist/libduckdb.so" "$GRAFANA_STATE/libduckdb.so" 2>/dev/null || true
    duck_lib="$GRAFANA_STATE"
  else
    duck_lib="$(find "${CARGO_TARGET_DIR:-$ROOT/target}/duckdb-download" -type f -name 'libduckdb.so*' -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
  fi
  if [[ -z "$duck_lib" ]]; then
    log "slo: softprobe restart skipped (libduckdb.so not found)"
    return 1
  fi

  stop_pidfile() {
    local pf="$1"
    local pid
    pid="$(tr -d '[:space:]' <"$pf" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      log "slo: stopping Softprobe pid=$pid ($pf)"
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 40); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
  }
  stop_pidfile "$WRITE_PID_FILE"
  stop_pidfile "$READ_PID_FILE"
  stop_pidfile "$PID_FILE"

  auth_url="${SOFTPROBE_AUTH_URL:-http://127.0.0.1:18080/validate}"
  start_role() {
    local role="$1" listen="$2" logf="$3" pidf="$4" cfgf="$5"
    local -a run_cmd
    : >"$logf"
    run_cmd=(env
      "SOFTPROBE_AUTH_URL=$auth_url"
      "SOFTPROBE_ADMIN_API_KEY=${SOFTPROBE_ADMIN_API_KEY:-local-dev-admin-key}"
      "SOFTPROBE_GRPC_DISABLE=1"
      "SOFTPROBE_HTTP_ROLE=$role"
      "SOFTPROBE_LISTEN_ADDR=$listen"
      "RUST_LOG=${RUST_LOG:-info}"
      "CONFIG_FILE=$cfgf"
      "LD_LIBRARY_PATH=${duck_lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    )
    if [[ -n "${THELAKE_CPU_AFFINITY:-}" ]] && command -v taskset >/dev/null 2>&1; then
      run_cmd+=(taskset -c "$THELAKE_CPU_AFFINITY")
      log "slo: Softprobe $role CPU affinity=$THELAKE_CPU_AFFINITY"
    fi
    run_cmd+=("$bin")
    if command -v setsid >/dev/null 2>&1; then
      setsid "${run_cmd[@]}" >>"$logf" 2>&1 &
      echo $! >"$pidf"
    else
      "${run_cmd[@]}" >>"$logf" 2>&1 &
      echo $! >"$pidf"
    fi
  }
  start_role ingest "0.0.0.0:8091" "$GRAFANA_STATE/softprobe-write.log" "$WRITE_PID_FILE" "$cfg_write"
  start_role query "0.0.0.0:8090" "$GRAFANA_STATE/softprobe-read.log" "$READ_PID_FILE" "$cfg_query"
  cp -f "$READ_PID_FILE" "$PID_FILE"

  local ok=0
  for _ in $(seq 1 60); do
    if curl -sf "${QUERY_URL}/ready" >/dev/null 2>&1 \
      && curl -sf "${INGEST_URL}/ready" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 0.5
  done
  if [[ "$ok" != 1 ]]; then
    log "slo: Softprobe dual-process did not become ready after restart"
    tail -40 "$GRAFANA_STATE/softprobe-write.log" | tee -a "$LOG" >&2 || true
    tail -40 "$GRAFANA_STATE/softprobe-read.log" | tee -a "$LOG" >&2 || true
    return 1
  fi
  log "slo: Softprobe restarted write=$(tr -d '[:space:]' <"$WRITE_PID_FILE") read=$(tr -d '[:space:]' <"$READ_PID_FILE")"
}

# Resolve write + read Softprobe pids (dual-process demo; legacy single pid fallback).
softprobe_pids() {
  local write_pid read_pid
  write_pid="$(tr -d '[:space:]' <"$WRITE_PID_FILE" 2>/dev/null || true)"
  read_pid="$(tr -d '[:space:]' <"$READ_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$write_pid" ]] && kill -0 "$write_pid" 2>/dev/null \
    && [[ -n "$read_pid" ]] && kill -0 "$read_pid" 2>/dev/null; then
    printf '%s %s' "$write_pid" "$read_pid"
    return 0
  fi
  local pid
  pid="$(tr -d '[:space:]' <"$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    printf '%s' "$pid"
    return 0
  fi
  pgrep -f 'softprobe-runtime' 2>/dev/null | head -n2 | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

# Live-stack CPU probe: each Softprobe process (write + read) must stay
# avg<100 and p95<100 under concurrent OTLP + Grafana + paced PromQL.
probe_live_cpu() {
  local pids load_pid samples_file
  ensure_grafana_running
  if ! docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true; then
    force_recreate_collector
  fi
  pids="$(softprobe_pids)"
  if [[ -z "$pids" ]]; then
    fail "live CPU probe: Softprobe pid(s) not found"
    return 1
  fi
  samples_file="$STATE_DIR/live-cpu-samples.txt"
  log "slo: live CPU probe (60s / 3s /proc deltas, pids=$pids, collector+Grafana+PromQL load)"
  python3 "$PY" --load-cpu --duration-s 65 --workers 1 >>"$LOG" 2>&1 &
  load_pid=$!
  local cpu_rc=0
  # shellcheck disable=SC2086
  python3 - "$samples_file" $pids >>"$LOG" 2>&1 <<'PY' || cpu_rc=$?
import os, sys, time
from pathlib import Path

out_path = Path(sys.argv[1])
pids = [p for p in sys.argv[2:] if p]
hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

def cpu_ticks(p: str) -> int:
    fields = Path(f"/proc/{p}/stat").read_text().split()
    return int(fields[13]) + int(fields[14])

if len(pids) >= 2:
    labels = {pids[0]: "write", pids[1]: "read"}
else:
    labels = {pids[0]: "softprobe"}

prev = {p: cpu_ticks(p) for p in pids}
prev_t = time.time()
samples = {p: [] for p in pids}
time.sleep(3.0)
for _ in range(20):
    now_t = time.time()
    dt = max(now_t - prev_t, 1e-6)
    for p in pids:
        now = cpu_ticks(p)
        pct = ((now - prev[p]) / hz) / dt * 100.0
        samples[p].append(pct)
        prev[p] = now
    prev_t = now_t
    time.sleep(3.0)

lines = []
failed = False
for p in pids:
    vals = samples[p]
    lines.append(f"# {labels[p]} pid={p}")
    lines.extend(f"{v:.3f}" for v in vals)
    if len(vals) < 15:
        print(f"live CPU probe FAILED ({labels[p]}): only {len(vals)} samples", file=sys.stderr)
        failed = True
        continue
    vals_sorted = sorted(vals)
    avg = sum(vals) / len(vals)
    idx = min(len(vals_sorted) - 1, max(0, int(0.95 * (len(vals_sorted) - 1))))
    p95 = vals_sorted[idx]
    print(
        f"live CPU probe {labels[p]} (instantaneous 3s): n={len(vals)} avg={avg:.1f} p95={p95:.1f} max={max(vals):.1f}",
        file=sys.stderr,
    )
    if avg >= 100.0 or p95 >= 100.0:
        print(
            f"live CPU probe FAILED ({labels[p]}): need avg<100 and p95<100 (got avg={avg:.1f} p95={p95:.1f})",
            file=sys.stderr,
        )
        failed = True
out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
if failed:
    raise SystemExit(1)
print("live CPU probe ok (all Softprobe processes)", file=sys.stderr)
PY
  wait "$load_pid" 2>/dev/null || true
  tail -n 12 "$LOG" >&2 || true
  return "$cpu_rc"
}

check_ops_tenant() {
  local body
  body="$(curl -sf -m 15 \
    -H "Authorization: Bearer ${SOFTPROBE_OPS_API_KEY:-local-ops-key}" \
    -H "X-Scope-OrgID: thelake-ops" \
    "${QUERY_URL}/api/v1/label/__name__/values" 2>/dev/null || true)"
  if [[ -z "$body" ]] || [[ "$body" != *'"status":"success"'* ]]; then
    fail "ops label values failed (Bearer local-ops-key + X-Scope-OrgID: thelake-ops)"
    return 1
  fi
  if ! python3 -c 'import json,sys
d=json.loads(sys.argv[1])
names=d.get("data") or []
ok=isinstance(names,list) and any(str(n).startswith("thelake_") for n in names)
raise SystemExit(0 if ok else 1)' "$body"; then
    fail "ops tenant has no thelake_* metric names (self_monitoring must be on)"
    return 1
  fi
  log "slo: ops tenant OK (thelake_* present)"
}

check_loki_labels() {
  local end_ns start_ns body
  end_ns="$(python3 -c 'import time; print(int(time.time()*1e9))')"
  start_ns="$((end_ns - 3600 * 1000000000))"
  body="$(curl -sf -m 15 \
    -H "Authorization: Bearer ${SOFTPROBE_API_KEY:-local-dev-key}" \
    -H "X-Scope-OrgID: ${GRAFANA_TENANT_ID:-local-dev-tenant}" \
    "${QUERY_URL}/loki/api/v1/labels?start=$start_ns&end=$end_ns" 2>/dev/null || true)"
  if [[ -z "$body" ]] || [[ "$body" != *'"status":"success"'* ]] \
    || [[ "$body" == *'"data":[]'* ]]; then
    fail "Loki labels empty in last hour (full OTLP logs required)"
    return 1
  fi
  log "slo: Loki labels OK"
}

# Ensure Softprobe + collector before live checks.
if ! curl -sf -m 2 "${QUERY_URL}/ready" >/dev/null 2>&1; then
  log "slo: Softprobe not ready; attempting demo restart"
  restart_softprobe_demo || fail "Softprobe query/ingest not serving on :8090/:8091. Start with: make grafana-up"
fi
ensure_grafana_running
if ! docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true; then
  log "slo: starting otel-collector before live checks"
  force_recreate_collector
fi

log "slo: pre-warmup ingest check (full stack)"
ingest_ok=0
ingest_out=""
for ingest_try in 1 2 3 4 5 6; do
  ingest_out="$(python3 "$PY" --check-ingest 2>&1)" || true
  printf '%s\n' "$ingest_out" | tee -a "$LOG" >&2
  if grep -q "ingest ok" <<<"$ingest_out"; then
    ingest_ok=1
    break
  fi
  log "slo: ingest not ready (try ${ingest_try}/6); waiting 20s"
  sleep 20
done
if [[ "$ingest_ok" != 1 ]]; then
  fail "OTEL ingest is not live before Grafana warmup (see $LOG)"
fi

check_ops_tenant
check_loki_labels

# CPU budget under concurrent full stack (must not depend on pausing Grafana).
if ! probe_live_cpu; then
  fail "Softprobe live-stack CPU budget failed (need each Softprobe process 60s avg<100 and p95<100 with collector+Grafana online)"
fi

# Short isolated PromQL measure (warmup + cache hits). Recover full ingest after;
# Softprobe bounce is last-resort only.
trap 'restart_collector; unpause_grafana' EXIT
if docker inspect -f '{{.State.Status}}' thelake-grafana-manual 2>/dev/null | grep -qx running; then
  if docker pause thelake-grafana-manual >/dev/null 2>&1; then
    grafana_paused=1
    log "slo: paused Grafana for PromQL measure only"
  fi
fi
if docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true; then
  if docker stop otel-collector >/dev/null 2>&1; then
    collector_stopped=1
    log "slo: stopped otel-collector for PromQL measure only"
  fi
fi
if docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true; then
  fail "otel-collector still running during SLO measure; refuse to continue"
fi

log "slo: global warmup"
if ! python3 "$PY" --warmup-all --timeout-s 60 >>"$LOG" 2>&1; then
  fail "Grafana global warmup failed (see $LOG)"
fi
log "slo: global warmup ok"

slo_rc=1
slo_out=""
for attempt in 1 2 3; do
  log "slo: measured pass attempt ${attempt}"
  if docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true; then
    docker stop otel-collector >/dev/null 2>&1 || true
    collector_stopped=1
  fi
  slo_rc=0
  slo_out="$(python3 "$PY" --slo-ms 100 --repeats 3 --workers 1 --skip-ingest 2>&1)" || slo_rc=$?
  printf '%s\n' "$slo_out" | tee -a "$LOG" >&2
  if [[ "$slo_rc" -eq 0 ]]; then
    break
  fi
  log "slo: attempt ${attempt} failed; retrying after short pause"
  sleep 2
done
if [[ "$slo_rc" -ne 0 ]]; then
  fail "Grafana SLO (every dashboard at 5m, 15m, 30m, 1h, 3h, 24h, 30d, 180d consistently ≤100ms) failed:
$slo_out"
fi

restart_collector
unpause_grafana

# Happy path: no Softprobe bounce — recreate collector + restart loadgen only.
log "slo: idle pause for OTLP after measure"
sleep 15
if docker inspect -f '{{.State.Running}}' load-generator >/dev/null 2>&1; then
  log "slo: restarting load-generator for fresh k6 counters"
  docker restart load-generator >/dev/null 2>&1 || true
fi
collector_stopped=1
restart_collector
# Collector batch timeout (30s) + Softprobe coalesce flush (60s) + one scrape.
sleep 90
log "slo: post-measure ingest check"
ingest_ok=0
for ingest_try in $(seq 1 16); do
  ingest_out="$(python3 "$PY" --check-ingest 2>&1)" || true
  printf '%s\n' "$ingest_out" | tee -a "$LOG" >&2
  if grep -q "ingest ok" <<<"$ingest_out"; then
    ingest_ok=1
    break
  fi
  if [[ "$ingest_try" -eq 8 ]]; then
    log "slo: ingest still stale; heal Softprobe + collector once (last resort)"
    restart_softprobe_demo || true
    collector_stopped=1
    restart_collector
    docker restart load-generator >/dev/null 2>&1 || true
    sleep 90
  else
    sleep 15
  fi
done
if [[ "$ingest_ok" != 1 ]]; then
  fail "OTEL ingest did not recover after Grafana SLO measure (see $LOG)"
fi
# Re-check full-fidelity signals after recovery.
check_ops_tenant
check_loki_labels
trap - EXIT

# --- 4. tests green (committed tree only; cache by HEAD) ---
if [[ -n "$dirty" ]]; then
  fail "Tests not verified: commit the working tree first, then this hook will run make test."
else
  head_sha="$(git rev-parse HEAD)"
  cache="$STATE_DIR/last-green-test"
  cached="$(cat "$cache" 2>/dev/null || true)"
  if [[ "$cached" == "$head_sha" ]]; then
    log "tests: cached green for $head_sha"
  else
    log "tests: running make test"
    if make test >>"$LOG" 2>&1; then
      printf '%s\n' "$head_sha" >"$cache"
      log "tests: green"
    else
      tail_txt="$(tail -n 80 "$LOG")"
      fail "make test is not green:
$tail_txt"
    fi
  fi
fi

if [[ ! -s "$FAILS" ]]; then
  log "stop gate passed"
  python3 - "$STATE_DIR/last-pass.json" <<'PY'
import json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
head = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
Path(sys.argv[1]).write_text(
    json.dumps(
        {
            "passed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "head": head,
            "measure": "consecutive-v1",
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY
  echo '{}'
  exit 0
fi

python3 - "$FAILS" <<'PY'
import json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
fails = Path(sys.argv[1]).read_text(encoding="utf-8")
head = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
msg = f"""STOP GATE FAILED. Do not stop.

gate_run_utc={run_id} head={head} measure=consecutive-v1
(Ignore older followups that say "OTEL ingest and/or" or list samples_ms values >100ms — those are pre-consecutive measure runs.)

All of these must be true before you may finish:
1. All tests green (`make test`).
2. Code committed (clean git status, ignoring .codegraph/ and hook state).
3. OTEL Astronomy Shop demo running with full-fidelity ingest (metrics+logs+traces; live non-flat scrapes; Loki labels; ops `thelake_*` with self_monitoring on).
4. Softprobe write+read processes live-stack CPU: each 60s avg %CPU <100 and p95 <100 with collector up and Grafana unpaused (no taskset cheat required).
5. Every Grafana dashboard PromQL at 5m, 15m, 30m, 1h, 3h, 24h, 30d, 180d consistently ≤100ms (3 consecutive repeats after warmup; short isolated measure OK — recover full ingest without Softprobe bounce on happy path).

Failures this turn:
{fails}

If Grafana latency, CPU, or ingest failed: go read and learn from ../greptime (workspace sibling; also ./greptime if present). Study how Greptime serves PromQL range queries (storage layout, indexing, aggregation, compaction). Apply those lessons to Softprobe. Do not vendor or fork Greptime. Do not reintroduce metrics allow-lists or self-mon-off as the CPU budget. Then re-measure until this hook returns success.

Keep working. Do not stop until this hook returns success.
"""
print(json.dumps({"followup_message": msg}))
PY
exit 0
