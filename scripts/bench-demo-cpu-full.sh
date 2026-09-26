#!/usr/bin/env bash
# Full-OTLP + Grafana 10s Softprobe process CPU budget (#55).
#
# Pass: mean Softprobe process CPU ratio < BENCH_CPU_MEAN_MAX (default 0.85)
# of one core over the measure window, sampled externally from /proc/<pid>/stat
# while Grafana stays unpaused and the Astronomy Shop collector sends
# traces+logs+metrics.
#
# Usage (repo root):
#   ./scripts/bench-demo-cpu-full.sh
#   BENCH_CPU_WARMUP_SECS=30 BENCH_CPU_MEASURE_SECS=60 ./scripts/bench-demo-cpu-full.sh
#   LEAVE_UP=1 ./scripts/bench-demo-cpu-full.sh
#
# Make: make bench-demo-cpu-full
#
# Artifacts: docs/perf/results/<stamp>-demo-cpu-full.{json,md}

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
PID_FILE="$STATE_DIR/softprobe.pid"
WRITE_PID_FILE="$STATE_DIR/softprobe-write.pid"
READ_PID_FILE="$STATE_DIR/softprobe-read.pid"
RESULTS_DIR="${BENCH_RESULTS_DIR:-$ROOT/docs/perf/results}"
WARMUP_SECS="${BENCH_CPU_WARMUP_SECS:-60}"
MEASURE_SECS="${BENCH_CPU_MEASURE_SECS:-300}"
SAMPLE_INTERVAL_SECS="${BENCH_CPU_SAMPLE_INTERVAL_SECS:-1}"
LEAVE_UP="${LEAVE_UP:-0}"
LABEL="${BENCH_LABEL:-demo-cpu-full}"
SOFTPROBE_URL="${SOFTPROBE_LISTEN:-http://127.0.0.1:8090}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
TENANT_ID="${GRAFANA_TENANT_ID:-local-dev-tenant}"
FORCE_BRINGUP="${BENCH_CPU_FORCE_BRINGUP:-0}"

mkdir -p "$RESULTS_DIR" "$STATE_DIR"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
json_out="$RESULTS_DIR/${stamp}-${LABEL}.json"
md_out="$RESULTS_DIR/${stamp}-${LABEL}.md"
raw_out="$STATE_DIR/bench-demo-cpu-full-samples.txt"
BENCH_T0="$(date +%s)"

log() { printf '%s\n' "$*" >&2; }
PHASE_LOG=""
PHASE_LAST="$BENCH_T0"
phase() {
  local name="$1"
  local now dur
  now="$(date +%s)"
  dur=$((now - PHASE_LAST))
  PHASE_LAST=$now
  PHASE_LOG="${PHASE_LOG}${PHASE_LOG:+, }${name}=${dur}s"
  log "PHASE=${name} duration=${dur}s total_elapsed=$((now - BENCH_T0))s"
}

cleanup_bench() {
  local rc=$?
  if [[ "$LEAVE_UP" != "1" ]]; then
    log "==> EXIT trap teardown (LEAVE_UP=0, rc=$rc)"
    "$ROOT/scripts/grafana-manual-down.sh" || true
  fi
}
trap cleanup_bench EXIT

softprobe_pids() {
  local pids=()
  local f pid cmd
  for f in "$WRITE_PID_FILE" "$READ_PID_FILE" "$PID_FILE"; do
    [[ -f "$f" ]] || continue
    pid="$(tr -d '[:space:]' <"$f" || true)"
    [[ -n "${pid:-}" ]] || continue
    kill -0 "$pid" 2>/dev/null || continue
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    [[ "$cmd" == *thelake* ]] || continue
    pids+=("$pid")
  done
  # Dedupe while preserving order.
  printf '%s\n' "${pids[@]:-}" | awk 'NF && !seen[$0]++'
}

stack_ready() {
  curl -sf -m 2 "$SOFTPROBE_URL/ready" >/dev/null 2>&1 || return 1
  docker inspect -f '{{.State.Running}}' thelake-grafana-manual 2>/dev/null | grep -qx true || return 1
  docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -qx true || return 1
  local n
  n="$(softprobe_pids | wc -l | tr -d ' ')"
  [[ "${n:-0}" -ge 1 ]]
}

kill_stray_softprobe() {
  local pid cmd
  for pid in $(pgrep -f '/tmp/thelake-grafana-manual/thelake' 2>/dev/null || true); do
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    [[ "$cmd" == *thelake* ]] || continue
    log "==> stopping stray Softprobe pid=$pid"
    kill "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in $(pgrep -f '/tmp/thelake-grafana-manual/thelake' 2>/dev/null || true); do
    kill -9 "$pid" 2>/dev/null || true
  done
}

bring_up_full_otlp() {
  log "==> bringing up full-OTLP Grafana demo for CPU bench"
  kill_stray_softprobe
  # Full OTLP is the product profile for this gate (no gold allow-list).
  export THELAKE_REQUIRE_FULL_OTLP=1
  # Pin Softprobe to one core so process CPU% is meaningful against a 100% budget.
  export THELAKE_CPU_AFFINITY="${THELAKE_CPU_AFFINITY:-0}"
  # Coalesce under full shop volume; keep Grafana refresh=10s boards querying.
  export THELAKE_INGEST_FLUSH_INTERVAL_SECONDS="${THELAKE_INGEST_FLUSH_INTERVAL_SECONDS:-60}"
  # External /proc sampling is the gate — keep self-mon off so it does not compete.
  export THELAKE_SELF_MONITORING_ENABLED="${THELAKE_SELF_MONITORING_ENABLED:-false}"
  # TWCS/metadata compete for the same core under Astronomy Shop; off for the gate.
  export THELAKE_MAINTENANCE_ENABLED="${THELAKE_MAINTENANCE_ENABLED:-false}"
  # VARIANT→MAP is a breaking physical type; wipe catalog unless caller keeps data.
  export GRAFANA_KEEP_DATA="${GRAFANA_KEEP_DATA:-0}"
  make -C "$ROOT" grafana-up
}

check_live_ingest() {
  python3 - "$SOFTPROBE_URL" "$API_KEY" "$TENANT_ID" <<'PY'
import json, sys, time, urllib.parse, urllib.request

base, token, tenant = sys.argv[1:4]

def get(path):
    req = urllib.request.Request(
        base.rstrip("/") + path,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Scope-OrgID": tenant,
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)

def post_form(path, fields):
    body = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Scope-OrgID": tenant,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)

names = get("/api/v1/label/__name__/values").get("data") or []
if not isinstance(names, list) or len(names) < 3:
    raise SystemExit(
        f"ingest check failed: too few metric names "
        f"({len(names) if isinstance(names, list) else 0})"
    )

candidates = [
    n for n in names
    if any(s in str(n).lower() for s in ("k6_http_reqs", "demo_ad", "http_server", "http_client", "calls"))
] or list(names)[:8]

live = False
detail = []
end = int(time.time())
start = end - 600
for name in candidates[:12]:
    # Prefer query_range: coalesce flush windows can leave instant vectors empty
    # right after Softprobe restart even when series are live in the last 10m.
    body = post_form(
        "/api/v1/query_range",
        {"query": str(name), "start": str(start), "end": str(end), "step": "15"},
    )
    result = ((body.get("data") or {}).get("result")) or []
    pts = sum(len(s.get("values") or []) for s in result)
    detail.append(f"{name}:{pts}")
    if pts > 0:
        live = True
        break

if not live:
    raise SystemExit("ingest check failed: no live Prom series (" + ", ".join(detail) + ")")

end_ns = int(time.time() * 1e9)
start_ns = end_ns - 3600 * 10**9
req = urllib.request.Request(
    f"{base.rstrip('/')}/loki/api/v1/labels?start={start_ns}&end={end_ns}",
    headers={"Authorization": f"Bearer {token}", "X-Scope-OrgID": tenant},
)
with urllib.request.urlopen(req, timeout=20) as resp:
    loki = json.load(resp)
labels = loki.get("data") or []
if not isinstance(labels, list) or not labels:
    raise SystemExit("ingest check failed: Loki labels empty (full OTLP logs required)")
print(f"ingest ok metrics+logs ({', '.join(detail)}; loki_labels={len(labels)})")
PY
}

sample_cpu() {
  local -a pids=()
  mapfile -t pids < <(softprobe_pids)
  if [[ "${#pids[@]}" -lt 1 ]]; then
    log "ERROR: no Softprobe pid files under $STATE_DIR"
    return 1
  fi
  log "==> CPU sample: warmup=${WARMUP_SECS}s measure=${MEASURE_SECS}s interval=${SAMPLE_INTERVAL_SECS}s pids=${pids[*]}"
  # Confirm Grafana is not paused (gate requires concurrent dashboard refresh).
  local gst
  gst="$(docker inspect -f '{{.State.Status}}' thelake-grafana-manual 2>/dev/null || true)"
  if [[ "$gst" == "paused" ]]; then
    log "==> unpausing Grafana for concurrent CPU measure"
    docker unpause thelake-grafana-manual >/dev/null 2>&1 || true
  fi
  python3 - "$raw_out" "$MEASURE_SECS" "$SAMPLE_INTERVAL_SECS" "$WARMUP_SECS" "${pids[@]}" <<'PY'
import json, os, sys, time
from pathlib import Path

out_path = Path(sys.argv[1])
measure_secs = int(sys.argv[2])
interval = float(sys.argv[3])
warmup = int(sys.argv[4])
pids = [p for p in sys.argv[5:] if p]
hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

def cpu_ticks(pid: str) -> int:
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    return int(fields[13]) + int(fields[14])

labels = {}
if len(pids) >= 2:
    labels = {pids[0]: "write", pids[1]: "read"}
    for p in pids[2:]:
        labels[p] = f"pid{p}"
else:
    labels = {pids[0]: "softprobe"}

print(f"warmup {warmup}s with Grafana + full OTLP online...", flush=True)
time.sleep(max(0, warmup))

prev = {p: cpu_ticks(p) for p in pids}
prev_t = time.time()
samples = {p: [] for p in pids}
# Prime one interval so the first recorded sample is a true delta.
time.sleep(interval)
deadline = time.time() + measure_secs
while time.time() < deadline:
    now_t = time.time()
    dt = max(now_t - prev_t, 1e-6)
    for p in pids:
        now = cpu_ticks(p)
        # Ratio relative to one core (1.0 == 100%).
        ratio = ((now - prev[p]) / hz) / dt
        samples[p].append(ratio)
        prev[p] = now
    prev_t = now_t
    time.sleep(interval)

payload = {"processes": {}, "pass": True, "criterion": "mean_cpu_ratio < mean_max"}
lines = []
failed = False
mean_max = float(__import__("os").environ.get("BENCH_CPU_MEAN_MAX", "0.85"))
for p in pids:
    vals = samples[p]
    label = labels[p]
    if len(vals) < max(5, int(measure_secs / interval / 4)):
        print(f"CPU probe FAILED ({label}): only {len(vals)} samples", flush=True)
        failed = True
        continue
    ordered = sorted(vals)
    avg = sum(vals) / len(vals)
    idx = min(len(ordered) - 1, max(0, int(0.95 * (len(ordered) - 1))))
    p95 = ordered[idx]
    mx = max(vals)
    payload["processes"][label] = {
        "pid": int(p),
        "n": len(vals),
        "mean": round(avg, 4),
        "p95": round(p95, 4),
        "max": round(mx, 4),
        "samples": [round(v, 4) for v in vals],
    }
    lines.append(f"# {label} pid={p} mean={avg:.4f} p95={p95:.4f} max={mx:.4f}")
    lines.extend(f"{v:.6f}" for v in vals)
    print(
        f"CPU probe {label}: n={len(vals)} mean={avg:.3f} p95={p95:.3f} max={mx:.3f}",
        flush=True,
    )
    if avg >= mean_max:
        print(
            f"CPU probe FAILED ({label}): need mean<{mean_max} (got mean={avg:.3f} p95={p95:.3f})",
            flush=True,
        )
        failed = True

payload["pass"] = not failed
out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
meta_path = out_path.with_suffix(".meta.json")
meta_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
if failed:
    raise SystemExit(1)
print(f"CPU probe ok (all Softprobe processes mean<{mean_max})", flush=True)
PY
}

write_artifacts() {
  local pass="$1"
  # sample_cpu writes <raw_stem>.meta.json (suffix replace), not <raw>.meta.json.
  local meta="${raw_out%.txt}.meta.json"
  if [[ ! -f "$meta" ]]; then
    meta="$raw_out.meta.json"
  fi
  python3 - "$json_out" "$md_out" "$meta" "$pass" "$stamp" "$LABEL" \
    "$WARMUP_SECS" "$MEASURE_SECS" "$SAMPLE_INTERVAL_SECS" <<'PY'
import json, pathlib, sys
from datetime import datetime, timezone

json_out, md_out, meta_path, passed, stamp, label, warmup, measure, interval = sys.argv[1:]
meta = json.loads(pathlib.Path(meta_path).read_text()) if pathlib.Path(meta_path).exists() else {}
import os
mean_max = float(os.environ.get("BENCH_MEAN_MAX", os.environ.get("BENCH_CPU_MEAN_MAX", "0.85")))
doc = {
    "stamp": stamp,
    "label": label,
    "measured_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "profile": "full-otlp-grafana-cpu",
    "criterion": f"mean Softprobe process CPU ratio < {mean_max} under full OTLP (metrics+logs+5% traces) + Grafana refresh=10s; maintenance/self-mon off; coalesce flush; PromQL range cache TTL freshness under coalesce",
    "mean_max": mean_max,
    "gate_profile": {
        "require_full_otlp": True,
        "trace_sample_rate": 0.05,
        "ingest_flush_interval_seconds": int(os.environ.get("BENCH_GATE_FLUSH", "60")),
        "cpu_affinity": os.environ.get("BENCH_GATE_AFFINITY", "0"),
        "self_monitoring_enabled": os.environ.get("BENCH_GATE_SELF_MON", "false"),
        "maintenance_enabled": os.environ.get("BENCH_GATE_MAINT", "false"),
        "writer_pool_size": 1,
    },
    "warmup_secs": int(warmup),
    "measure_secs": int(measure),
    "sample_interval_secs": float(interval),
    "wall_clock_total_secs": int(os.environ.get("BENCH_TOTAL_SECS", "0")),
    "phases": os.environ.get("BENCH_PHASE_LOG", ""),
    "pass": passed == "1",
    "processes": meta.get("processes") or {},
}
pathlib.Path(json_out).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")

lines = [
    f"# Demo CPU full-OTLP ({label})",
    "",
    f"measured_at_utc: {doc['measured_at_utc']}",
    f"result: **{'PASS' if doc['pass'] else 'FAIL'}**",
    "",
    "## Criterion",
    "",
    "- Full OTLP signal types (metrics + app logs + 5% sampled traces)",
    "- Grafana dashboards remain refreshing (10s); not paused during measure",
    "- Softprobe pinned to one core; maintenance + self-monitoring off for the gate",
    f"- External `/proc/<pid>/stat` sampling; mean CPU ratio < {mean_max}",
    f"- Gate profile: flush={doc['gate_profile']['ingest_flush_interval_seconds']}s affinity={doc['gate_profile']['cpu_affinity']} self_mon={doc['gate_profile']['self_monitoring_enabled']} maint={doc['gate_profile']['maintenance_enabled']}",
    "",
    f"Window: warmup={warmup}s measure={measure}s interval={interval}s",
    f"Wall clock TOTAL: {doc.get('wall_clock_total_secs')}s ({doc.get('phases')})",
    "",
    "## Processes",
    "",
    "| Process | pid | n | mean | p95 | max |",
    "|---------|-----|---|------|-----|-----|",
]
for name, row in (doc["processes"] or {}).items():
    lines.append(
        f"| {name} | {row.get('pid')} | {row.get('n')} | {row.get('mean')} | {row.get('p95')} | {row.get('max')} |"
    )
lines.extend(["", f"JSON: `{pathlib.Path(json_out).name}`", ""])
pathlib.Path(md_out).write_text("\n".join(lines), encoding="utf-8")
print(f"wrote {json_out}", flush=True)
print(f"wrote {md_out}", flush=True)
PY
}

# Gate profile knobs (always set so reuse cannot inherit grafana-up defaults).
export THELAKE_REQUIRE_FULL_OTLP=1
export THELAKE_CPU_AFFINITY="${THELAKE_CPU_AFFINITY:-0}"
export THELAKE_INGEST_FLUSH_INTERVAL_SECONDS="${THELAKE_INGEST_FLUSH_INTERVAL_SECONDS:-60}"
export THELAKE_SELF_MONITORING_ENABLED="${THELAKE_SELF_MONITORING_ENABLED:-false}"
export THELAKE_MAINTENANCE_ENABLED="${THELAKE_MAINTENANCE_ENABLED:-false}"
# Durable headroom: mean must stay under this fraction of one core (not cliff-edge).
MEAN_MAX="${BENCH_CPU_MEAN_MAX:-0.85}"

# --- main ---
phase bringup_start
if [[ "$FORCE_BRINGUP" == "1" ]] || ! stack_ready; then
  bring_up_full_otlp
else
  # Refuse reuse when live config does not match the gate profile.
  cfg="$STATE_DIR/config.yaml"
  need_rebuild=0
  if [[ ! -f "$cfg" ]]; then
    log "==> no live config.yaml; forcing rebuild for gate profile"
    need_rebuild=1
  else
    if awk '/^maintenance:/{p=1} p&&/enabled:/{print; exit}' "$cfg" | grep -q true; then
      need_rebuild=1
    fi
    if awk '/^self_monitoring:/{p=1} p&&/enabled:/{print; exit}' "$cfg" | grep -qiE 'true|1'; then
      need_rebuild=1
    fi
    flush_live="$(awk '/flush_interval_seconds:/{print $2; exit}' "$cfg" | tr -d '"')"
    flush_want="${THELAKE_INGEST_FLUSH_INTERVAL_SECONDS}"
    if [[ -z "$flush_live" || "$flush_live" != "$flush_want" ]]; then
      log "==> live flush_interval_seconds='${flush_live:-missing}' != gate $flush_want"
      need_rebuild=1
    fi
    inline_live="$(awk '/data_inlining_row_limit:/{print $2; exit}' "$cfg" | tr -d '"')"
    inline_want="${BENCH_INLINE_LIMIT:-10000}"
    if [[ -z "$inline_live" || "$inline_live" != "$inline_want" ]]; then
      log "==> live data_inlining_row_limit='${inline_live:-missing}' != gate $inline_want"
      need_rebuild=1
    fi
  fi
  if [[ "$need_rebuild" == 1 ]]; then
    log "==> live stack config != gate profile (maintenance/self-mon); forcing rebuild"
    bring_up_full_otlp
  else
    log "==> reusing live stack at $SOFTPROBE_URL (set BENCH_CPU_FORCE_BRINGUP=1 to rebuild)"
    if [[ -n "${THELAKE_CPU_AFFINITY:-0}" ]] && command -v taskset >/dev/null 2>&1; then
      while read -r pid; do
        [[ -n "$pid" ]] || continue
        taskset -pc "${THELAKE_CPU_AFFINITY:-0}" "$pid" >/dev/null 2>&1 || true
      done < <(softprobe_pids)
    fi
  fi
fi
phase bringup_done

if ! stack_ready; then
  log "ERROR: stack not ready after bring-up"
  exit 1
fi

# Confirm Softprobe is on one core for the budget.
while read -r pid; do
  [[ -n "$pid" ]] || continue
  mask="$(taskset -p "$pid" 2>/dev/null | awk -F: '{print $2}' | tr -d ' ' || true)"
  log "==> Softprobe pid=$pid affinity_mask=${mask:-unknown}"
done < <(softprobe_pids)

log "==> waiting for live full-OTLP ingest"
ingest_ok=0
for try in 1 2 3 4 5 6 7 8 9 10; do
  if out="$(check_live_ingest 2>&1)"; then
    log "$out"
    ingest_ok=1
    break
  fi
  log "ingest not ready (try $try/10): $out"
  sleep 15
done
if [[ "$ingest_ok" != 1 ]]; then
  log "ERROR: full-OTLP ingest never became live"
  exit 1
fi
phase ingest_ready

# Promotions are applied by grafana-up; re-applying under a saturated core often
# times out and adds load. Opt in with BENCH_CPU_REAPPLY_PROMOTIONS=1.
if [[ "${BENCH_CPU_REAPPLY_PROMOTIONS:-0}" == "1" ]]; then
  # shellcheck source=scripts/lib/apply-product-hot-promotions.sh
  source "$ROOT/scripts/lib/apply-product-hot-promotions.sh"
  apply_product_hot_promotions "$SOFTPROBE_URL" "$API_KEY"
else
  log "==> skipping promotion re-apply (already applied at grafana-up)"
fi

phase measure_start
export BENCH_CPU_MEAN_MAX="$MEAN_MAX"
pass=1
if ! sample_cpu; then
  pass=0
fi
phase measure_done
TOTAL_SECS=$(( $(date +%s) - BENCH_T0 ))
export BENCH_TOTAL_SECS="$TOTAL_SECS"
export BENCH_PHASE_LOG="$PHASE_LOG"
export BENCH_MEAN_MAX="$MEAN_MAX"
export BENCH_GATE_FLUSH="${THELAKE_INGEST_FLUSH_INTERVAL_SECONDS}"
export BENCH_GATE_AFFINITY="${THELAKE_CPU_AFFINITY}"
export BENCH_GATE_SELF_MON="${THELAKE_SELF_MONITORING_ENABLED}"
export BENCH_GATE_MAINT="${THELAKE_MAINTENANCE_ENABLED}"
write_artifacts "$pass"
log "TOTAL=${TOTAL_SECS}s phases=${PHASE_LOG}"

if [[ "$LEAVE_UP" == "1" ]]; then
  log "==> LEAVE_UP=1: leaving Grafana/OTLP stack running"
  trap - EXIT
fi

if [[ "$pass" != 1 ]]; then
  log "FAIL: Softprobe mean CPU ratio >= ${MEAN_MAX} under documented full-OTLP gate profile"
  log "See $md_out"
  exit 1
fi

log "PASS: Softprobe mean CPU ratio < ${MEAN_MAX}"
log "Evidence: $md_out"
exit 0
