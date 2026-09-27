#!/usr/bin/env python3
"""Holistic LLM/agent load client: concurrent session ingest + session HTTP APIs.

Measures p50/p95/p99 for:
  - agent session OTLP ingest (/v1/traces)
  - POST /v1/llm/sessions/search
  - GET  /v1/llm/sessions/{id} (full session detail and spans)
  - process CPU under load and after load stops (idle window)

Does not replace make stress / perf_stress — complementary product-shaped bench.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

# Allow `python3 scripts/perf/bench_llm_load.py` without installing a package.
_PERF_DIR = Path(__file__).resolve().parent
if str(_PERF_DIR) not in sys.path:
    sys.path.insert(0, str(_PERF_DIR))

from agent_otlp import build_agent_session  # noqa: E402
from percentiles import summarize  # noqa: E402
from process_cpu import resolve_pid, sample_window, summarize_cpu  # noqa: E402


@dataclass
class WorkloadStats:
    name: str
    latencies_ms: list[float] = field(default_factory=list)
    ok: int = 0
    errors: int = 0
    units: int = 0  # sessions or requests
    record_after: float = 0.0  # perf_counter deadline; samples before are dropped
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, ms: float, *, ok: bool, units: int = 1) -> None:
        if time.perf_counter() < self.record_after:
            return
        with self.lock:
            self.latencies_ms.append(ms)
            self.units += units
            if ok:
                self.ok += 1
            else:
                self.errors += 1

    def report(self, duration_s: float) -> dict[str, Any]:
        s = summarize(self.latencies_ms)
        qps = self.ok / duration_s if duration_s > 0 else 0.0
        return {
            "workload": self.name,
            "ok": self.ok,
            "errors": self.errors,
            "units": self.units,
            "achieved_qps": round(qps, 3),
            **s,
        }


class SessionPool:
    def __init__(self, ids: list[str]) -> None:
        self._ids = list(ids) if ids else []
        self._lock = threading.Lock()
        self._live: list[str] = []

    def add_live(self, session_id: str) -> None:
        with self._lock:
            self._live.append(session_id)
            if len(self._live) > 10_000:
                self._live = self._live[-5000:]

    def pick(self, hit_ratio: float = 0.8) -> str:
        with self._lock:
            pool = self._live + self._ids
        if pool and random.random() < hit_ratio:
            return random.choice(pool)
        return f"miss-{random.randrange(1_000_000)}"


def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def ingest_loop(
    *,
    base_url: str,
    token: str,
    deadline: float,
    session_qps: float,
    traces_per_session: int,
    spans_per_trace: int,
    concurrency: int,
    stats: WorkloadStats,
    pool: SessionPool,
) -> None:
    interval = 1.0 / max(session_qps, 0.001)
    sem = threading.Semaphore(concurrency)
    session = requests.Session()

    def one() -> None:
        body, sid, nspans = build_agent_session(
            traces_per_session=traces_per_session,
            spans_per_trace=spans_per_trace,
        )
        t0 = time.perf_counter()
        try:
            r = session.post(
                f"{base_url}/v1/traces",
                headers=_auth_headers(token),
                json=body,
                timeout=60,
            )
            ms = (time.perf_counter() - t0) * 1000
            ok = r.status_code < 300
            stats.record(ms, ok=ok, units=nspans)
            if ok:
                pool.add_live(sid)
        except requests.RequestException:
            ms = (time.perf_counter() - t0) * 1000
            stats.record(ms, ok=False, units=0)
        finally:
            sem.release()

    next_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_at:
                time.sleep(min(0.05, next_at - now))
                continue
            next_at += interval
            if not sem.acquire(blocking=False):
                continue
            futures.append(ex.submit(one))
        for f in as_completed(futures):
            f.result()


def llm_loop(
    *,
    name: str,
    base_url: str,
    token: str,
    deadline: float,
    interval_ms: int,
    concurrency: int,
    stats: WorkloadStats,
    pool: SessionPool,
    window: tuple[str, str],
) -> None:
    interval = max(interval_ms, 50) / 1000.0
    sem = threading.Semaphore(concurrency)
    session = requests.Session()
    from_ts, to_ts = window

    def one() -> None:
        t0 = time.perf_counter()
        ok = False
        try:
            if name == "llm_session_search":
                r = session.post(
                    f"{base_url}/v1/llm/sessions/search",
                    headers=_auth_headers(token),
                    json={"from": from_ts, "to": to_ts, "limit": 50},
                    timeout=60,
                )
            elif name == "llm_session_detail":
                sid = pool.pick()
                r = session.get(
                    f"{base_url}/v1/llm/sessions/{sid}",
                    headers=_auth_headers(token),
                    timeout=60,
                )
                # 404 on intentional miss is still a successful HTTP probe
                ok = r.status_code in (200, 404)
                ms = (time.perf_counter() - t0) * 1000
                stats.record(ms, ok=ok)
                return
            else:
                raise ValueError(name)
            ok = r.status_code < 300
            ms = (time.perf_counter() - t0) * 1000
            stats.record(ms, ok=ok)
        except requests.RequestException:
            ms = (time.perf_counter() - t0) * 1000
            stats.record(ms, ok=False)
        finally:
            sem.release()

    next_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_at:
                time.sleep(min(0.05, next_at - now))
                continue
            next_at += interval
            if not sem.acquire(blocking=False):
                continue
            futures.append(ex.submit(one))
        for f in as_completed(futures):
            f.result()


def load_session_ids(path: Path | None) -> list[str]:
    if not path or not path.is_file():
        return []
    return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]


def default_window() -> tuple[str, str]:
    to = datetime.now(timezone.utc)
    fr = to - timedelta(days=7)
    return fr.strftime("%Y-%m-%dT%H:%M:%SZ"), to.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_rfc3339(value: str) -> str:
    """Normalize DuckDB/Postgres-ish timestamps to RFC3339 UTC for chrono JSON."""
    s = value.strip()
    if not s:
        raise ValueError("empty timestamp")
    # DuckDB often emits "+00" without minutes; chrono wants "+00:00" or "Z".
    if re.search(r"[+-]\d{2}$", s):
        s = s + ":00"
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _format_cpu(label: str, summary: dict[str, Any]) -> str:
    if not summary.get("count"):
        return f"{label:28} (no samples)"
    return (
        f"{label:28} mean={summary['mean_cores']:.3f} "
        f"p50={summary['p50_cores']:.3f} p95={summary['p95_cores']:.3f} "
        f"p99={summary['p99_cores']:.3f} max={summary['max_cores']:.3f} cores "
        f"(n={summary['count']})"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--api-token", default="test-token")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--warmup-secs", type=int, default=5)
    ap.add_argument(
        "--idle-secs",
        type=int,
        default=60,
        help="Sample process CPU with no client traffic before load and after load (default 60)",
    )
    ap.add_argument(
        "--idle-cooldown-secs",
        type=int,
        default=15,
        help="After load stops, wait this long before post-load idle CPU sampling (drain coalesce/dirty)",
    )
    ap.add_argument(
        "--cpu-sample-interval-ms",
        type=int,
        default=1000,
        help="CPU sample interval for load and idle windows",
    )
    ap.add_argument(
        "--thelake-pid",
        type=int,
        default=None,
        help="thelake PID for /proc CPU sampling (else discover via --base-url port)",
    )
    ap.add_argument("--session-qps", type=float, default=2.0)
    ap.add_argument("--traces-per-session", type=int, default=3)
    ap.add_argument("--spans-per-trace", type=int, default=5)
    ap.add_argument("--ingest-concurrency", type=int, default=8)
    ap.add_argument("--llm-concurrency", type=int, default=4)
    ap.add_argument("--llm-interval-ms", type=int, default=500)
    ap.add_argument("--session-id-file", type=Path, default=None)
    ap.add_argument("--from", dest="from_ts", default=None)
    ap.add_argument("--to", dest="to_ts", default=None)
    ap.add_argument("--report-json", type=Path, default=None)
    ap.add_argument(
        "--workloads",
        default="ingest,search,detail",
        help="comma list: ingest,search,detail",
    )
    ap.add_argument("--max-error-rate", type=float, default=0.05)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    token = args.api_token
    wanted = {w.strip() for w in args.workloads.split(",") if w.strip()}
    fr, to = args.from_ts, args.to_ts
    if not fr or not to:
        fr, to = default_window()
    else:
        fr, to = to_rfc3339(fr), to_rfc3339(to)

    thelake_pid = resolve_pid(pid=args.thelake_pid, base_url=base)
    # Prefer port discovery when an explicit pid looks wrong (e.g. cargo wrapper).
    discovered = resolve_pid(pid=None, base_url=base)
    if discovered is not None and thelake_pid is not None and discovered != thelake_pid:
        print(
            f"WARN: --thelake-pid={thelake_pid} is not the :{urlparse(base).port} listener "
            f"(pid={discovered}); using listener PID",
            file=sys.stderr,
        )
        thelake_pid = discovered
    elif thelake_pid is None:
        thelake_pid = discovered
    cpu_interval = max(args.cpu_sample_interval_ms, 100) / 1000.0
    if thelake_pid is None:
        print(
            "WARN: could not resolve thelake PID; CPU sections will be empty "
            "(pass --thelake-pid)",
            file=sys.stderr,
        )

    idle_secs = max(0, args.idle_secs)
    idle_cooldown = max(0, args.idle_cooldown_secs)
    pre_idle_samples: list[float] = []
    if thelake_pid is not None and idle_secs > 0:
        print(
            f"Pre-load idle CPU sample {idle_secs}s (pid={thelake_pid}, "
            "no client traffic)…"
        )
        pre_idle_samples = sample_window(
            thelake_pid, float(idle_secs), interval_s=cpu_interval
        )

    pool = SessionPool(load_session_ids(args.session_id_file))
    record_after = time.perf_counter() + max(0, args.warmup_secs)
    stats: dict[str, WorkloadStats] = {
        "ingest_sessions": WorkloadStats("ingest_sessions", record_after=record_after),
        "llm_session_search": WorkloadStats("llm_session_search", record_after=record_after),
        "llm_session_detail": WorkloadStats("llm_session_detail", record_after=record_after),
    }

    deadline = record_after + args.duration
    t_wall0 = record_after  # report QPS over post-warmup window

    threads: list[threading.Thread] = []
    if "ingest" in wanted:
        threads.append(
            threading.Thread(
                target=ingest_loop,
                kwargs=dict(
                    base_url=base,
                    token=token,
                    deadline=deadline,
                    session_qps=args.session_qps,
                    traces_per_session=args.traces_per_session,
                    spans_per_trace=args.spans_per_trace,
                    concurrency=args.ingest_concurrency,
                    stats=stats["ingest_sessions"],
                    pool=pool,
                ),
                daemon=True,
            )
        )
    if "search" in wanted:
        threads.append(
            threading.Thread(
                target=llm_loop,
                kwargs=dict(
                    name="llm_session_search",
                    base_url=base,
                    token=token,
                    deadline=deadline,
                    interval_ms=args.llm_interval_ms,
                    concurrency=args.llm_concurrency,
                    stats=stats["llm_session_search"],
                    pool=pool,
                    window=(fr, to),
                ),
                daemon=True,
            )
        )
    if "detail" in wanted:
        threads.append(
            threading.Thread(
                target=llm_loop,
                kwargs=dict(
                    name="llm_session_detail",
                    base_url=base,
                    token=token,
                    deadline=deadline,
                    interval_ms=args.llm_interval_ms,
                    concurrency=args.llm_concurrency,
                    stats=stats["llm_session_detail"],
                    pool=pool,
                    window=(fr, to),
                ),
                daemon=True,
            )
        )
    load_cpu_samples: list[float] = []
    load_cpu_box: list[list[float]] = [load_cpu_samples]

    def _sample_load_cpu() -> None:
        if thelake_pid is None:
            return
        # Align with latency window: start after warmup, end at load deadline.
        delay = record_after - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        load_cpu_box[0] = sample_window(
            thelake_pid,
            float(args.duration),
            interval_s=cpu_interval,
            stop_at=deadline,
        )

    cpu_thread = threading.Thread(target=_sample_load_cpu, daemon=True)

    for th in threads:
        th.start()
    cpu_thread.start()
    # Wait until post-warmup window starts before measuring wall clock for QPS.
    delay = record_after - time.perf_counter()
    if delay > 0:
        time.sleep(delay)
    t_wall0 = time.perf_counter()
    for th in threads:
        th.join()
    cpu_thread.join()
    load_cpu_samples = load_cpu_box[0]

    duration_s = max(time.perf_counter() - t_wall0, 0.001)

    post_idle_samples: list[float] = []
    if thelake_pid is not None and idle_secs > 0:
        if idle_cooldown > 0:
            print(
                f"\nLoad stopped — cooling down {idle_cooldown}s "
                "(coalesce/dirty drain) before idle sample…"
            )
            time.sleep(idle_cooldown)
        print(f"Post-load idle CPU sample {idle_secs}s (pid={thelake_pid})…")
        post_idle_samples = sample_window(
            thelake_pid, float(idle_secs), interval_s=cpu_interval
        )

    pre_idle_cpu = summarize_cpu(pre_idle_samples)
    load_cpu = summarize_cpu(load_cpu_samples)
    post_idle_cpu = summarize_cpu(post_idle_samples)

    reports = []
    print(f"\n========== LLM Bench Report ({duration_s:.1f}s load) ==========")
    print(f"base_url={base} window={fr}..{to} thelake_pid={thelake_pid}")
    fail = False
    for key, st in stats.items():
        if st.ok + st.errors == 0:
            continue
        rep = st.report(duration_s)
        reports.append(rep)
        total = rep["ok"] + rep["errors"]
        err_rate = rep["errors"] / total if total else 0.0
        print(
            f"{rep['workload']:28} ok={rep['ok']:<6} err={rep['errors']:<5} "
            f"qps={rep['achieved_qps']:<8} "
            f"p50={rep['p50_ms']} p95={rep['p95_ms']} p99={rep['p99_ms']} ms"
        )
        if err_rate > args.max_error_rate:
            fail = True
            print(f"  FAIL error_rate={err_rate:.3f} > max={args.max_error_rate}")

    print(_format_cpu(f"cpu_idle_pre_load ({idle_secs}s)", pre_idle_cpu))
    print(_format_cpu("cpu_under_load", load_cpu))
    print(
        _format_cpu(
            f"cpu_idle_post_load ({idle_secs}s after {idle_cooldown}s cool)",
            post_idle_cpu,
        )
    )

    out = {
        "duration_s": duration_s,
        "idle_secs": idle_secs,
        "idle_cooldown_secs": idle_cooldown,
        "base_url": base,
        "thelake_pid": thelake_pid,
        "window": {"from": fr, "to": to},
        "workloads": reports,
        "cpu": {
            "idle_pre_load": pre_idle_cpu,
            "under_load": load_cpu,
            "idle_post_load": post_idle_cpu,
            "sample_interval_s": cpu_interval,
            "unit": "cores (1.0 = one full core; same scale as thelake.process.cpu_ratio)",
            "notes": (
                "idle_pre_load is the fair no-traffic baseline after startup; "
                "idle_post_load waits idle_cooldown_secs after load stops then samples"
            ),
        },
    }
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(out, indent=2) + "\n")
        print(f"wrote {args.report_json}")

    print("==============================================\n")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
