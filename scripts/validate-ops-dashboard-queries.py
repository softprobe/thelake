#!/usr/bin/env python3
"""Validate every PromQL/LogQL expr on the thelake-ops Grafana dashboard.

No skips: exit 0 only when each target returns ≥1 finite series (Prom) or
≥1 log line (Loki). Seeds customer ingest / errors / queries when needed.

Usage:
  ./scripts/validate-ops-dashboard-queries.py
  ./scripts/validate-ops-dashboard-queries.py --timeout-secs 240

Env:
  SOFTPROBE_URL          default http://127.0.0.1:8090
  SOFTPROBE_OPS_API_KEY  default local-ops-key
  SOFTPROBE_API_KEY      default local-dev-key (customer ingest seed)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DASHBOARD = ROOT / "tests/compat/grafana/dashboards/ops/thelake-ops.json"


class Check:
    __slots__ = ("panel", "ref_id", "datasource", "expr")

    def __init__(self, panel: str, ref_id: str, datasource: str, expr: str):
        self.panel = panel
        self.ref_id = ref_id
        self.datasource = datasource
        self.expr = expr

    @property
    def id(self) -> str:
        return f"{self.panel} [{self.ref_id}]"


def load_checks(path: Path) -> list[Check]:
    doc = json.loads(path.read_text())
    dash = doc.get("dashboard", doc)
    checks: list[Check] = []
    for panel in dash.get("panels", []):
        title = str(panel.get("title") or f"panel-{panel.get('id')}")
        ds = panel.get("datasource") or {}
        ds_type = str(ds.get("type") or "prometheus")
        for target in panel.get("targets") or []:
            expr = target.get("expr")
            if not expr or not str(expr).strip():
                raise SystemExit(f"{title}: target missing expr")
            checks.append(
                Check(
                    panel=title,
                    ref_id=str(target.get("refId") or "?"),
                    datasource=ds_type,
                    expr=str(expr).strip(),
                )
            )
    if not checks:
        raise SystemExit(f"no queries found in {path}")
    return checks


def http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 120.0,
) -> tuple[int, Any]:
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            code = resp.getcode() or 200
    except urllib.error.HTTPError as e:
        raw = e.read()
        code = e.code
    except urllib.error.URLError as e:
        raise RuntimeError(f"{method} {url}: {e}") from e
    if not raw:
        return code, None
    try:
        return code, json.loads(raw.decode())
    except json.JSONDecodeError:
        return code, raw.decode(errors="replace")


def wait_ready(base: str, timeout_secs: float) -> None:
    deadline = time.time() + timeout_secs
    last = ""
    while time.time() < deadline:
        try:
            code, _ = http_json("GET", f"{base}/ready", timeout=3.0)
            if code == 200:
                return
            last = f"HTTP {code}"
        except Exception as e:  # noqa: BLE001 — surface last error at timeout
            last = str(e)
        time.sleep(1.0)
    raise SystemExit(f"Softprobe not ready at {base}/ready ({last})")


def otlp_gauge_body(name: str, value: float) -> bytes:
    """OTLP JSON matching opentelemetry-proto 0.7 serde (`data.gauge`, numeric times)."""
    now_ns = time.time_ns()
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "ops-dashboard-validator"},
                        }
                    ],
                    "droppedAttributesCount": 0,
                },
                "scopeMetrics": [
                    {
                        "scope": None,
                        "metrics": [
                            {
                                "name": name,
                                "description": "validator seed",
                                "unit": "1",
                                "metadata": [],
                                                "data": {
                                                    "gauge": {
                                                        "dataPoints": [
                                                            {
                                                                "attributes": [],
                                                                "startTimeUnixNano": 0,
                                                                "timeUnixNano": now_ns,
                                                                "exemplars": [],
                                                                "flags": 0,
                                                                "value": {"asDouble": value},
                                                            }
                                                        ]
                                                    }
                                                },
                            }
                        ],
                        "schemaUrl": "",
                    }
                ],
                "schemaUrl": "",
            }
        ]
    }
    return json.dumps(payload).encode()


def seed(
    base: str,
    customer_key: str,
    ops_key: str,
    rounds: int,
) -> None:
    cust = {"Authorization": f"Bearer {customer_key}", "Content-Type": "application/json"}
    ops = {"Authorization": f"Bearer {ops_key}"}

    print("==> seed: customer OTLP ingest (ok + decode error)")
    for i in range(rounds):
        code, body = http_json(
            "POST",
            f"{base}/v1/metrics",
            headers=cust,
            body=otlp_gauge_body("validator_seed_requests_total", float(i + 1)),
            timeout=120.0,
        )
        if code >= 300:
            raise SystemExit(f"seed ingest failed HTTP {code}: {body!r}")
        # Decode failure is recorded as thelake_ingest_errors_total (customer tenant).
        code_err, _ = http_json(
            "POST",
            f"{base}/v1/metrics",
            headers=cust,
            body=b"{",
            timeout=60.0,
        )
        if code_err < 400:
            raise SystemExit(
                f"expected ingest error seed to fail, got HTTP {code_err}"
            )
        time.sleep(0.2)

    print("==> seed: ops PromQL (query duration / queue wait)")
    warm_queries = [
        "thelake_process_cpu_ratio",
        "thelake_table_live_files",
        "thelake_ingest_requests_total",
        "up",
    ]
    for _ in range(rounds):
        for q in warm_queries:
            url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q})
            try:
                http_json("GET", url, headers=ops, timeout=90.0)
            except Exception as e:  # noqa: BLE001
                print(f"  warn warm ops query: {e}")

    print("==> seed: force slow customer queries (ops Loki + slow_queries_total)")
    heavy = [
        'count({__name__=~".+"})',
        'count({__name__=~"http_.*|demo_.*|traces_.*|validator_.*"})',
        '{__name__=~".+"}',
    ]
    cust_h = {"Authorization": f"Bearer {customer_key}"}
    for _ in range(max(2, rounds)):
        for q in heavy:
            url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q})
            try:
                http_json("GET", url, headers=cust_h, timeout=180.0)
            except Exception as e:  # noqa: BLE001
                print(f"  warn heavy customer query: {e}")
        time.sleep(0.5)
    # Allow slow-log drain (2s interval) to flush
    time.sleep(3.0)


def finite_sample(value: Any) -> bool:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v)


def check_prom(base: str, ops_key: str, expr: str) -> tuple[bool, str]:
    url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    code, body = http_json(
        "GET",
        url,
        headers={"Authorization": f"Bearer {ops_key}"},
        timeout=120.0,
    )
    if code != 200:
        return False, f"HTTP {code}: {body!r}"
    if not isinstance(body, dict):
        return False, f"non-json body: {body!r}"
    if body.get("status") != "success":
        return False, f"status={body.get('status')} error={body.get('error')}"
    result = (body.get("data") or {}).get("result") or []
    if not result:
        return False, "0 series"
    bad = 0
    for series in result:
        val = None
        if "value" in series and isinstance(series["value"], list) and len(series["value"]) >= 2:
            val = series["value"][1]
        elif "values" in series and series["values"]:
            val = series["values"][-1][1]
        if not finite_sample(val):
            bad += 1
    if bad:
        return False, f"{len(result)} series but {bad} non-finite sample(s)"
    return True, f"{len(result)} series"


def check_loki(base: str, ops_key: str, expr: str, lookback_secs: int) -> tuple[bool, str]:
    end_ns = time.time_ns()
    start_ns = end_ns - lookback_secs * 1_000_000_000
    url = f"{base}/loki/api/v1/query_range?" + urllib.parse.urlencode(
        {
            "query": expr,
            "start": str(start_ns),
            "end": str(end_ns),
            "limit": "100",
        }
    )
    code, body = http_json(
        "GET",
        url,
        headers={"Authorization": f"Bearer {ops_key}"},
        timeout=120.0,
    )
    if code != 200:
        return False, f"HTTP {code}: {body!r}"
    if not isinstance(body, dict):
        return False, f"non-json body: {body!r}"
    if body.get("status") != "success":
        return False, f"status={body.get('status')} error={body.get('error')}"
    result = (body.get("data") or {}).get("result") or []
    lines = 0
    for stream in result:
        lines += len(stream.get("values") or [])
    if lines < 1:
        return False, "0 log lines"
    return True, f"{lines} log line(s) in {len(result)} stream(s)"


def check_grafana_loki(
    grafana_url: str,
    grafana_user: str,
    grafana_password: str,
    expr: str,
    lookback_secs: int,
) -> tuple[bool, str]:
    """Same LogQL via Grafana /api/ds/query (what the dashboard panel uses)."""
    import base64

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_secs * 1000
    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "loki", "uid": "softprobe-loki-ops"},
                "expr": expr,
                "queryType": "range",
                "maxLines": 1000,
            }
        ],
        "from": str(start_ms),
        "to": str(end_ms),
    }
    token = base64.b64encode(f"{grafana_user}:{grafana_password}".encode()).decode()
    code, body = http_json(
        "POST",
        f"{grafana_url.rstrip('/')}/api/ds/query",
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        },
        body=json.dumps(payload).encode(),
        timeout=120.0,
    )
    if code != 200:
        return False, f"Grafana HTTP {code}: {body!r}"
    if not isinstance(body, dict):
        return False, f"Grafana non-json: {body!r}"
    result_a = (body.get("results") or {}).get("A") or {}
    if result_a.get("error"):
        return False, f"Grafana error: {result_a.get('error')}"
    rows = 0
    for frame in result_a.get("frames") or []:
        values = ((frame.get("data") or {}).get("values")) or []
        if values and values[0]:
            rows = max(rows, len(values[0]))
    if rows < 1:
        return False, "Grafana 0 log rows"
    return True, f"Grafana {rows} log row(s)"


def evaluate(
    checks: list[Check],
    base: str,
    ops_key: str,
    lookback_secs: int,
    grafana_url: str | None,
    grafana_user: str,
    grafana_password: str,
) -> list[str]:
    failures: list[str] = []
    for c in checks:
        if c.datasource == "loki":
            ok, detail = check_loki(base, ops_key, c.expr, lookback_secs)
            status = "PASS" if ok else "FAIL"
            print(f"  {status}  {c.id}: {detail}")
            if not ok:
                failures.append(f"{c.id}: {detail}\n    expr: {c.expr}")
            if grafana_url:
                g_ok, g_detail = check_grafana_loki(
                    grafana_url, grafana_user, grafana_password, c.expr, lookback_secs
                )
                g_status = "PASS" if g_ok else "FAIL"
                print(f"  {g_status}  {c.id} [grafana]: {g_detail}")
                if not g_ok:
                    failures.append(f"{c.id} [grafana]: {g_detail}\n    expr: {c.expr}")
        else:
            ok, detail = check_prom(base, ops_key, c.expr)
            status = "PASS" if ok else "FAIL"
            print(f"  {status}  {c.id}: {detail}")
            if not ok:
                failures.append(f"{c.id}: {detail}\n    expr: {c.expr}")
    return failures


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dashboard",
        type=Path,
        default=DEFAULT_DASHBOARD,
        help="path to thelake-ops.json",
    )
    ap.add_argument("--base-url", default=os.environ.get("SOFTPROBE_URL", "http://127.0.0.1:8090"))
    ap.add_argument(
        "--ops-key",
        default=os.environ.get("SOFTPROBE_OPS_API_KEY", "local-ops-key"),
    )
    ap.add_argument(
        "--customer-key",
        default=os.environ.get("SOFTPROBE_API_KEY", "local-dev-key"),
    )
    ap.add_argument("--timeout-secs", type=float, default=300.0)
    ap.add_argument("--ready-timeout-secs", type=float, default=90.0)
    ap.add_argument("--seed-rounds", type=int, default=4)
    ap.add_argument("--export-wait-secs", type=float, default=35.0)
    ap.add_argument("--poll-secs", type=float, default=10.0)
    ap.add_argument("--loki-lookback-secs", type=int, default=3600)
    ap.add_argument("--no-seed", action="store_true", help="only query; do not ingest")
    ap.add_argument(
        "--grafana-url",
        default=os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000"),
        help="Also verify Loki panels via Grafana /api/ds/query (empty to skip)",
    )
    ap.add_argument("--grafana-user", default=os.environ.get("GRAFANA_ADMIN_USER", "admin"))
    ap.add_argument(
        "--grafana-password",
        default=os.environ.get("GRAFANA_ADMIN_PASSWORD", "admin"),
    )
    args = ap.parse_args()

    checks = load_checks(args.dashboard)
    print(f"==> loaded {len(checks)} queries from {args.dashboard}")
    wait_ready(args.base_url.rstrip("/"), args.ready_timeout_secs)
    print("==> Softprobe ready")

    base = args.base_url.rstrip("/")
    grafana_url = (args.grafana_url or "").strip() or None
    if not args.no_seed:
        seed(base, args.customer_key, args.ops_key, args.seed_rounds)
        print(f"==> waiting {args.export_wait_secs:.0f}s for self-monitoring export")
        time.sleep(args.export_wait_secs)
        # Second seed pulse so rate() has multiple samples in-window
        seed(base, args.customer_key, args.ops_key, max(2, args.seed_rounds // 2))
        time.sleep(max(20.0, args.export_wait_secs * 0.6))

    deadline = time.time() + args.timeout_secs
    attempt = 0
    last_failures: list[str] = []
    while time.time() < deadline:
        attempt += 1
        print(f"\n==> validate attempt {attempt}")
        last_failures = evaluate(
            checks,
            base,
            args.ops_key,
            args.loki_lookback_secs,
            grafana_url,
            args.grafana_user,
            args.grafana_password,
        )
        if not last_failures:
            n = len(checks) + (1 if grafana_url and any(c.datasource == "loki" for c in checks) else 0)
            print(f"\nOK: all self-monitoring dashboard queries passed ({len(checks)} panel exprs)")
            return 0
        remaining = deadline - time.time()
        if remaining < args.poll_secs:
            break
        print(f"==> {len(last_failures)} failing; re-seed + retry in {args.poll_secs:.0f}s")
        if not args.no_seed:
            seed(base, args.customer_key, args.ops_key, 2)
        time.sleep(args.poll_secs)

    print("\nFAILED queries:", file=sys.stderr)
    for f in last_failures:
        print(f"  - {f}", file=sys.stderr)
    print(
        f"\nERROR: {len(last_failures)}/{len(checks)} queries failed after timeout",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
