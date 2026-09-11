# Demo CPU full-OTLP (demo-cpu-full-inline10k-flush10)

measured_at_utc: 2026-09-11T09:25:07Z
result: **PASS**

## Criterion

- Full OTLP signal types (metrics + app logs + 5% sampled traces)
- Grafana dashboards remain refreshing (10s); not paused during measure
- Softprobe pinned to one core; maintenance + self-monitoring off for the gate
- External `/proc/<pid>/stat` sampling; mean CPU ratio < 0.85
- Gate profile: flush=10s affinity=0 self_mon=false maint=false

Window: warmup=60s measure=300s interval=1s
Wall clock TOTAL: 762s (bringup_start=0s, bringup_done=399s, ingest_ready=2s, measure_start=0s, measure_done=361s)

## Processes

| Process | pid | n | mean | p95 | max |
|---------|-----|---|------|-----|-----|
| softprobe | 69701 | 300 | 0.8124 | 0.9998 | 1.0098 |

JSON: `20260911T091225Z-demo-cpu-full-inline10k-flush10.json`
