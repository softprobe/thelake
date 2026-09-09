# Full-fidelity verification (honest)

measured_at_utc: 2026-09-09T18:20:21Z

## Accountability

Earlier "CPU PASS avg=77.9" used `ps %CPU`, which on Linux is **lifetime average
since process start** (poisoned by startup TWCS). That was not a valid success
gate. Corrected probe uses `/proc/<pid>/stat` utime+stime 1s deltas.

## Goals

| Goal | Result | Evidence |
|------|--------|----------|
| PromQL ≤100ms all dashboard×range (isolated warmup) | **PASS** | 1672/1672 cells, worst 56.99ms |
| Live Softprobe CPU avg&lt;100 and p95&lt;100 (instantaneous, full OTLP + Grafana) | **FAIL (so far)** | ingest-only ~avg 102 / p95 107; with paced PromQL much higher |
| Live ingest (metrics) | **PASS** when collector healthy | http_server / demo_ad / k6 changing |
| Ops thelake_* | **PASS** names present; panels ~20/25 nonempty at 1h | compaction panels empty when maintenance off |
| Loki labels | **PASS** | service_name, service_namespace, cloud_region |
| Traces tags | **PASS** | Tempo search tags non-empty (5% sample) |
| Astronomy Shop panels 30m | **mostly PASS** | Ad/Cart/Checkout/Frontend/GOLD/Infra/Loadgen/Payment/Rec 100%; Currency partial; Product Catalog empty (service unhealthy); Compose smoke empty (seeded fixtures) |
| H-04 hist bucket rate | **FAIL** | `limit_exceeded` 16032 &gt; max_series 10000 |
| label/job/values | **FAIL** | same max_series cap |

## Knobs currently live

- flush_interval_seconds=30, otel batch timeout=30s, trace sample 5%
- worker_threads=2, max_connections=2, self_monitoring on
- maintenance currently **false** on live config (CPU experiment); re-enable for long-range downsample fill
- coalesce MAX_ROWS_PER_FLUSH=1536

## Stack left running

- Grafana http://127.0.0.1:3000 admin/admin
- Softprobe :8090
- otel-collector + load-generator + Astronomy Shop
