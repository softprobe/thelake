# OpenTelemetry Demo (Astronomy Shop) — Softprobe Grafana traffic source

Pinned release: **3.0.0** (`ghcr.io/open-telemetry/demo:3.0.0-*`).

`make grafana-up` clones the pin into `~/.cache/thelake/otel-demo/3.0.0` (override
with `THELAKE_CACHE_ROOT`) and starts **minimal + no demo observability stack**:

```text
docker compose -f compose.yaml -f <thelake>/compose.softprobe.yaml
```

Collector extras ([`otelcol-config-extras.yml`](otelcol-config-extras.yml)):

- Export **full application metrics** (no name allow-list), **app logs**, and
  **sampled traces** to Softprobe (`host.docker.internal:8090`, Bearer `local-dev-key`)
- Receivers stay shop-relevant: `otlp`, `prometheus/ad`, `span_metrics`
  (not docker_stats / host_metrics / redis / postgres — infra noise, not boards)
- Timeout-dominated batch (`timeout: 15s`, `num_consumers: 1`) for pacing —
  Softprobe coalesce absorbs volume; do **not** drop shop metrics for CPU budget
- Traces use probabilistic sampling (10%) so Tempo stays online without span
  storms; metrics + app logs stay complete
- Traces also feed `span_metrics`; app-log filter keeps collector/docker noise out

`grafana-up` waits until Prom series show **non-identical** samples (live scrapes)
and Loki `/labels` returns names inside the last hour — not flat lookback lines or
stale seeded logs outside Explore's default window. Self-monitoring/ops stays on
by default (`THELAKE_SELF_MONITORING_ENABLED=true`).

Requires ~3 GB RAM and Docker. Store UI: http://127.0.0.1:8080
