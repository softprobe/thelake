# Query Features & Aggregations Verification Checklist

Canonical checklist of query features verified for Softprobe/thelake product
signals (**traces + logs**) via Loki/Tempo compatibility and Grafana Explore.
Customer metrics / Prometheus / PromQL are **out of scope** (removed).

---

## 1. Ingestion Pipeline Verification

| Item | Feature | Source | Ingestion Verification | Verification Status |
|:-----|:--------|:-------|:-----------------------|:--------------------|
| I-01 | OTLP HTTP Trace Stream | OTel Demo / collectors | `/v1/traces` accepts OTLP batches; spans written to DuckLake | VERIFIED |
| I-02 | Continuous Live Ingestion | Load Generator & services | Trace/log timestamps stay near wall clock under live export | VERIFIED |
| I-03 | Application Logs Ingestion | OTel Demo application logs | `/v1/logs` accepts OTLP log batches; labels and body queryable | VERIFIED |

---

## 2. Loki (LogQL) Features

| Item | Feature | Syntax / Endpoint | Browser / Explore Verification | Status |
|:-----|:--------|:------------------|:-------------------------------|:-------|
| L-01 | Stream Selector | `{service_name="frontend"}` | Log stream returned in Explore | VERIFIED |
| L-02 | Line Filter Contains | `{service_name="frontend"} \|= "HTTP"` | Filtered log lines | VERIFIED |
| L-03 | Line Filter Not Contains | `{service_name="frontend"} != "DEBUG"` | Filtered log lines | VERIFIED |
| L-04 | Line Filter Regex | `{service_name="frontend"} \|~ "GET\|POST"` | Regex filtered log lines | VERIFIED |
| L-05 | JSON Parser Stage | `{service_name="frontend"} \| json` | Parsed JSON attributes into fields | VERIFIED |
| L-06 | Parsed Field Matcher | `{service_name="frontend"} \| json \| status_code = "200"` | Field filtered log lines | VERIFIED |
| L-07 | Labels & Values | `/loki/api/v1/labels`, `/loki/api/v1/label/{name}/values` | Label autocomplete in Explore | VERIFIED |

---

## 3. Tempo (TraceQL) Features

> Full span-tree retrieval and TraceQL filter evaluations are tested by the
> dedicated Tempo contract and differential suite (`tests/compat/tempo/` via
> `make test-tempo-diff` and
> `tests/compat/grafana/e2e/tempo_tenant_contract_test.sh`).

| Item | Feature | Endpoint | Browser / Test Verification | Status |
|:-----|:--------|:---------|:----------------------------|:-------|
| TR-01 | Trace Lookup | `/api/traces/{traceID}` | Protocol not-found / detail responses verified | VERIFIED (Protocol & Contract Suite) |
| TR-02 | Trace Search | `/api/search` with tags & duration | Trace search endpoint envelope verified | VERIFIED (Protocol & Contract Suite) |
| TR-03 | Search Tag Names | `/api/search/tags` | Resource & span tag discovery verified | VERIFIED (Protocol & Contract Suite) |
| TR-04 | Search Tag Values | `/api/search/tag/{tag}/values` | Tag value discovery verified | VERIFIED (Protocol & Contract Suite) |

---

## 4. Real Grafana Settings & Browser Automations

| Item | Capability | Verification Scope | Status |
|:-----|:-----------|:-------------------|:-------|
| G-01 | Headless Browser Login | Playwright logs in as `admin:admin`, skips password change | VERIFIED |
| G-02 | Native Datasources Provisioning | Loki and Tempo Softprobe datasources present | VERIFIED |
| G-03 | Cross-Signal Link Wiring | Loki `TraceID` derived field targets Tempo; Tempo `tracesToLogsV2` targets Loki | VERIFIED |
| G-04 | Interactive Explore Automation | Playwright exercises Loki/Tempo Explore paths against live Softprobe | VERIFIED |
