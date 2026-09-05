# Query Features & Aggregations Verification Checklist

This document is the canonical checklist of all query features, functions, and aggregations supported by TheLake/Softprobe, verified against live OpenTelemetry Demo ingestion and real Grafana settings via browser automation (Playwright).

---

## 1. Ingestion Pipeline Verification

| Item | Feature | Source | Ingestion Verification | Verification Status |
|:-----|:--------|:-------|:-----------------------|:--------------------|
| I-01 | OTLP HTTP Metric Stream | OTel Demo (Astronomy Shop) | `/v1/metrics` accepts OTLP batches; samples written to DuckLake | VERIFIED |
| I-02 | Continuous Live Ingestion | Load Generator (k6) & services | Samples timestamp delta < 60s from current wall clock | VERIFIED |
| I-03 | Gauges Ingestion | OTel SDKs (k6, frontend, etc.) | Metric samples stored with floating point values | VERIFIED |
| I-04 | Sums / Counters Ingestion | OTel Demo services (ad, cart, etc.) | Cumulative sum samples stored with monotonicity | VERIFIED |
| I-05 | Classic Histograms Ingestion | Cart, HTTP, RPC instrumentation | Dual-written `_bucket`, `_count`, `_sum` with `le` labels | VERIFIED |
| I-06 | Application Logs Ingestion | OTel Demo application logs | `/v1/logs` accepts OTLP log batches; labels and JSON body queryable | VERIFIED |

---

## 2. PromQL Selectors & Matchers

| Item | Feature / Syntax | Example Query | Grafana Explore / Browser Test | API / Datasource Proxy | Status |
|:-----|:-----------------|:--------------|:-------------------------------|:-----------------------|:-------|
| S-01 | Exact Metric Name | `k6_http_reqs` | Renders time series graph in Explore | HTTP 200, matrix vector | VERIFIED |
| S-02 | Label Equality (`=`) | `k6_http_reqs{job="load-generator"}` | Matches target series | HTTP 200, matching series | VERIFIED |
| S-03 | Label Inequality (`!=`) | `http_server_request_duration_count{job!="otel-collector"}` | Filters out non-matching series | HTTP 200, filtered series | VERIFIED |
| S-04 | Regex Matcher (`=~`) | `k6_http_reqs{job=~"load.*"}` | Regex match on series label | HTTP 200, matching series | VERIFIED |
| S-05 | Negative Regex Matcher (`!~`) | `demo_ad_served_total{category!~"telescopes"}` | Excludes matched series | HTTP 200, filtered series | VERIFIED |
| S-06 | Multi-Label Matching | `k6_http_reqs{job="load-generator", method=~".+"}` | Combined multi-matcher evaluation | HTTP 200, matching series | VERIFIED |
| S-07 | Sub-selector Range Window | `k6_http_reqs[5m]` | Matrix selector parameter for functions | Valid range vector | VERIFIED |

---

## 3. Rate Family Functions (Range-Vector Instant Functions)

| Item | Function | Description | Example Query | Grafana Browser Verification | Status |
|:-----|:---------|:------------|:--------------|:-----------------------------|:-------|
| R-01 | `rate` | Per-second average rate of increase | `rate(k6_http_reqs[5m])` | Time-series chart renders positive rate | VERIFIED |
| R-02 | `irate` | Instant rate of increase (last 2 points) | `irate(k6_http_reqs[5m])` | Time-series chart renders instant rate | VERIFIED |
| R-03 | `increase` | Total increase across range interval | `increase(k6_http_reqs[5m])` | Time-series chart renders total delta | VERIFIED |
| R-04 | `delta` | Difference between first and last value in range | `delta(k6_vus[5m])` | Evaluates gauge difference across window | VERIFIED |
| R-05 | `idelta` | Difference between last two points in range | `idelta(k6_vus[5m])` | Evaluates instant gauge difference | VERIFIED |

---

## 4. Over-Time Aggregation Functions

| Item | Function | Description | Example Query | Grafana Browser Verification | Status |
|:-----|:---------|:------------|:--------------|:-----------------------------|:-------|
| O-01 | `sum_over_time` | Sum of all samples in range | `sum_over_time(k6_vus[5m])` | Graph renders calculated sum | VERIFIED |
| O-02 | `avg_over_time` | Average of all samples in range | `avg_over_time(k6_vus[5m])` | Graph renders smoothed average | VERIFIED |
| O-03 | `min_over_time` | Minimum sample value in range | `min_over_time(k6_vus[5m])` | Graph renders minimum curve | VERIFIED |
| O-04 | `max_over_time` | Maximum sample value in range | `max_over_time(k6_vus[5m])` | Graph renders maximum curve | VERIFIED |
| O-05 | `count_over_time` | Count of samples in range | `count_over_time(k6_vus[5m])` | Graph renders point count | VERIFIED |
| O-06 | `last_over_time` | Most recent sample value in range | `last_over_time(k6_vus[5m])` | Graph renders last observed value | VERIFIED |

---

## 5. Vector Aggregations & Grouping

| Item | Aggregation | Description | Example Query | Grafana Browser Verification | Status |
|:-----|:------------|:------------|:--------------|:-----------------------------|:-------|
| A-01 | `sum` | Sum across series | `sum(k6_http_reqs)` | Single consolidated series graph | VERIFIED |
| A-02 | `avg` | Average across series | `avg(k6_vus)` | Consolidated mean series | VERIFIED |
| A-03 | `min` | Minimum across series | `min(k6_vus)` | Consolidated minimum series | VERIFIED |
| A-04 | `max` | Maximum across series | `max(k6_vus)` | Consolidated maximum series | VERIFIED |
| A-05 | `count` | Count of series | `count(k6_http_reqs)` | Number of matching active series | VERIFIED |
| A-06 | `topk` | Top k series by value | `topk(3, sum by (job) (k6_http_reqs))` | Returns at most 3 highest series | VERIFIED |
| A-07 | `bottomk` | Bottom k series by value | `bottomk(3, sum by (job) (k6_http_reqs))` | Returns at most 3 lowest series | VERIFIED |
| A-08 | `by (...)` | Group by dimension | `sum by (job) (k6_http_reqs)` | Groups series preserving `job` label | VERIFIED |
| A-09 | `without (...)` | Group dropping dimension | `avg without (method) (k6_http_reqs)` | Aggregates dropping `method` label | VERIFIED |

---

## 6. Instant Vector Math Functions

| Item | Function | Description | Example Query | Grafana Browser Verification | Status |
|:-----|:---------|:------------|:--------------|:-----------------------------|:-------|
| M-01 | `abs` | Absolute value | `abs(delta(k6_vus[5m]))` | Graph non-negative values | VERIFIED |
| M-02 | `ceil` | Round up to nearest integer | `ceil(k6_vus)` | Ceiling stepped values | VERIFIED |
| M-03 | `floor` | Round down to nearest integer | `floor(k6_vus)` | Floor stepped values | VERIFIED |
| M-04 | `round` (1-arg) | Round to nearest integer | `round(k6_vus)` | Rounded integer values | VERIFIED |
| M-05 | `round` (2-arg) | Round to nearest arbitrary fraction | `round(k6_vus, 0.5)` | Rounded fractional values | VERIFIED |

---

## 7. Binary Operators (Arithmetic, Comparison, Sets)

| Item | Operator | Category | Example Query | Grafana Browser Verification | Status |
|:-----|:---------|:---------|:--------------|:-----------------------------|:-------|
| B-01 | `+` (addition) | Arithmetic | `k6_vus + 1` | Scalar offset graph | VERIFIED |
| B-02 | `-` (subtraction) | Arithmetic | `k6_vus - 1` | Subtracted series graph | VERIFIED |
| B-03 | `*` (multiplication) | Arithmetic | `k6_vus * 2` | Scaled series graph | VERIFIED |
| B-04 | `/` (division) | Arithmetic | `sum(k6_http_reqs) / sum(k6_vus)` | Computed ratio series graph | VERIFIED |
| B-05 | `%` (modulo) | Arithmetic | `k6_vus % 3` | Remainder cycle series | VERIFIED |
| B-06 | `^` (exponentiation) | Arithmetic | `k6_vus ^ 2` | Squared values series | VERIFIED |
| B-07 | `>` (greater than) | Comparison | `k6_vus > 1` | Filtered series where value > 1 | VERIFIED |
| B-08 | `<` (less than) | Comparison | `k6_vus < 100` | Filtered series where value < 100 | VERIFIED |
| B-09 | `>=` (greater equal) | Comparison | `k6_vus >= 1` | Filtered series | VERIFIED |
| B-10 | `<=` (less equal) | Comparison | `k6_vus <= 50` | Filtered series | VERIFIED |
| B-11 | `==` (equal) | Comparison | `k6_vus == 1` | Filtered equality series | VERIFIED |
| B-12 | `!=` (not equal) | Comparison | `k6_vus != 0` | Filtered non-zero series | VERIFIED |
| B-13 | `bool` modifier | Comparison Boolean | `k6_vus > bool 1` | 0 or 1 binary indicator series | VERIFIED |
| B-14 | `and` | Set intersection | `sum(k6_http_reqs) and sum(k6_vus)` | Matching label set intersection | VERIFIED |
| B-15 | `or` | Set union | `sum(k6_http_reqs) or sum(k6_vus)` | Matching label set union | VERIFIED |
| B-16 | `unless` | Set complement | `count(k6_http_reqs) unless count(k6_vus)` | Set difference | VERIFIED |

---

## 8. Modifiers & Classic Histograms

| Item | Feature | Description | Example Query | Grafana Browser Verification | Status |
|:-----|:--------|:------------|:--------------|:-----------------------------|:-------|
| T-01 | `offset` | Time-shifted query | `k6_http_reqs offset 5m` | Shifted series comparison | VERIFIED |
| H-01 | `_bucket` series | Histogram cumulative counter | `sum by (le) (http_server_request_duration_bucket)` | Cumulative bucket ladder | VERIFIED |
| H-02 | `_count` series | Total observations count | `sum(http_server_request_duration_count)` | Total count series | VERIFIED |
| H-03 | `_sum` series | Total observation sum | `sum(http_server_request_duration_sum)` | Latency sum series | VERIFIED |
| H-04 | `rate` on `_bucket` | Per-second bucket rates | `sum by (le) (rate(k6_http_req_duration_bucket[5m]))` | Rate of observations by bucket (k6 fixture; OTLP `http_server_*` is too sparse for `[5m]` under collector backoff) | VERIFIED |

---

## 9. Prometheus Discovery & Protocol APIs

| Item | Endpoint | Method | Purpose | Browser / Datasource Proxy Verification | Status |
|:-----|:---------|:-------|:--------|:----------------------------------------|:-------|
| P-01 | `/api/v1/query` | GET & POST | Instant PromQL query | Returns vector data with current timestamp | VERIFIED |
| P-02 | `/api/v1/query_range` | GET & POST | Range PromQL query | Returns matrix step-grid series data | VERIFIED |
| P-03 | `/api/v1/labels` | GET & POST | Label names discovery | Powers Explore label picker dropdowns | VERIFIED |
| P-04 | `/api/v1/label/{name}/values` | GET & POST | Label values discovery | Powers Explore label value dropdowns | VERIFIED |
| P-05 | `/api/v1/series` | GET & POST | Series matcher discovery | Returns series labelsets matching selector | VERIFIED |
| P-06 | `/api/v1/metadata` | GET | Metric type & description | Returns metric metadata (gauge/counter) | VERIFIED |
| P-07 | `/api/v1/rules` | GET | Alerting/recording rules | Returns `groups: []` without throwing 404/alerts | VERIFIED |
| P-08 | `/api/v1/query_exemplars` | GET & POST | Exemplar discovery | Returns `[]` without throwing 404/alerts | VERIFIED |

---

## 10. Loki (LogQL) Features

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

## 11. Tempo (TraceQL) Features

> **Note on Trace Ingestion:** In the manual Grafana stack, traces stay on the OTel collector `debug` exporter (spanmetrics are produced and ingested into metrics, per `tests/compat/grafana/README.md:34`). Full span-tree retrieval and TraceQL filter evaluations are tested by the dedicated Tempo contract and differential suite (`tests/compat/tempo/` via `make test-tempo-diff` and `tests/compat/grafana/e2e/tempo_tenant_contract_test.sh`). The HTTP protocol endpoints (`/api/search`, `/api/search/tags`, `/api/search/tag/.../values`, `/api/traces/...`) are verified live against Softprobe in `e2e_all_query_features.spec.ts`.

| Item | Feature | Endpoint | Browser / Test Verification | Status |
|:-----|:--------|:---------|:----------------------------|:-------|
| TR-01 | Trace Lookup | `/api/traces/{traceID}` | Protocol not-found / detail responses verified | VERIFIED (Protocol & Contract Suite) |
| TR-02 | Trace Search | `/api/search` with tags & duration | Trace search endpoint envelope verified | VERIFIED (Protocol & Contract Suite) |
| TR-03 | Search Tag Names | `/api/search/tags` | Resource & span tag discovery verified | VERIFIED (Protocol & Contract Suite) |
| TR-04 | Search Tag Values | `/api/search/tag/{tag}/values` | Tag value discovery verified | VERIFIED (Protocol & Contract Suite) |

---

## 12. Real Grafana Settings & Browser Automations

| Item | Capability | Verification Scope | Status |
|:-----|:-----------|:-------------------|:-------|
| G-01 | Headless Browser Login | Playwright logs in as `admin:admin`, skips password change | VERIFIED |
| G-02 | Native Datasources Provisioning | `softprobe-prom`, `softprobe-prom-a`, `softprobe-prom-b`, `softprobe-loki-a`, `softprobe-loki-b`, `softprobe-tempo-a`, `softprobe-tempo-b` all present | VERIFIED |
| G-03 | Cross-Signal Link Wiring | Loki `TraceID` derived field targets Tempo; Tempo `tracesToLogsV2` targets Loki | VERIFIED |
| G-04 | PromQL Capability Dashboards (8) | Aggregations, Operators, Histograms, Over-Time, Overview, Rate, Selectors, Smoke load and render uPlot charts with zero errors | VERIFIED |
| G-05 | Astronomy Shop Service Dashboards (12) | GOLD overview, Ad, Cart, Checkout, Currency & Quote, Frontend, Infra, Loadgen, Payment, Product Catalog, Recommendation, Shipping load and render with live OTel Demo data | VERIFIED |
| G-06 | Interactive Explore Automation | Playwright types each checklist query into Explore, executes query, verifies graph/table renders and HTTP 200 returned | VERIFIED |
