# Design: temporary MAP bags + prefer-promoted SQL

## Bag type

Restore `MAP(VARCHAR, VARCHAR)` for hot attribute bags. Proven Postgres-inline
path (scores metadata), matches pre-#11 physical type, keeps OTel string-map
model and existing `CAST(col['k'] AS VARCHAR)` / `CAST(col AS JSON)` query forms.

Out of scope (already MAP): `scores.metadata`, nested `traces.events[].attributes`.

## Inlining

Catalog-global `DATA_INLINING_ROW_LIMIT` default is **10_000**:

- MAP bags are Postgres-inline-safe; #55 re-enables small-batch inlining.
- Metrics **AC-F7** is wait-for-next-run: TWCS merges live Parquet only and does
  **not** flush catalog-inlined rows every maintenance pass.
- Per-write: batches over the limit write Parquet (merged later); prior ≤limit
  batches remain inlined unless something else flushes (no auto-promote).
- Downsample reads the DuckLake table (inlined ∪ Parquet); closed-bucket lag
  keeps accuracy.

Primary wins: remove VARIANT cast/shredding CPU **and** avoid Parquet-per-small-batch
write amplification.

## Prefer-promoted rule

Shared helpers emit `COALESCE(promoted_col, CAST(bag['key'] AS VARCHAR), …)`
with promoted first when a matching active promotion exists. Bag-only when no
promotion. Compilers: LLM query, telemetry, Prom labels, Loki, Tempo.

## Promotion shipping

Versioned YAMLs under `docs/promotion/`; tenant `POST /v1/promotions/apply` only.
Demo/bench scripts apply all hot manifests at bring-up (same pattern as
`metrics-prom-hot-labels.yaml`).

## Full-OTLP CPU gate

Profile sends traces+logs+metrics to Softprobe at demo load-generator rates.
Grafana keeps `refresh=10s` during measure. Softprobe pinned to one core.
Host samples `/proc/<pid>/stat`; mean CPU ratio must be &lt; 0.85 (durable
headroom under one-core pin). External
sampling (not self-mon export) so the gate does not compete for the core.

## Migration

Fail-fast if hot columns are still VARIANT. Operator rebuilds catalog/data path
(same class of break as MAP→VARIANT). No auto-drop.
