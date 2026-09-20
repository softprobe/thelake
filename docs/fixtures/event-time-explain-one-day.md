# EXPLAIN fixture — one-day session fetch (AC6)

**Purpose:** Prove D4 predicates prune unrelated partition days for a one-day session lake fetch.

**Integration lock:** `tests/integration/event_time_prune.rs` —
`one_day_session_fetch_does_not_list_unrelated_day_files`

1. Seed spans on `2026-09-10` and `2026-09-11` (Parquet; `data_inlining_row_limit=0`).
2. Compile + execute session observations SQL for the `2026-09-10` window only.
3. Assert row count is 1 (day-B span excluded).
4. Assert `EXPLAIN` text does **not** contain `2026-09-11`.

**Compile companion:** `one_day_session_fetch_predicates_do_not_name_unrelated_days` in `api/llm/query.rs`.
