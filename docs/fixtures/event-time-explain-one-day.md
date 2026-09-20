# EXPLAIN fixture — one-day session fetch (AC6)

**Purpose:** Prove D4 predicates prune unrelated partition days for a one-day session lake fetch.

**Integration lock:** `tests/integration/event_time_prune.rs` —
`one_day_session_fetch_does_not_list_unrelated_day_files`

1. Seed spans on `2026-09-10` and `2026-09-11` (Parquet; `data_inlining_row_limit=0`).
2. Capture the on-disk day-B parquet **basename stem** (`ducklake-<uuid>`). That stem cannot appear in compiled SQL.
3. Compile + `EXPLAIN ANALYZE` a **wide** window covering both days:
   - `Total Files Read: 2`
   - flattened Filename(s) contains the day-B stem (positive control).
4. Compile + `EXPLAIN ANALYZE` the **narrow** `2026-09-10` window:
   - `Total Files Read: 1`
   - flattened Filename(s) must **not** contain the day-B stem.
5. Execute the narrow SQL; row count is 1 (day-B span excluded).

Do **not** assert on the date string `2026-09-11` alone — wide BETWEEN SQL embeds that literal and would make the gate vacuous.

**Compile companion:** `one_day_session_fetch_predicates_do_not_name_unrelated_days` in `api/llm/query.rs`.
