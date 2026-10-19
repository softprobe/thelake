# SQL safety invariants

Authoritative detail for the `AGENTS.md` SQL safety rule. These are hard
repository contracts, not review preferences. Schema and one-clock design live
in [`architecture/sql-and-schema.md`](../architecture/sql-and-schema.md).

1. **No query SQL in code string literals.** Put query statements in `.sql`
   files and load/render them from code. Small non-query fragments
   (identifiers, predicates, projection fragments) may be composed by
   typed/query-builder helpers, but must not contain complete SQL statements.
2. **Every `traces` or `logs` read is finitely time-bounded.** It must carry
   both a lower and an upper bare-`timestamp` predicate so DuckLake can
   partition-prune. Never add an unbounded historical scan, including
   lookup/detail queries that already have trace/session IDs.
3. **Do not bypass the execution gate.** Fact-table SQL must execute through
   the checked query/engine paths. Do not add raw DuckDB execution sites to
   work around a rejected query.
4. **Fix the harness instead of adding exceptions.** If a legitimate query is
   rejected, preserve the invariant and improve the template/builder/gate. Do
   not add a file, directory, or call-site exemption merely to make CI green.

`scripts/check_sql_guardrails.py` blocks newly introduced inline SQL and checks
changed `.sql` templates that read traces/logs. Runtime DuckDB plan checks
remain the final enforcement layer for time bounds.
