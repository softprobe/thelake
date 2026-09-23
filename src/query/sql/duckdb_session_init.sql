-- DuckDB per-connection session settings.
--
-- Extension loading, object-store setup, retry settings, and resource caps are
-- owned by DuckLakeSessionFactory. Keep this script free of dynamic values.
-- Version guessing lets a worker serve a cached DuckLake snapshot instead of
-- reading the latest committed version from the catalog, which made freshly
-- ingested data invisible to interactive queries for minutes at a time.
-- Correctness/compatibility (#25) requires read-after-write visibility; the
-- catalog roundtrip per query is cheap next to serving stale evidence.
SET unsafe_enable_version_guessing = false;
