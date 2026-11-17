# Postgres Schema Migrations (Atlas)

`thelake` separates schema management across three layers:

1. **Postgres control and operational tables (Atlas):**
   Tracked in `migrations/` with checksums in `migrations/atlas.sum` and applied history in `atlas_schema_revisions`. `atlas.hcl` sets `exclude = ["ducklake_*"]` so Atlas never inspects or modifies DuckLake's internal catalog tables living in the same Postgres schema.
2. **DuckLake catalog & additive Parquet evolution (DuckLake):**
   DuckLake upgrades its internal metadata (`AUTOMATIC_MIGRATION true`) and handles additive columns (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) without rewriting existing Parquet files in GCS.
3. **Breaking Parquet type rewrites (blue/green Python job):**
   Rare breaking Parquet rewrites (such as `MAP(VARCHAR, VARCHAR)` to `VARIANT`) are executed via `scripts/migrate_lakehouse.py`, which tracks per-partition progress in `_migration_progress`.

## Common commands

```bash
export ATLAS_DATABASE_URL="postgres://user:pass@host:5432/softprobe?search_path=thelake_staging_ducklake11&sslmode=disable"
export ATLAS_REVISIONS_SCHEMA="thelake_staging_ducklake11"

# Recompute atlas.sum after adding or editing a migration file
docker run --rm -v "$PWD:/work" -w /work arigaio/atlas migrate hash --dir file://migrations

# Validate migration directory integrity
docker run --rm -v "$PWD:/work" -w /work arigaio/atlas migrate validate --dir file://migrations

# Check pending migrations
docker run --rm -v "$PWD:/work" -w /work -e ATLAS_DATABASE_URL -e ATLAS_REVISIONS_SCHEMA \
  arigaio/atlas migrate status --env runtime

# Apply pending migrations (use --baseline 20261117000000 once on an already-initialized schema)
docker run --rm -v "$PWD:/work" -w /work -e ATLAS_DATABASE_URL -e ATLAS_REVISIONS_SCHEMA \
  arigaio/atlas migrate apply --env runtime
```
