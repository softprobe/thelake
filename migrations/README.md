# Postgres Schema Migrations (Atlas)

`thelake` separates schema management across three layers:

1. **Postgres control and operational tables (Atlas):**
   Tracked in `migrations/` with checksums in `migrations/atlas.sum` and applied history in `atlas_schema_revisions`. `atlas.hcl` sets `exclude = ["ducklake_*"]` so Atlas never inspects or modifies DuckLake's internal catalog tables living in the same Postgres schema.
2. **DuckLake catalog & additive Parquet evolution (DuckLake):**
   DuckLake upgrades its internal metadata (`AUTOMATIC_MIGRATION true`) and handles additive columns (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) without rewriting existing Parquet files in GCS.
3. **Breaking Parquet type rewrites (blue/green Python job):**
   Rare breaking Parquet rewrites (such as `MAP(VARCHAR, VARCHAR)` to `VARIANT`) are executed via `scripts/migrate_lakehouse.py`, which tracks per-partition progress in `_migration_progress`.

## Blue/Green Naming Convention

When a breaking Parquet rewrite creates a new target catalog and GCS prefix, name them after the **`thelake` release version** that introduces the layout:

| Artifact | Pattern | Example (`prod`, `v0.1.34`) |
| :--- | :--- | :--- |
| **Postgres metadata schema** | `thelake_<env>_v<major>_<minor>_<patch>` | `thelake_prod_v0_1_34` |
| **GCS data prefix** | `<env>/shared-v<major>.<minor>.<patch>` | `prod/shared-v0.1.34` |
| **Temp clone schema** | `thelake_<env>_mig_src` | `thelake_prod_mig_src` (auto-dropped) |

- Postgres schemas use underscores (`v0_1_34`) because unquoted SQL identifiers cannot contain dots.
- Keep at most two generations per environment (the active schema/prefix and the previous rollback target), and drop older schemas and GCS prefixes once cutover is verified.

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
