variable "database_url" {
  type    = string
  default = getenv("ATLAS_DATABASE_URL")
}

variable "revisions_schema" {
  type    = string
  default = getenv("ATLAS_REVISIONS_SCHEMA")
}

env "runtime" {
  url = var.database_url

  # DuckLake owns ducklake_* catalog tables in the same Postgres schema.
  # Exclude them so Atlas never inspects, diffs, or drops DuckLake metadata.
  exclude = ["ducklake_*"]

  migration {
    dir              = "file://migrations"
    revisions_schema = var.revisions_schema != "" ? var.revisions_schema : null
  }
}
