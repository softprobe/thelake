## REMOVED Requirements

### Requirement: Legacy Iceberg and buffer SQL aliases
~~thelake SHALL rewrite historical SQL table aliases (`union_spans`, `union_logs`,
`committed_*`, `buffer_*`, `staged_*`, `iceberg_*`, and intermediate `tm_*`
tokens) to the committed DuckLake telemetry tables.~~

#### Scenario: Legacy alias no longer rewritten
- **WHEN** a client submits `POST /v1/query/sql` with `FROM union_spans` (or
  another Iceberg/buffer-era alias)
- **THEN** the engine does not rewrite that identifier to `traces` / `logs`

## ADDED Requirements

### Requirement: Public SQL names are registry tables only
thelake SHALL accept bare public SQL table names `traces`, `logs`, and `scores`
on `POST /v1/query/sql`. Those bare names MUST be expanded to the tenant's
qualified DuckLake catalog table before execution. Historical Iceberg/buffer
aliases MUST NOT be rewritten.

#### Scenario: Bare traces qualifies against attached catalog
- **WHEN** a client runs `SELECT … FROM traces WHERE timestamp >= …`
- **THEN** the engine expands `traces` to the attached catalog-qualified table
  before DuckDB executes the statement

#### Scenario: Scores qualify the same way
- **WHEN** a client runs a bounded scan against bare `scores`
- **THEN** the engine expands `scores` to the attached catalog-qualified table

#### Scenario: Legacy union_spans is not a public name
- **WHEN** a client runs `SELECT … FROM union_spans …`
- **THEN** the statement is not rewritten to `traces` and fails as an unknown
  relation (or equivalent catalog error)
