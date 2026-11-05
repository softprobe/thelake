# Health

Health tells an operator whether the process should stay up. It does not check Postgres.

## Sub-features

- `health-ok` returns status ok while query rebuild failures stay under the limit.
- `ready-ok` returns success after the process can run a statement.

## How to get to it (user POV)

- `GET http://127.0.0.1:18090/health`
- `GET http://127.0.0.1:18090/ready`

## Driving it with curl

Preconditions:

- `launch.sh` has printed `ready`.
- `doctor.sh` has printed `doctor ok`.

- **Liveness.** Run `curl -sS -D /tmp/verify-thelake-evidence/health.headers -o /tmp/verify-thelake-evidence/health.json http://127.0.0.1:18090/health`. The status line is `200` and the body has `"status": "ok"`.
- **Readiness.** Run `curl -sS -D /tmp/verify-thelake-evidence/ready.headers -o /tmp/verify-thelake-evidence/ready.json http://127.0.0.1:18090/ready`. The status line is `200`.

## Gotchas

- `/health` stays ok when Postgres is down. Use `/ready` when the check needs a live engine.
- The first `/ready` builds the default workspace engine and can take many seconds.
- A process on port 8090 is not this run. This skill listens on 18090 unless `VERIFY_THELAKE_PORT` is set.
- Startup exits before `/health` when `ducklake.extension_path` does not point at a real `ducklake.duckdb_extension`. Run `make ducklake-extension` and set that path. `dist/` only has the file after `make build-release`.
