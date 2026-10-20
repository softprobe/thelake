# Tests and evaluations

Authoritative detail for the consecutive `AGENTS.md` rules on TDD and coverage,
transient Docker test environments, running the covering suites, and real agent
execution. Short invariants stay in [`AGENTS.md`](../../AGENTS.md); follow this
page when applying them.

## TDD, coverage, and transient state

Write a failing test before implementation. Every change needs proportionate
unit, integration, and end-to-end coverage. Prefer one shared contract suite
over copying tests between adapters.

Every automated test or evaluation that writes state must use transient,
run-owned databases and workspaces. Never connect to or mutate a developer's
running service, database, workspace, or other pre-existing state. Provision
resources through shared fixtures/setup and tear them down automatically,
including on failure. Stub external services when live behavior is not part of
the test.

For Lisa specifically, **all** disposable environment setup and teardown lives in
[`lisa/conftest.py`](../../lisa/conftest.py): Postgres via `test_stack.py`,
per-run Lisa databases/roles/schema, and policy-evaluation dogfood bind /
dispose. Pytest fixtures and `lisa/benchmark/` runners must call those helpers;
they must not own Compose, database, or dogfood lifecycle themselves.
Reuse one Postgres container for a pytest session or benchmark run is fine;
when the session or process ends, `conftest` must stop every tracked stack
(fixture `finally`, `pytest_sessionfinish`, and `atexit`) so no orphan
containers remain. Diagnostic artifacts under `target/test-artifacts/` are kept.

## Automated test environments

Automated tests that need Docker use one lifecycle owner:
[`scripts/test_stack.py`](../../scripts/test_stack.py). It creates a uniquely
named Compose project or one labeled Docker container for each run, captures
service logs before teardown, and removes only resources recorded in that run's
state file. Test outputs and logs stay under `target/test-artifacts/`; temporary
workspace files are removed after the run.

Compose files describe service topology and test-only settings. Do not call
`docker compose up/down`, `docker run`, or `docker rm` from test fixtures or
harnesses. Start and stop through the shared script so a failed test cannot
reuse or remove a developer's stack. Developer commands such as `make setup`,
`make grafana-up`, and the dogfood are separate operator workflows and are
not automated-test infrastructure.

### Use from a test harness

```bash
stack_json="$(python3 scripts/test_stack.py start-compose \
  --suite my-suite \
  --compose-file tests/compose.test.yaml \
  --service lisa-postgres --published-port lisa-postgres:5432 --wait)"
state_file="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["state_file"])' "$stack_json")"
# Run the test against the published port returned in stack_json.
python3 scripts/test_stack.py stop --state-file "$state_file"
```

Put the stop command in an `EXIT`/`finally` cleanup path. The `stack.json`
response includes the run ID, project name, scratch directory, artifact
directory, state file, and mapped ports. Artifacts are deliberately retained
for diagnosis; scratch data and Docker volumes are removed during teardown.
If startup partially succeeds and its automatic teardown also fails, the
command retains a `cleanup-required` state and prints the `stop` command to
retry. Use that command after Docker is available again; it verifies the run's
Compose project resources before removing them. GitHub Actions uploads these
stack logs with the test evidence so they remain available after the job ends.

Rust tests get dynamic host endpoints through `THELAKE_TEST_POSTGRES_PORT` and
`THELAKE_TEST_MINIO_ENDPOINT`, exported from the returned `stack.json` by the
harness. There is no fixed-port fallback, so a missing test stack cannot direct
tests to a developer's services.

Single-container compatibility fixtures use the same owner:

```bash
python3 scripts/test_stack.py start-container --suite compat-loki \
  --image grafana/loki:PINNED --docker-option=-p \
  --docker-option=127.0.0.1::3100 --published-port 3100
```

Use `make test`, `make test-e2e`, `make test-perf`, and the browser test targets
to invoke repository harnesses. Each target provisions its own transient
stack. Never
point an automated test at a service started by `make setup` or at any other
pre-existing database, workspace, or Docker project.

The Explorer UI harness uses `scripts/run-explorer-e2e-services.py` to bind
TheLake and Lisa HTTP sockets before starting either service. The supervisor
passes the bound listeners to both processes, writes their assigned ports for
the browser harness, and stops both services during cleanup.

## Run the relevant tests and evaluations

Changing code without executing the appropriate checks is not allowed. After
every change, run the relevant unit, integration, end-to-end, and labeled
evaluation suites that cover the touched behavior; report what ran and the
outcomes.

Keep lasting regression coverage in the existing test module for the same
component and contract; create a new test module only for a distinct subsystem
or test layer. Do not add one-off verification tests or scripts to the tracked
codebase. When a temporary script is the clearest way to verify a change, put
it in `/tmp` or another excluded location. Use `./tmp` only when Git confirms
that path is ignored. Run the script and remove it when finished.

Route suite selection through
[`run-thelake-tests`](../../.agents/skills/run-thelake-tests/SKILL.md).

## Real agent execution validation

For any change to an agent's prompt, skills, tools, hooks, routing, scheduling,
or workflow, validate the actual consumer through a real end-to-end execution.
Run the relevant labeled agent evaluation with its configured model and
runtime; unit tests and mocked integration tests supplement this check and
never replace it. Use the real production entrypoint for the behavior under
evaluation, such as a scheduled service for scheduled behavior. Keep all state
transient and run-owned under the rules above.

A real agent execution means invoking the actual configured model and runtime
with a fresh run-owned workspace and observing its tool calls and resulting
state. Unit tests, mocks, canned transcripts, replayed traces, or an offline
scorer are not substitutes for that execution.

Offline Lisa dataset loaders and benchmark runners live under
[`lisa/benchmark/`](../../lisa/benchmark/README.md), not the installable
runtime package. Agentic held-out splits use the name `benchmark`, not `test`
(`test` means traditional software unit/integration tests only).

### Credentials and `.env`

Before declaring model credentials unavailable, inspect the repository's
documented environment sources, including the repository-root `.env` when
present, and the current process environment. A credential in `.env` counts as
configured even when the shell did not export it. Load that file through the
project's supported dotenv/configuration mechanism before checking or running
the entrypoint. Check only whether required values are present; never print,
log, copy into reports, or commit secret values. Do not ask the user to provide
a credential until these sources have been checked. If no configured
credentials exist, report that the real execution could not run and make no
agent-performance claim.

### Before/after claims

For before/after performance claims, capture a baseline before changing agent
behavior and compare it with the same pinned evaluation cases, model, and
runtime. If no comparable baseline exists, report current results without
attributing a measured improvement to the changes.
