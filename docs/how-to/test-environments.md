# Automated test environments

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
`make grafana-up`, and the quickstart are separate operator workflows and are
not automated-test infrastructure.

## Use from a test harness

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
