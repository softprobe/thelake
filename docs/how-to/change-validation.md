# Change validation

Authoritative detail for `AGENTS.md` rules on running tests and evaluations and
real agent execution. The short invariants stay in
[`AGENTS.md`](../../AGENTS.md); follow this page when applying them.

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
[`run-thelake-tests`](../../.agents/skills/run-thelake-tests/SKILL.md). Keep
Docker lifecycle rules from
[`test-environments.md`](test-environments.md).

## Real agent execution validation

For any change to an agent's prompt, skills, tools, hooks, routing, scheduling,
or workflow, validate the actual consumer through a real end-to-end execution.
Run the relevant labeled agent evaluation with its configured model and
runtime; unit tests and mocked integration tests supplement this check and
never replace it. Use the real production entrypoint for the behavior under
evaluation, such as a scheduled service for scheduled behavior. Keep all state
transient and run-owned under the automated-test rules in `AGENTS.md`.

A real agent execution means invoking the actual configured model and runtime
with a fresh run-owned workspace and observing its tool calls and resulting
state. Unit tests, mocks, canned transcripts, replayed traces, or an offline
scorer are not substitutes for that execution.

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
