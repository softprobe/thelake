# Lisa evaluation terms

This page is the shared definition for Lisa documentation, development skills,
and code comments. Use these terms when discussing policy evaluation.

The short IDs below are examples of meaning. The policy-evaluation fixture uses
the same session and policy IDs as the runtime contract.

| Term | Definition | Example |
|---|---|---|
| **Session** | One user-facing conversation or job in TheLake, identified by `session_id`. The value is a string resolved from session telemetry; TheLake does not require UUIDs or a particular length. If no explicit session ID is present, TheLake may fall back to `trace_id` for correlation, which does not make it an explicit product session. | `S017` |
| **Trajectory** | The ordered user, assistant, context, and tool events Lisa projects from a session's spans for triage and judging. It is data derived from a session, not a second object with its own ID. | Session `S017` projects to a trajectory; Lisa still identifies the input as `S017`. |
| **Trace** | A group of spans within a session that share a `trace_id`. A session can contain more than one trace. Trace IDs are evidence references, not the evaluation input's ID. | `trace-8` belongs to session `S017`. |
| **Policy** | One rule record in Lisa's policy catalog. Its readable `policy_id` is the document section ID used in Lisa, such as `P1.2`. Approval determines whether Lisa may evaluate it. | `P1.2` — “Confirm before issuing a refund.” |
| **Annotation** | One dataset judgment for a session-policy pair. It has no separate identifier. | (`S017`, `P1.2`) is labeled `violation`. |
| **Runtime evaluation pair** | The unit Lisa evaluates: `(session_id, policy_id)`. | (`S017`, `P1.2`) |
| **Dataset pair** | The same pair key used for evaluation: `(session_id, policy_id)`. | (`S017`, `P1.2`) |
| **Activity** | One inspectable Lisa action that can include multiple session-policy pairs and their triage and judgment results. | Lisa evaluates session `S017` against its approved policies in one activity. |

Use `session_id` for the runtime evaluation input's ID. Do not introduce
`trajectory_id` for the same session. Use `policy_id` for the policy's readable
document section ID. An activity ID identifies a Lisa action; it is not the
runtime evaluation pair ID. Evidence hashes may detect changed
content but are not human-facing identifiers.

Dataset annotations and runtime evaluations use the same `(session_id,
policy_id)` pair. The trajectory is derived from its session, and annotation
metadata is only scoring truth; it is not passed into the evaluator.

TheLake's base definition of a product session is in the [telemetry data
model](../sdk/data-model.md). Lisa's `trajectory` is the evidence projection
described above, and its event serialization is documented in the [judge
trajectory format](../reference/lisa-trajectory-format.md).
