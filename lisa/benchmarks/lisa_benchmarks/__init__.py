"""Offline Lisa benchmark harness — not part of the runtime package.

Anti-pattern (do not repeat): dataset loaders, scorers, and live benchmark
runners must NOT live under ``lisa.evaluation`` or any other installable
runtime module. That package is production policy-evaluation code
(trajectory, triage, judge, scores). Mixing offline harness into it ships
eval tooling with the service image and blurs the production boundary.

Allowed dependency direction: this package may import ``lisa.*`` runtime
APIs. Runtime ``lisa.*`` must never import ``lisa_benchmarks``.

Keep loaders and runners here (or under ``lisa/scripts`` / ``lisa/tests``).
Frozen case bytes stay under ``lisa/datasets/``.

Naming: agentic held-out splits and CLI flags use ``benchmark``, not ``test``.
``test`` means traditional software unit/integration tests only.
"""
