"""Live performance-evaluation runners for Lisa (not unit tests, not data).

Java-style layout under ``lisa/``:

- ``src/lisa/`` — production code (main)
- ``tests/`` — all software unit/integration tests
- ``datasets/`` — labeled resources + their loaders
- ``benchmark/`` — this package: live runners only

Loaders live in ``datasets`` because they are tied to the labeled release.
Runtime ``lisa.*`` must not import ``benchmark`` or ``datasets``.
Tests may import either to unit-test loaders/scorers/runners.

Held-out agentic splits are named ``benchmark``, never ``test`` (pytest only).
"""
