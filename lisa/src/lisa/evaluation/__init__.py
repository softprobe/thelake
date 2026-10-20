"""Production Lisa evaluation: trajectory, triage, judge, and scores.

Anti-pattern (do not repeat): labeled resources and loaders belong in
``lisa/datasets/``; live runners belong in ``lisa/benchmark/``; software tests
belong in ``lisa/tests/``. Do not put those under this installable runtime
package. Runtime code must never import ``benchmark`` or ``datasets``.
"""
