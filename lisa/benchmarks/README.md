# Lisa offline benchmarks

Harness code for labeled dataset benchmarks. This tree is **not** the Lisa
runtime and is **not** copied into the Lisa service image.

| Concern | Location |
|---------|----------|
| Production evaluation (triage, judge, scores) | `lisa/src/lisa/evaluation/` |
| Offline loaders and runners | `lisa/benchmarks/lisa_benchmarks/` |
| Frozen cases / manifests | `lisa/datasets/` |

Do not move benchmark modules back under `lisa/src/lisa/evaluation/`. See the
package docstring in `lisa_benchmarks/__init__.py`. Agentic held-out splits use
the name `benchmark`, not `test` (`test` is reserved for pytest).
