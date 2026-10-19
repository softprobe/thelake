"""Production Lisa evaluation: trajectory, triage, judge, and scores.

Anti-pattern (do not repeat): offline dataset loaders, benchmark scorers, and
CLI runners do NOT belong in this package. They live under
``lisa/benchmarks/lisa_benchmarks/`` so they are not part of the installable
runtime and cannot be mistaken for production evaluation.

If you are about to add ``activity_dataset``, ``dataset_benchmark``, or similar
here, stop — put it in ``lisa_benchmarks`` instead. Runtime code must never
import ``lisa_benchmarks``.
"""
