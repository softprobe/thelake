"""Latency percentile helpers for load benches."""

from __future__ import annotations


def percentile_ms(samples_ms: list[float], p: int) -> float | None:
    """Nearest-rank percentile; p in 0..100. Empty → None."""
    if not samples_ms:
        return None
    if p <= 0:
        return float(min(samples_ms))
    if p >= 100:
        return float(max(samples_ms))
    ordered = sorted(samples_ms)
    # ceil(n * p / 100) index, 1-based → 0-based
    idx = (len(ordered) * p + 99) // 100 - 1
    idx = max(0, min(idx, len(ordered) - 1))
    return float(ordered[idx])


def summarize(samples_ms: list[float]) -> dict:
    return {
        "count": len(samples_ms),
        "p50_ms": percentile_ms(samples_ms, 50),
        "p95_ms": percentile_ms(samples_ms, 95),
        "p99_ms": percentile_ms(samples_ms, 99),
    }
