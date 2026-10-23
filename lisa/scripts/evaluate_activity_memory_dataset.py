"""Run Lisa's text-only activity-memory benchmark through the live runtime."""

from __future__ import annotations

import sys
from pathlib import Path

# Offline benchmark runner lives outside the installable runtime package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.activity_runner import MEMORY_MANIFEST, main

if __name__ == "__main__":
    main(default_manifest=MEMORY_MANIFEST)
