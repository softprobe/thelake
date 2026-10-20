"""Run Lisa's labeled activity-management agent benchmark."""

from __future__ import annotations

import sys
from pathlib import Path

# Offline harness lives outside the installable runtime package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.activity_runner import main

if __name__ == "__main__":
    main()
