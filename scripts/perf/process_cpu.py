"""External /proc/<pid>/stat CPU sampling for load benches.

Ratio units match `thelake.process.cpu_ratio` / bench-demo-cpu-full:
1.0 = one full core over the sample interval (Linux USER_HZ=100).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from percentiles import percentile_ms

# Linux USER_HZ is almost always 100; keep lockstep with self_monitoring.
USER_HZ = 100.0


def read_jiffies(pid: int) -> int | None:
    """Return utime+stime jiffies for pid, or None if the process is gone.

    Parses after \") \" so comm names with spaces match self_monitoring.
    """
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    rest = text.rsplit(") ", 1)
    if len(rest) != 2:
        return None
    fields = rest[1].split()
    # After ") ": state=[0], … utime=[11], stime=[12] (1-based fields 14/15).
    if len(fields) < 13:
        return None
    try:
        return int(fields[11]) + int(fields[12])
    except ValueError:
        return None


def cpu_ratio(prev_jiffies: int, next_jiffies: int, elapsed_s: float) -> float:
    """Cores used between two jiffy snapshots."""
    if elapsed_s <= 0:
        return 0.0
    dj = max(0, next_jiffies - prev_jiffies)
    return (dj / USER_HZ) / elapsed_s


def sample_window(
    pid: int,
    duration_s: float,
    *,
    interval_s: float = 1.0,
    stop_at: float | None = None,
) -> list[float]:
    """Sample process CPU ratio for up to duration_s (or until stop_at)."""
    samples: list[float] = []
    interval = max(0.1, interval_s)
    deadline = time.perf_counter() + max(0.0, duration_s)
    if stop_at is not None:
        deadline = min(deadline, stop_at)

    prev = read_jiffies(pid)
    prev_t = time.perf_counter()
    if prev is None:
        return samples

    while True:
        now = time.perf_counter()
        if now >= deadline:
            break
        time.sleep(min(interval, max(0.0, deadline - now)))
        cur = read_jiffies(pid)
        cur_t = time.perf_counter()
        if cur is None:
            break
        samples.append(cpu_ratio(prev, cur, cur_t - prev_t))
        prev, prev_t = cur, cur_t
    return samples


def summarize_cpu(samples: list[float]) -> dict[str, Any]:
    """Summarize core-ratio samples (not milliseconds — reuse percentile helper)."""
    if not samples:
        return {
            "count": 0,
            "mean_cores": None,
            "p50_cores": None,
            "p95_cores": None,
            "p99_cores": None,
            "max_cores": None,
            "unit": "cores (1.0 = one full core)",
        }
    mean = sum(samples) / len(samples)
    return {
        "count": len(samples),
        "mean_cores": round(mean, 4),
        "p50_cores": round(percentile_ms(samples, 50) or 0.0, 4),
        "p95_cores": round(percentile_ms(samples, 95) or 0.0, 4),
        "p99_cores": round(percentile_ms(samples, 99) or 0.0, 4),
        "max_cores": round(max(samples), 4),
        "unit": "cores (1.0 = one full core)",
    }


def resolve_pid(*, pid: int | None = None, base_url: str | None = None) -> int | None:
    """Use explicit pid, else find listener on base_url host:port via /proc/net/tcp."""
    if pid is not None and pid > 0:
        return pid
    if not base_url:
        return None
    parsed = urlparse(base_url)
    host = parsed.hostname or "127.0.0.1"
    if host not in ("127.0.0.1", "localhost", "::1"):
        return None
    port = parsed.port
    if not port:
        return None
    return _pid_listening_on_port(port)


def _pid_listening_on_port(port: int) -> int | None:
    """Best-effort: match local TCP listen inode to /proc/*/fd."""
    port_hex = f"{port:04X}"
    inodes: set[str] = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = Path(table).read_text().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 10:
                continue
            local = parts[1]
            state = parts[3]
            inode = parts[9]
            # state 0A = LISTEN
            if state != "0A":
                continue
            if ":" not in local:
                continue
            _, phex = local.rsplit(":", 1)
            if phex.upper() == port_hex:
                inodes.add(inode)
    if not inodes:
        return None
    proc = Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        try:
            for fd in fd_dir.iterdir():
                try:
                    target = fd.readlink()
                except OSError:
                    continue
                text = str(target)
                if text.startswith("socket:[") and text.endswith("]"):
                    inode = text[len("socket:[") : -1]
                    if inode in inodes:
                        return int(entry.name)
        except OSError:
            continue
    return None
