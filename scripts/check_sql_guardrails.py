#!/usr/bin/env python3
"""Ratchet SQL safety rules without grandfathering new violations.

Existing inline SQL is tolerated only while it remains byte-for-byte equivalent
to the selected base revision. Any new or modified SQL-like string literal must
move into a .sql template.

Usage:
  python3 scripts/check_sql_guardrails.py [BASE_SHA]

When BASE_SHA is omitted, a dirty worktree is compared with HEAD; a clean
worktree is compared with HEAD^ so the command is useful after a local commit.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
import subprocess
import sys

SELF = "scripts/check_sql_guardrails.py"
CODE_SUFFIXES = {".rs", ".py", ".sh", ".bash", ".zsh", ".ts", ".tsx", ".js", ".jsx"}

FACT_SOURCE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:(?:[A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\")\.)*"
    r"\"?(traces|logs)\"?\b",
    re.IGNORECASE,
)
LOWER_TIME_BOUND = re.compile(r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)?timestamp\s*(?:>=|>)", re.IGNORECASE)
UPPER_TIME_BOUND = re.compile(r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)?timestamp\s*(?:<=|<)", re.IGNORECASE)
BETWEEN_TIME_BOUND = re.compile(
    r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)?timestamp\s+BETWEEN\b", re.IGNORECASE
)

SQL_PATTERNS = [
    re.compile(r"\bSELECT\b[\s\S]*\b(?:FROM|WHERE|JOIN|UNION|GROUP\s+BY|ORDER\s+BY|LIMIT)\b", re.IGNORECASE),
    re.compile(r"\b(?:FROM|JOIN)\s+(?:[A-Za-z_\"][A-Za-z0-9_\".]*\.)?\"?(?:traces|logs|scores)\"?\b", re.IGNORECASE),
    re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE),
    re.compile(r"\bUPDATE\b[\s\S]*\bSET\b", re.IGNORECASE),
    re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE),
    re.compile(r"\bCREATE\s+(?:(?:OR\s+REPLACE|TEMP(?:ORARY)?)\s+)?(?:TABLE|VIEW|SCHEMA)\b", re.IGNORECASE),
    re.compile(r"\bALTER\s+TABLE\b", re.IGNORECASE),
    re.compile(r"\bDROP\s+(?:TABLE|VIEW|SCHEMA)\b", re.IGNORECASE),
    re.compile(r"\b(?:ATTACH|DETACH|EXPLAIN|PRAGMA)\b", re.IGNORECASE),
    re.compile(r"\b(?:timestamp|trace_id|session_id|span_id)\s*(?:=|<>|!=|>=|<=|>|<|\bIN\b|\bIS\b)", re.IGNORECASE),
]


def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout


def commit_exists(rev: str) -> bool:
    return subprocess.run(
        ["git", "cat-file", "-e", f"{rev}^{{commit}}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def choose_base() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()
    if git("status", "--porcelain").strip():
        return "HEAD"
    return "HEAD^"


def changed_paths(base: str) -> list[str]:
    paths = {
        line.strip()
        for line in git("diff", "--name-only", "--diff-filter=ACMR", base, "--").splitlines()
        if line.strip()
    }
    if base == "HEAD":
        paths.update(
            line.strip()
            for line in git("ls-files", "--others", "--exclude-standard").splitlines()
            if line.strip()
        )
    return sorted(paths)


def base_text(base: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{base}:{path}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.stdout if result.returncode == 0 else ""


def skip_comment(text: str, i: int, suffix: str) -> int | None:
    if suffix in {".rs", ".ts", ".tsx", ".js", ".jsx"}:
        if text.startswith("//", i):
            end = text.find("\n", i + 2)
            return len(text) if end < 0 else end
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            return len(text) if end < 0 else end + 2
    if suffix in {".py", ".sh", ".bash", ".zsh"} and text.startswith("#", i):
        end = text.find("\n", i + 1)
        return len(text) if end < 0 else end
    return None


def read_standard_string(text: str, i: int, quote: str) -> tuple[str, int]:
    triple = text.startswith(quote * 3, i)
    delimiter = quote * (3 if triple else 1)
    start = i + len(delimiter)
    j = start
    escaped = False
    while j < len(text):
        if not triple and escaped:
            escaped = False
            j += 1
            continue
        if not triple and text[j] == "\\":
            escaped = True
            j += 1
            continue
        if text.startswith(delimiter, j):
            return text[start:j], j + len(delimiter)
        j += 1
    return text[start:], len(text)


def read_rust_raw_string(text: str, i: int) -> tuple[str, int] | None:
    if text[i] != "r":
        return None
    j = i + 1
    hashes = 0
    while j < len(text) and text[j] == "#":
        hashes += 1
        j += 1
    if j >= len(text) or text[j] != '"':
        return None
    delimiter = '"' + ("#" * hashes)
    start = j + 1
    end = text.find(delimiter, start)
    if end < 0:
        return text[start:], len(text)
    return text[start:end], end + len(delimiter)


def sql_literals(text: str, suffix: str) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    i = 0
    line = 1
    while i < len(text):
        raw = read_rust_raw_string(text, i) if suffix == ".rs" else None
        if raw is not None:
            body, end = raw
            if is_sql_like(body):
                out.append((normalize(body), line))
            line += text[i:end].count("\n")
            i = end
            continue

        comment_end = skip_comment(text, i, suffix)
        if comment_end is not None:
            line += text[i:comment_end].count("\n")
            i = comment_end
            continue

        if text[i] in {'"', "'"}:
            body, end = read_standard_string(text, i, text[i])
            if is_sql_like(body):
                out.append((normalize(body), line))
            line += text[i:end].count("\n")
            i = end
            continue

        if text[i] == "\n":
            line += 1
        i += 1
    return out


def normalize(value: str) -> str:
    return " ".join(value.split())


def is_sql_like(value: str) -> bool:
    candidate = value.strip()
    if not candidate:
        return False
    return any(pattern.search(candidate) for pattern in SQL_PATTERNS)


def check_inline_sql(base: str, path: str) -> list[str]:
    if path == SELF:
        return []
    suffix = Path(path).suffix.lower()
    if suffix not in CODE_SUFFIXES:
        return []
    current_path = Path(path)
    if not current_path.is_file():
        return []
    old = sql_literals(base_text(base, path), suffix)
    new = sql_literals(current_path.read_text(encoding="utf-8"), suffix)
    old_counts = Counter(value for value, _ in old)
    new_counts = Counter(value for value, _ in new)
    violations: list[str] = []
    for value, count in new_counts.items():
        extra = count - old_counts[value]
        if extra <= 0:
            continue
        lines = [line for literal, line in new if literal == value][:extra]
        preview = value[:140] + ("..." if len(value) > 140 else "")
        for line in lines:
            violations.append(
                f"{path}:{line}: new inline SQL is forbidden; move it to a .sql template: {preview}"
            )
    return violations


def check_sql_template(path: str) -> list[str]:
    if not path.endswith(".sql"):
        return []
    file = Path(path)
    if not file.is_file():
        return []
    sql = file.read_text(encoding="utf-8")
    if not FACT_SOURCE.search(sql):
        return []
    has_window_placeholder = "{{timestamp_filter}}" in sql
    has_explicit_window = bool(BETWEEN_TIME_BOUND.search(sql)) or (
        bool(LOWER_TIME_BOUND.search(sql)) and bool(UPPER_TIME_BOUND.search(sql))
    )
    if has_window_placeholder or has_explicit_window:
        return []
    return [
        f"{path}: traces/logs template must contain {{timestamp_filter}} or explicit lower+upper timestamp bounds"
    ]


def main() -> int:
    base = choose_base()
    if not commit_exists(base):
        print(f"SQL guardrail error: base commit {base!r} is not available", file=sys.stderr)
        return 2

    violations: list[str] = []
    paths = changed_paths(base)
    for path in paths:
        violations.extend(check_inline_sql(base, path))
        violations.extend(check_sql_template(path))

    if violations:
        print("SQL guardrails failed:", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        print(
            "\nRules: query SQL lives in .sql templates; traces/logs reads use a finite time window.",
            file=sys.stderr,
        )
        return 1

    print(f"SQL guardrails ok ({len(paths)} changed paths checked against {base})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
