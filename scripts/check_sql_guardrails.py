#!/usr/bin/env python3
"""Ratchet SQL safety rules without grandfathering new violations.

Existing inline SQL is tolerated only while it remains byte-for-byte equivalent
to the selected base revision. Any new or modified SQL-like string literal must
move into a .sql template.

Literals *moved* between changed paths in the same diff are credited: a literal
removed from one changed file may appear in another without counting as new.

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
    # Include deletes so moved SQL can be credited from the old path.
    paths = {
        line.strip()
        for line in git("diff", "--name-only", "--diff-filter=ACMRD", base, "--").splitlines()
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


def skip_rust_lifetime_or_char(text: str, i: int) -> int:
    """Advance past a Rust lifetime (`'a`, `'_`) or char literal (`'x'`, `'\\''`)."""
    if i >= len(text) or text[i] != "'":
        return i + 1
    j = i + 1
    if j >= len(text):
        return len(text)
    # Lifetime / label: 'ident or '_
    if text[j] == "_" or text[j].isalpha():
        j += 1
        while j < len(text) and (text[j].isalnum() or text[j] == "_"):
            j += 1
        return j
    # Char literal
    if text[j] == "\\":
        j += 2 if j + 1 < len(text) else 1
    else:
        j += 1
    if j < len(text) and text[j] == "'":
        j += 1
    return j


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


def without_cfg_test_modules(text: str) -> str:
    """Drop `#[cfg(test)] mod … { … }` bodies so unit-test SQL is not ratcheted.

    Mirrors `src/sql/mod.rs` ownership arch tests: production inline SQL is the
    ratchet target; embedded test fixtures may still assert on SQL text.
    """
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    while i < len(lines):
        trimmed = lines[i].lstrip()
        if trimmed.startswith("#[cfg(test)]"):
            i += 1
            while i < len(lines) and lines[i].lstrip().startswith("#["):
                i += 1
            if i < len(lines) and lines[i].lstrip().startswith("mod "):
                depth = 0
                started = False
                while i < len(lines):
                    for ch in lines[i]:
                        if ch == "{":
                            depth += 1
                            started = True
                        elif ch == "}":
                            depth -= 1
                    i += 1
                    if started and depth <= 0:
                        break
                continue
            continue
        out.append(lines[i])
        i += 1
    return "".join(out)


def sql_literals(text: str, suffix: str) -> list[tuple[str, int]]:
    if suffix == ".rs":
        text = without_cfg_test_modules(text)
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

        # Rust strings are double-quoted only. Single quotes are lifetimes/chars
        # (`<'_>`, `'a', '\\'`) and must not be treated as string delimiters.
        if suffix == ".rs":
            if text[i] == '"':
                body, end = read_standard_string(text, i, '"')
                if is_sql_like(body):
                    out.append((normalize(body), line))
                line += text[i:end].count("\n")
                i = end
                continue
            if text[i] == "'":
                i = skip_rust_lifetime_or_char(text, i)
                continue
        elif text[i] in {'"', "'"}:
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


def path_sql_delta(base: str, path: str) -> tuple[Counter[str], list[tuple[str, int]]] | None:
    """Return (removed_counts, new_literals_with_lines) for a changed code path."""
    if path == SELF:
        return None
    suffix = Path(path).suffix.lower()
    if suffix not in CODE_SUFFIXES:
        return None
    current_path = Path(path)
    old = sql_literals(base_text(base, path), suffix)
    new = (
        sql_literals(current_path.read_text(encoding="utf-8"), suffix)
        if current_path.is_file()
        else []
    )
    old_counts = Counter(value for value, _ in old)
    new_counts = Counter(value for value, _ in new)
    removed: Counter[str] = Counter()
    for value, count in old_counts.items():
        drop = count - new_counts[value]
        if drop > 0:
            removed[value] = drop
    extras: list[tuple[str, int]] = []
    for value, count in new_counts.items():
        extra = count - old_counts[value]
        if extra <= 0:
            continue
        lines = [line for literal, line in new if literal == value][:extra]
        extras.extend((value, line) for line in lines)
    return removed, extras


def check_inline_sql_moves(base: str, paths: list[str]) -> list[str]:
    """Forbid new SQL literals, crediting literals removed from other changed paths."""
    removed_pool: Counter[str] = Counter()
    all_extras: list[tuple[str, str, int]] = []  # path, value, line
    for path in paths:
        delta = path_sql_delta(base, path)
        if delta is None:
            continue
        removed, extras = delta
        removed_pool.update(removed)
        all_extras.extend((path, value, line) for value, line in extras)

    violations: list[str] = []
    for path, value, line in all_extras:
        if removed_pool[value] > 0:
            removed_pool[value] -= 1
            continue
        preview = value[:140] + ("..." if len(value) > 140 else "")
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

    paths = changed_paths(base)
    violations: list[str] = []
    violations.extend(check_inline_sql_moves(base, paths))
    for path in paths:
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
