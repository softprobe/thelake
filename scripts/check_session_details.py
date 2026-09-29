#!/usr/bin/env python3
"""Load every session detail through the public API and validate full payloads.

Set THELAKE_API_TOKEN and pass the inclusive search window. Provide one known
inline session and one Parquet-backed session to make both storage paths part of
the check, even when one of them falls outside the search window.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class Failure:
    session_id: str
    status: str


def request_json(url: str, token: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}") from None
    except urllib.error.URLError as error:
        raise RuntimeError(f"request failed: {error.reason}") from None
    if not isinstance(value, dict):
        raise RuntimeError("response is not a JSON object")
    return value


def fetch_session_ids(
    base_url: str, token: str, start: str, end: str
) -> tuple[list[str], int]:
    session_ids: list[str] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    pages = 0

    while True:
        body = {
            "from": start,
            "to": end,
            "roots_only": False,
            "order_by": "start_time",
            "order": "desc",
            "limit": 100,
            "cursor": cursor,
        }
        response = request_json(f"{base_url}/v1/llm/sessions/search", token, body)
        if response.get("cursor_supported") is not True:
            raise RuntimeError("session search does not support the cursor order required to enumerate all sessions")
        pages += 1
        items = response.get("items")
        if not isinstance(items, list):
            raise RuntimeError("session search response has no items array")
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("session_id"), str):
                session_ids.append(item["session_id"])

        next_cursor = response.get("next_cursor")
        if not next_cursor:
            return list(dict.fromkeys(session_ids)), pages
        if next_cursor in seen_cursors:
            raise RuntimeError("session search repeated a cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor


def validate_detail_payload(
    session_id: str, detail: dict, require_event: bool = False
) -> Failure | None:
    if detail.get("session_id") != session_id:
        return Failure(session_id, "response session_id mismatch")
    spans = detail.get("spans")
    if not isinstance(spans, list):
        return Failure(session_id, "response has no spans array")
    span_count = detail.get("span_count")
    if not isinstance(span_count, int) or span_count != len(spans):
        return Failure(
            session_id,
            f"span_count mismatch (reported {span_count!r}, returned {len(spans)})",
        )
    has_event = False
    for index, span in enumerate(spans):
        if not isinstance(span, dict):
            return Failure(session_id, f"span {index} is not an object")
        events = span.get("events")
        if not isinstance(events, list):
            return Failure(session_id, f"span {index} has no events array")
        has_event = has_event or bool(events)
    if require_event and not has_event:
        return Failure(session_id, "storage-path probe returned no events")
    if not isinstance(detail.get("scores"), list):
        return Failure(session_id, "response has no scores array")
    return None


def validate_recording_payload(session_id: str, recording: dict) -> Failure | None:
    if recording.get("session_id") != session_id:
        return Failure(session_id, "recording response session_id mismatch")
    if recording.get("truncated") is True:
        return Failure(session_id, "recording response is truncated")
    batches = recording.get("batches")
    events = recording.get("events")
    if not isinstance(batches, list) or not isinstance(events, list):
        return Failure(session_id, "recording response has no batches or events array")
    if not batches or not events:
        return Failure(session_id, "recording-only session returned no recording payload")
    for index, batch in enumerate(batches):
        if not isinstance(batch, dict) or not isinstance(batch.get("events"), list):
            return Failure(session_id, f"recording batch {index} has no events array")
    return None


def validate_detail(
    base_url: str, token: str, session_id: str, require_event: bool = False
) -> tuple[Failure | None, str]:
    encoded = urllib.parse.quote(session_id, safe="")
    try:
        detail = request_json(f"{base_url}/v1/llm/sessions/{encoded}", token)
    except RuntimeError as error:
        if str(error) != "HTTP 404":
            return Failure(session_id, str(error)), "detail"
        try:
            recording = request_json(
                f"{base_url}/v1/llm/sessions/{encoded}/recording?limit=1000", token
            )
        except RuntimeError as recording_error:
            return Failure(session_id, f"detail {error}; recording {recording_error}"), "recording"
        return validate_recording_payload(session_id, recording), "recording"
    return validate_detail_payload(session_id, detail, require_event), "detail"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="API origin, e.g. http://127.0.0.1:8080/api/thelake")
    parser.add_argument("--from", dest="start", required=True, help="RFC3339 inclusive lower bound")
    parser.add_argument("--to", dest="end", required=True, help="RFC3339 inclusive upper bound")
    parser.add_argument("--inline-session", required=True, help="Known session containing catalog-inlined rows")
    parser.add_argument("--parquet-session", required=True, help="Known session backed by Parquet")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--token", default=os.environ.get("THELAKE_API_TOKEN"))
    args = parser.parse_args()
    if not args.token:
        parser.error("set THELAKE_API_TOKEN or pass --token")
    if args.workers < 1:
        parser.error("--workers must be positive")

    base_url = args.base_url.rstrip("/")
    try:
        session_ids, pages = fetch_session_ids(base_url, args.token, args.start, args.end)
    except RuntimeError as error:
        print(f"session search failed: {error}", file=sys.stderr)
        return 1

    required_event_sessions = {args.inline_session, args.parquet_session}
    for required in required_event_sessions:
        if required not in session_ids:
            session_ids.append(required)

    failures: list[Failure] = []
    detail_loaded = 0
    recording_loaded = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                validate_detail,
                base_url,
                args.token,
                session_id,
                session_id in required_event_sessions,
            )
            for session_id in session_ids
        ]
        for future in concurrent.futures.as_completed(futures):
            failure, route = future.result()
            if failure is not None:
                failures.append(failure)
            elif route == "recording":
                recording_loaded += 1
            else:
                detail_loaded += 1

    print(
        f"pages={pages} sessions={len(session_ids)} inline_check={args.inline_session} "
        f"parquet_check={args.parquet_session} detail_loaded={detail_loaded} "
        f"recording_loaded={recording_loaded} "
        f"loaded={len(session_ids) - len(failures)} failed={len(failures)}"
    )
    for failure in sorted(failures, key=lambda item: item.session_id):
        print(f"FAILED {failure.session_id}: {failure.status}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
