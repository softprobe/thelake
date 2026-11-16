import argparse
import json
import secrets
import time
from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


STAGING_URL = "https://thelake-staging.softprobe.ai"
STAGING_TOKEN = "staging-thelake-smoke-token"
POLL_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 10


def trace_payload(trace_id: str, span_id: str, start_time_ns: int) -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "thelake-staging-e2e"},
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": span_id,
                                "name": "GET /staging-smoke",
                                "kind": 2,
                                "startTimeUnixNano": str(start_time_ns),
                                "endTimeUnixNano": str(start_time_ns + 5_000_000),
                                "attributes": [],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def readback_matches(
    response: dict, trace_id: str, span_id: str, session_id: str
) -> bool:
    trace = response.get("trace")
    if not isinstance(trace, dict):
        return False
    if trace.get("trace_id") != trace_id or trace.get("session_id") != session_id:
        return False

    spans = response.get("spans")
    if not isinstance(spans, list):
        return False
    return any(
        isinstance(span, dict)
        and span.get("trace_id") == trace_id
        and span.get("span_id") == span_id
        and span.get("session_id") == session_id
        for span in spans
    )


def request_json(
    url: str, method: str, body: dict | None = None
) -> tuple[int | None, dict | None]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Authorization": f"Bearer {STAGING_TOKEN}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        if method == "GET" and (error.code == 404 or 500 <= error.code < 600):
            return error.code, None
        raise RuntimeError(f"{method} request returned HTTP {error.code}") from error
    except (TimeoutError, URLError, IncompleteRead, json.JSONDecodeError):
        if method == "GET":
            return None, None
        raise


def rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def run_staging_e2e(base_url: str, timeout_seconds: int) -> None:
    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    session_id = f"ses_staging_e2e_{span_id}"
    start_time_ns = time.time_ns()
    payload = trace_payload(trace_id, span_id, start_time_ns)

    ingest_request = Request(
        f"{base_url.rstrip('/')}/v1/traces",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {STAGING_TOKEN}",
            "Baggage": f"sp.session.id={session_id}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(ingest_request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise RuntimeError(f"trace ingest returned HTTP {response.status}")
    except HTTPError as error:
        raise RuntimeError(f"trace ingest returned HTTP {error.code}") from error

    now = datetime.now(timezone.utc)
    query = urlencode(
        {
            "from": rfc3339(now - timedelta(minutes=1)),
            "to": rfc3339(now + timedelta(seconds=timeout_seconds + 60)),
        }
    )
    trace_url = f"{base_url.rstrip('/')}/v1/traces/{trace_id}?{query}"
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while time.monotonic() < deadline:
        status, result = request_json(trace_url, "GET")
        last_status = f"HTTP {status}" if status is not None else "transient network error"
        if status == 200 and result is not None:
            if readback_matches(result, trace_id, span_id, session_id):
                print(f"persisted trace {trace_id} with session {session_id}")
                return
            last_status = "HTTP 200 (waiting for the expected span and session)"
        time.sleep(min(POLL_INTERVAL_SECONDS, max(0, deadline - time.monotonic())))

    raise RuntimeError(
        f"trace {trace_id} was not persisted before timeout; last response: {last_status}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send one synthetic span to staging and verify persisted readback."
    )
    parser.add_argument("--url", default=STAGING_URL)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")

    run_staging_e2e(args.url, args.timeout_seconds)


if __name__ == "__main__":
    main()
