"""Shared API helpers for the dogfood policy QA walkthrough."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


EVALUATOR_NAME = "Refund eligibility before refund"
AGENT_NAME = "dogfood-refund-agent"


class TheLakeApiError(RuntimeError):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        super().__init__(f"TheLake API returned HTTP {status_code}: {detail}")


def request_json(url: str, method: str = "GET", payload: dict | None = None) -> dict | list:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read())
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise TheLakeApiError(error.code, detail) from error
    except URLError as error:
        raise RuntimeError(f"Could not reach TheLake API at {url}: {error.reason}") from error
