"""Read the behavior check authored through Lisa for the quickstart."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


EVALUATOR_ID = "quickstart-refund-eligibility"
EVALUATOR_NAME = "Refund eligibility before refund"
AGENT_NAME = "quickstart-refund-agent"


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


def find_active_evaluator(api_url: str, name: str) -> dict | None:
    definitions = request_json(f"{api_url}/v1/evaluators")
    return next(
        (
            item for item in definitions
            if item.get("name") == name
            and item.get("target_agent_name") == AGENT_NAME
            and item.get("active") is True
        ),
        None,
    )
