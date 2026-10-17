from __future__ import annotations

import hmac
import json
import urllib.error
import urllib.request
from hashlib import sha256
from typing import Literal, overload

from .context import WorkspaceContext
from .security import redact_text


class TheLakeClient:
    def __init__(self, context: WorkspaceContext):
        self.context = context

    @overload
    def post(self, path: str, body: dict, *, include_status: Literal[False] = False) -> dict: ...

    @overload
    def post(self, path: str, body: dict, *, include_status: Literal[True]) -> tuple[dict, int]: ...

    def post(
        self, path: str, body: dict, *, include_status: bool = False
    ) -> dict | tuple[dict, int]:
        return self._request("POST", path, body, include_status=include_status)

    def get(self, path: str) -> dict:
        return self._request("GET", path)

    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        include_status: bool = False,
    ) -> dict | tuple[dict, int]:
        base_url = self.context.api_url
        token = self.context.api_token
        workspace_id = self.context.workspace_id()
        if not base_url or not token:
            raise RuntimeError("TheLake API is not configured")
        # This versioned domain separator is shared with TheLake's verifier;
        # it remains stable across Python package and service renames.
        signature = hmac.new(
            token.encode(),
            f"thelake-lisa-workspace-v1:{workspace_id}".encode(),
            sha256,
        ).hexdigest()
        request = urllib.request.Request(
            f"{base_url}{path}",
            data=json.dumps(body).encode("utf-8") if body is not None else None,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "X-TheLake-Workspace-ID": workspace_id,
                "X-TheLake-Workspace-Signature": signature,
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                result = json.loads(response.read().decode("utf-8"))
                return (result, response.status) if include_status else result
        except urllib.error.HTTPError as error:
            try:
                response_body = json.loads(error.read().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                response_body = {}
            detail = response_body.get("error") or response_body.get("message")
            if not isinstance(detail, str) or not detail.strip():
                detail = error.reason
            detail = redact_text(detail)[:500]
            raise RuntimeError(f"TheLake API request failed ({error.code}): {detail}") from None
