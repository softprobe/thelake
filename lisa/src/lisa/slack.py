"""Shared Slack message transport for conversation replies and QA alerts."""

import json
import urllib.request

from .security import redact_text


class SlackClient:
    def __init__(self, token: str):
        self.token = token

    def post_message(
        self, channel: str, text: str, thread_ts: str | None = None, *, timeout: int = 10
    ) -> dict:
        if not self.token:
            raise RuntimeError("Lisa Slack bot token is not configured")
        payload = {"channel": channel, "text": text}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        request = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            reason = redact_text(str(result.get("error", "unknown error")))
            raise RuntimeError(f"Slack rejected Lisa's message: {reason}")
        return result
