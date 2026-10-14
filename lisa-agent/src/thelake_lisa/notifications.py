from __future__ import annotations

import json
import logging
from hashlib import sha256
from pathlib import Path

from .context import WorkspaceContext
from .security import redact_text
from .slack import SlackClient
from .storage import atomic_write_text, file_lock

logger = logging.getLogger(__name__)


class SlackNotifier:
    def __init__(self, context: WorkspaceContext, client: SlackClient | None = None):
        self.context = context
        self.client = client or SlackClient(context.slack_token)

    def _notification_receipt_path(
        self, evaluator: dict, trace_id: str, channel: str, thread_ts: str | None
    ) -> Path:
        identity = json.dumps(
            [
                evaluator.get("policy_hash") or evaluator.get("evaluator_id"),
                evaluator.get("version"),
                trace_id,
                channel,
                thread_ts,
            ],
            separators=(",", ":"),
        )
        digest = sha256(identity.encode("utf-8")).hexdigest()
        home = self.context.home()
        directory = home / "notifications"
        if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
            raise ValueError(
                "Notification receipt path escapes Lisa's workspace home or uses a symlink"
            )
        return directory / f"{digest}.json"

    def notify(self, evaluator: dict, result: dict, session_id: str, trace_id: str) -> dict:
        evaluator_channel = str(evaluator.get("slack_channel_id") or "").strip()
        # New snapshots retain even an absent destination; later turns must not
        # redirect a historical finding. Legacy records retain their old fallback.
        channel = (
            evaluator_channel
            if evaluator.get("policy_hash")
            else evaluator_channel or self.context.slack_channel
        )
        if not channel or result.get("status") != "fail":
            return {"status": "not_configured"}
        token = self.context.slack_token
        if not token:
            return {"status": "failed", "reason": "Slack bot token is not configured"}
        text = (
            f":warning: *Lisa found an agent issue:* {evaluator['name']}\n"
            f"Agent: `{evaluator['target_agent_name']}` · session: `{session_id}`\n"
            f"{redact_text(str(result.get('rationale', 'Evaluation failed.')))}"
        )
        # A thread timestamp is meaningful only with the channel it came from.
        thread_ts = evaluator.get("slack_thread_ts") if evaluator_channel else None
        receipt_path = self._notification_receipt_path(evaluator, trace_id, channel, thread_ts)
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = receipt_path.with_suffix(".lock")
        try:
            with file_lock(lock_path):
                if receipt_path.exists():
                    return {"status": "already_sent", "session_id": session_id}
                try:
                    body = self.client.post_message(channel, text, thread_ts)
                except Exception as error:
                    logger.exception("Lisa Slack notification request failed")
                    return {"status": "failed", "reason": redact_text(str(error))}
                atomic_write_text(
                    receipt_path,
                    json.dumps(
                        {
                            "session_id": session_id,
                            "message_ts": body.get("ts"),
                        }
                    ),
                )
                return {"status": "sent", "channel_id": channel, "message_ts": body.get("ts")}
        except Exception as error:
            logger.exception("Lisa could not persist Slack notification receipt")
            return {"status": "failed", "reason": redact_text(str(error))}
