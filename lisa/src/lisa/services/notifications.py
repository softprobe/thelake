from __future__ import annotations

import logging

from ..context import WorkspaceContext
from ..repositories.notifications import NotificationReceiptRepository
from ..security import redact_text
from ..slack import SlackClient

logger = logging.getLogger(__name__)


class SlackNotifier:
    def __init__(
        self,
        context: WorkspaceContext,
        client: SlackClient,
        repository: NotificationReceiptRepository,
    ):
        self.context = context
        self.client = client
        self.repository = repository

    def notify(self, evaluator: dict, result: dict, session_id: str, trace_id: str) -> dict:
        evaluator_channel = str(evaluator.get("slack_channel_id") or "").strip()
        # Findings with a recorded policy keep that destination, including an
        # absent one. Other findings may use the workspace default channel.
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
        try:
            with self.repository.locked_receipt(evaluator, trace_id, channel, thread_ts) as (
                already_sent,
                save_receipt,
            ):
                if already_sent:
                    return {"status": "already_sent", "session_id": session_id}
                try:
                    body = self.client.post_message(channel, text, thread_ts)
                except Exception as error:
                    logger.exception("Lisa Slack notification request failed")
                    return {"status": "failed", "reason": redact_text(str(error))}
                save_receipt(session_id, body.get("ts"))
                return {"status": "sent", "channel_id": channel, "message_ts": body.get("ts")}
        except Exception as error:
            logger.exception("Lisa could not persist Slack notification receipt")
            return {"status": "failed", "reason": redact_text(str(error))}
