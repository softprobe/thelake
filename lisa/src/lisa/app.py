"""Build Lisa's FastAPI application and configure process-level behavior."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .bootstrap import WorkspaceServices, build_services
from .context import WorkspaceContext
from .controllers.http import (
    router as http_router,
)
from .llm_recording import recording_maintenance
from .runtime import confirmed_policy_title, initialize_backend, run_turn

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialize_backend()
    workspace_id = os.getenv("LISA_WORKSPACE_ID", "").strip()
    services = build_services(WorkspaceContext.from_environment())
    _app.state.workspace_services = services
    qa_scheduler = None
    slack_worker_task = None
    if workspace_id:
        cron_schedule = os.getenv("LISA_QA_CRON", "0 * * * *").strip()
        try:
            trigger = CronTrigger.from_crontab(cron_schedule, timezone="UTC")
        except ValueError as error:
            raise RuntimeError(
                "LISA_QA_CRON must be a valid five-field UTC cron expression"
            ) from error
        qa_scheduler = AsyncIOScheduler(timezone="UTC")
        job = qa_scheduler.add_job(
            _run_scheduled_qa,
            trigger=trigger,
            args=[services, workspace_id],
            id="lisa-scheduled-qa",
            name="Lisa scheduled QA",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=900,
            replace_existing=True,
        )
        if services.activity_store is not None:
            qa_scheduler.add_job(
                _run_activity_followups,
                trigger="interval",
                seconds=15,
                args=[services, workspace_id],
                id="lisa-activity-followups",
                name="Lisa approved activity follow-ups",
                coalesce=True,
                max_instances=1,
                misfire_grace_time=900,
                replace_existing=True,
            )
        qa_scheduler.start()
        logger.info(
            "Registered cron job '%s' with schedule '%s' UTC; next run at %s",
            job.name,
            cron_schedule,
            job.next_run_time,
        )
        slack_worker_task = asyncio.create_task(
            services.slack_worker.run_forever(workspace_id)
        )
    async with recording_maintenance(services.context):
        yield
    if qa_scheduler is not None:
        qa_scheduler.shutdown(wait=False)
    if slack_worker_task is not None:
        slack_worker_task.cancel()
        try:
            await slack_worker_task
        except asyncio.CancelledError:
            pass
    await services.slack_worker.shutdown()


async def _run_scheduled_qa(services: WorkspaceServices, workspace_id: str) -> None:
    try:
        result = await services.scheduled_qa.run_cycle(
            str(workspace_id),
            run_turn=run_turn,
            confirm_policy_title=confirmed_policy_title,
        )
        logger.info(
            "Cron job 'Lisa scheduled QA' completed for conversation %s",
            result["conversation_id"],
        )
    except Exception:
        logger.exception("Cron job 'Lisa scheduled QA' failed")


async def _run_activity_followups(services: WorkspaceServices, workspace_id: str) -> None:
    store = services.activity_store
    if store is None:
        return
    for followup in store.claim_due_activity_followups():
        followup_id = str(followup["id"])
        try:
            activity_id = str(followup["activity_id"])
            activity = store.get_activity_thread(activity_id)
            if activity is None:
                store.finish_activity_followup(followup_id)
                continue
            main = await services.conversations.main_conversation(workspace_id)
            prompt = (
                f"Resume Lisa activity {activity_id}. An owner approved this follow-up. "
                f"Follow-up request: {followup['prompt']}\n"
                "Treat this request as the next step on the linked activity, inspect its "
                "timeline first, and update the activity with your progress and outcome."
            )
            turn = await services.conversations.run_user_turn(
                main["conversation_id"], workspace_id, prompt,
                run_turn=run_turn, confirmed_policy_title=None, turn_source="scheduled",
            )
            summary = next(
                (message.get("text", "") for message in reversed(turn["messages"])
                 if message.get("role") == "assistant"),
                "Lisa completed the scheduled follow-up.",
            )
            delivery = services.notifier.notify_activity_update(
                followup_id, activity["activity"], summary,
            )
            store.record_activity_event(
                activity_id, "followup_completed", title="Scheduled follow-up completed",
                summary=summary[:4000],
            )
            if delivery.get("status") == "failed":
                store.record_activity_event(
                    activity_id, "notification_failed", title="Slack update failed",
                    summary=str(delivery.get("reason", "Slack delivery failed"))[:1000],
                )
            store.finish_activity_followup(followup_id)
            store.complete_agent_activity(activity_id, summary, status="succeeded")
        except Exception:
            logger.exception("Lisa approved activity follow-up failed")
            store.release_activity_followup(followup_id)


app = FastAPI(title="Lisa Agent Runtime", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip() for origin in os.getenv("LISA_CORS_ORIGINS", "").split(",") if origin.strip()
    ],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Softprobe-Assertion"],
)
app.include_router(http_router)
