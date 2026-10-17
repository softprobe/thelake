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
