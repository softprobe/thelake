"""Build Lisa's FastAPI application and configure process-level behavior."""

from __future__ import annotations

import asyncio
from html import escape
import mimetypes
import os
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, Response

from .bootstrap import build_services
from .context import WorkspaceContext
from .controllers.http import (
    router as http_router,
)
from .llm_recording import recording_maintenance
from .runtime import initialize_backend


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialize_backend()
    workspace_id = os.getenv("LISA_WORKSPACE_ID", "").strip()
    services = build_services(WorkspaceContext.from_environment())
    _app.state.workspace_services = services
    slack_worker_task = None
    evaluation_worker_task = None
    if workspace_id:
        if services.session_evaluation_worker is not None:
            evaluation_worker_task = asyncio.create_task(
                services.session_evaluation_worker.run_forever()
            )
        slack_worker_task = asyncio.create_task(
            services.slack_worker.run_forever(workspace_id)
        )
    async with recording_maintenance(services.context):
        yield
    if slack_worker_task is not None:
        slack_worker_task.cancel()
        try:
            await slack_worker_task
        except asyncio.CancelledError:
            pass
    if evaluation_worker_task is not None:
        evaluation_worker_task.cancel()
        try:
            await evaluation_worker_task
        except asyncio.CancelledError:
            pass
    await services.slack_worker.shutdown()


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


def _static_root() -> Path:
    return Path(os.getenv("LISA_STATIC_DIR", "/app/static")).resolve()


@app.get("/lisa", include_in_schema=False)
async def lisa_frontend_root() -> RedirectResponse:
    return RedirectResponse("/lisa/", status_code=307)


@app.get("/lisa/", include_in_schema=False)
async def lisa_frontend_index() -> Response:
    return _lisa_frontend_file("index.html", spa_fallback=False)


@app.get("/lisa/{asset_path:path}", include_in_schema=False)
async def lisa_frontend_asset(asset_path: str) -> Response:
    pure_path = PurePosixPath(asset_path)
    if pure_path.is_absolute() or any(part in {"", ".", ".."} for part in pure_path.parts):
        raise HTTPException(status_code=404)
    return _lisa_frontend_file(asset_path, spa_fallback=True)


def _lisa_frontend_file(path: str, *, spa_fallback: bool) -> Response:
    root = _static_root()
    candidate = (root / path).resolve()
    if not candidate.is_relative_to(root):
        raise HTTPException(status_code=404)
    if not candidate.is_file():
        if not spa_fallback or Path(path).suffix:
            raise HTTPException(status_code=404)
        candidate = root / "index.html"
    if candidate.name == "index.html":
        html = candidate.read_text(encoding="utf-8").replace(
            '"__LISA_API_BASE_PATH__"', '"/v1"'
        )
        html = html.replace(
            "__LISA_EXPLORER_URL__",
            escape(os.getenv("LISA_EXPLORER_URL", "").strip(), quote=True),
        )
        return Response(html, media_type="text/html", headers={"Cache-Control": "no-cache"})
    return FileResponse(
        candidate,
        media_type=mimetypes.guess_type(candidate.name)[0],
        headers={"Cache-Control": "public, max-age=3600"},
    )
