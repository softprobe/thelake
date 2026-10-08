from __future__ import annotations

import hmac
import os

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.concurrency import run_in_threadpool

from .contracts import EvaluationRequest, EvaluationResponse
from .evaluator import evaluate

app = FastAPI(title="thelake Evaluation Runner", version="0.1.0")


@app.middleware("http")
async def limit_declared_request_size(request: Request, call_next):
    declared_size = request.headers.get("content-length")
    if declared_size is not None:
        try:
            if int(declared_size) > 1_100_000:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "evaluation request exceeds the 1.1 MB limit"},
                )
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "invalid content length"})
    return await call_next(request)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/evaluate", response_model=EvaluationResponse)
async def evaluate_trace(
    request: EvaluationRequest,
    authorization: str | None = Header(default=None),
) -> EvaluationResponse:
    expected = os.environ.get("EVALUATION_RUNNER_TOKEN", "")
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not expected or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="evaluation runner authentication required")
    if not (os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")):
        raise HTTPException(status_code=503, detail="Gemini judge credentials are not configured")
    os.environ.setdefault("USE_GEMINI_MODEL", "1")
    os.environ.setdefault("GEMINI_MODEL_NAME", "gemini-2.5-flash")
    try:
        return await run_in_threadpool(evaluate, request)
    except Exception as error:
        # Do not return provider exception text; it may contain request or secret data.
        raise HTTPException(status_code=502, detail="evaluation provider failed") from error
