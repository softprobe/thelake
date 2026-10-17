"""LiteLLM hooks for Lisa calls; no provider state or conversation duplication."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict

import litellm
from litellm.integrations.custom_logger import CustomLogger

from ..llm_recording import CallRecorder, current_scope

logger = logging.getLogger(__name__)


class LisaCallLogger(CustomLogger):
    def __init__(self):
        super().__init__()
        self.pending = OrderedDict()
        self.lock = threading.Lock()

    @staticmethod
    def _key(kwargs):
        return kwargs.get("litellm_call_id") or kwargs.get("litellm_params", {}).get(
            "litellm_call_id"
        )

    def log_pre_api_call(self, model, messages, kwargs):
        try:
            scope = current_scope()
            key = self._key(kwargs)
            if scope is None or not key:
                return
            with self.lock:
                if key in self.pending:
                    return
                recorder = CallRecorder(scope[0])
                if not recorder.enabled:
                    return
                options = kwargs.get("optional_params") or {}
                # Allowlist application request fields. Never dump SDK kwargs,
                # which include API keys, headers, endpoint credentials and callbacks.
                request = {
                    "model": model,
                    "provider": kwargs.get("litellm_params", {}).get("custom_llm_provider"),
                    "messages": messages,
                }
                request.update(
                    {
                        name: options[name]
                        for name in (
                            "tools",
                            "tool_choice",
                            "response_format",
                            "temperature",
                            "max_tokens",
                            "max_completion_tokens",
                            "max_output_tokens",
                            "stream",
                            "input",
                            "instructions",
                            "text",
                            "stop",
                            "top_p",
                            "parallel_tool_calls",
                        )
                        if name in options
                    }
                )
                self.pending[key] = (recorder.start("chat", request, **scope[1]), time.monotonic())
                # Bound memory for abandoned callbacks; their request-only artifacts remain.
                while len(self.pending) > 10_000:
                    self.pending.popitem(last=False)
        except Exception:  # noqa: BLE001 - Observability must fail open.
            logger.warning("Lisa chat call recording failed; continuing")

    def _complete(self, kwargs, response, error=False):
        try:
            with self.lock:
                pending = self.pending.pop(self._key(kwargs), None)
            if pending is None:
                return
            call, _ = pending
            if error:
                call.fail(kwargs.get("exception") or RuntimeError())
            else:
                if hasattr(response, "model_dump"):
                    response = response.model_dump(mode="json")
                elif isinstance(response, str):
                    response = json.loads(response)
                call.finish(response)
        except Exception:  # noqa: BLE001 - Observability must fail open.
            logger.warning("Lisa chat call recording failed; continuing")

    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        self._complete(kwargs, response_obj)

    def log_failure_event(self, kwargs, response_obj, start_time, end_time):
        self._complete(kwargs, response_obj, error=True)

    async def async_log_pre_api_call(self, model, messages, kwargs):
        self.log_pre_api_call(model, messages, kwargs)

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self.log_success_event(kwargs, response_obj, start_time, end_time)

    async def async_log_failure_event(self, kwargs, response_obj, start_time, end_time):
        self.log_failure_event(kwargs, response_obj, start_time, end_time)


_LOGGER = LisaCallLogger()
_INSTALL_LOCK = threading.Lock()


def install_call_logger():
    with _INSTALL_LOCK:
        if _LOGGER not in litellm.callbacks:
            litellm.callbacks.append(_LOGGER)
