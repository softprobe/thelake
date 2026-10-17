import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from lisa.context import WorkspaceContext
from lisa.llm_recording import CallRecorder, recording_scope


def context(tmp_path, workspace="11111111-1111-4111-8111-111111111111"):
    return WorkspaceContext(workspace=workspace, data_dir=tmp_path)


def records(ctx):
    return list((ctx.home() / "llm-calls").glob("*.jsonl"))


def test_opt_in_and_request_only_then_terminal(tmp_path, monkeypatch):
    ctx = context(tmp_path)
    CallRecorder(ctx).start("judge", {"input": "hello"})
    assert not records(ctx)
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    recorder = CallRecorder(ctx)
    call = recorder.start("judge", {"input": "password=private"}, run_id="r1")
    (path,) = records(ctx)
    assert len(path.read_text().splitlines()) == 1
    call.finish({"text": "ok", "reasoning_content": "hidden", "api_key": "private"})
    call.finish({"text": "duplicate"})
    data = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["type"] for row in data] == ["request", "response"]
    assert data[0]["correlation"]["run_id"] == "r1"
    assert "private" not in path.read_text()
    assert "hidden" not in path.read_text()
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_failure_and_disk_errors_fail_open(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "true")
    ctx = context(tmp_path)
    recorder = CallRecorder(ctx)
    recorder.start("judge", {}).fail(ValueError("private secret"))
    assert "private secret" not in records(ctx)[0].read_text()
    root = ctx.home() / "llm-calls"
    root.rename(root.with_name("saved"))
    root.symlink_to(root.with_name("saved"), target_is_directory=True)
    recorder.start("judge", {"input": "private"}).finish({})
    assert "private" not in caplog.text
    assert "record" in caplog.text.lower()


def test_retention_only_owns_call_files(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    CallRecorder(ctx).start("judge", {})
    (path,) = records(ctx)
    unrelated = path.parent / "notes.txt"
    unrelated.write_text("keep")
    old = time.time() - 31 * 86400
    os.utime(path, (old, old))
    os.utime(unrelated, (old, old))
    CallRecorder(ctx).cleanup()
    assert not path.exists()
    assert unrelated.exists()


def test_chat_hooks_are_isolated_deduplicated_and_include_schema(tmp_path, monkeypatch):
    from lisa.backends.llm_recording import LisaCallLogger

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    logger = LisaCallLogger()

    def run(workspace):
        ctx = context(tmp_path, workspace)
        kwargs = {"litellm_call_id": workspace, "optional_params": {"tools": [{"name": "lookup"}]}}
        with recording_scope(ctx, conversation_id=workspace):
            logger.log_pre_api_call("model", [{"role": "user", "content": "hello"}], kwargs)
            logger.log_pre_api_call("model", [], kwargs)
        # terminal callbacks need not run in the request thread/context
        logger.log_success_event(kwargs, {"choices": [{"message": {"content": "ok"}}]}, None, None)
        logger.log_success_event(kwargs, {"choices": []}, None, None)
        return ctx

    with ThreadPoolExecutor() as pool:
        contexts = list(
            pool.map(
                run,
                ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"],
            )
        )
    for ctx in contexts:
        (path,) = records(ctx)
        data = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(data) == 2
        assert data[0]["correlation"]["conversation_id"] == ctx.workspace
        assert data[0]["request"]["tools"] == [{"name": "lookup"}]
    logger.log_pre_api_call("model", [], {"litellm_call_id": "unrelated"})
    assert sum(len(records(ctx)) for ctx in contexts) == 2


def test_redaction_is_idempotent_for_pre_redacted_provider_prompts():
    from lisa.security import redact_evidence

    value = 'password=private api_key="private" Authorization: Bearer private'
    once = redact_evidence(value)
    assert redact_evidence(once) == once


def test_request_scope_inherited_by_async_callbacks(tmp_path, monkeypatch):
    import asyncio

    from lisa.backends.llm_recording import LisaCallLogger

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    logger = LisaCallLogger()
    kwargs = {"litellm_call_id": "async", "exception": ValueError("private")}

    async def run():
        with recording_scope(ctx, conversation_id="c1"):
            await logger.async_log_pre_api_call("model", [], kwargs)
            logger.log_pre_api_call("model", [], kwargs)
        await logger.async_log_failure_event(kwargs, None, None, None)
        logger.log_failure_event(kwargs, None, None, None)

    asyncio.run(run())
    (path,) = records(ctx)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 2
    assert rows[1]["error"] == {"error_type": "ValueError"}
    assert "private" not in path.read_text()


def test_rejects_parent_path_replacement_before_terminal(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    call = CallRecorder(ctx).start("judge", {})
    home = ctx.home()
    original = home.with_name("original")
    home.rename(original)
    outside = tmp_path / "outside"
    (outside / "llm-calls").mkdir(parents=True)
    home.symlink_to(outside, target_is_directory=True)
    call.finish({"text": "should not escape"})
    assert not list((outside / "llm-calls").iterdir())


def test_cleanup_is_throttled_across_recorder_instances(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    calls = []
    original = CallRecorder.cleanup

    def cleanup(self):
        calls.append(True)
        return original(self)

    monkeypatch.setattr(CallRecorder, "cleanup", cleanup)
    CallRecorder(ctx).start("judge", {})
    CallRecorder(ctx).start("selector", {})
    assert calls == [True]


def test_lifecycle_cleans_at_startup_and_while_idle(tmp_path, monkeypatch):
    import asyncio

    from lisa import llm_recording

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    monkeypatch.setattr(llm_recording, "CLEANUP_INTERVAL_SECONDS", 0.01)
    ctx = context(tmp_path)
    calls = []

    async def run():
        ready = asyncio.Event()

        def safe_cleanup(self):
            calls.append(self.context.workspace)
            if len(calls) >= 2:
                loop.call_soon_threadsafe(ready.set)

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(CallRecorder, "cleanup", safe_cleanup)
        async with llm_recording.recording_maintenance(ctx):
            assert len(calls) == 1
            await asyncio.wait_for(ready.wait(), 1)
        count = len(calls)
        await asyncio.sleep(0.03)
        assert len(calls) == count

    asyncio.run(run())


def test_nested_workspace_scope_does_not_inherit_foreign_links(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    first = context(tmp_path)
    second = context(tmp_path, "22222222-2222-4222-8222-222222222222")
    with recording_scope(first, conversation_id="foreign"):
        with recording_scope(second, session_id="own"):
            CallRecorder.current().start("selector", {})
        CallRecorder(second).start("judge", {})
    for path in records(second):
        assert "foreign" not in path.read_text()


def test_rejects_workspace_directory_symlink_even_inside_data_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    other = tmp_path / "other-workspace"
    other.mkdir()
    ctx.home().parent.symlink_to(other, target_is_directory=True)
    CallRecorder(ctx).start("judge", {"text": "private"})
    assert not list(other.iterdir())
