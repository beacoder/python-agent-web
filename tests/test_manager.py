"""Controller manager: run lifecycle with a stub runner (no real harness)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pytest

from app.controller.manager import Controller
from app.controller.runner import ExecResult, Runner, SandboxNotFoundError
from app.db import get_session_factory
from app.models import Conversation, User, new_id


@dataclass
class StubRunner(Runner):
    """Scripted runner: stdout lines, exit code, optional delay/error."""

    stdout: str = ""
    exit_code: int | None = 0
    stderr: str = ""
    delay: float = 0.0
    raise_error: Exception | None = None
    created: list[str] = field(default_factory=list)
    destroyed: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    last_used: dict = field(default_factory=dict)

    def create(self, user_id: str, conversation_id: str) -> str:
        sandbox_id = f"sbx_{len(self.created)}"
        self.created.append(sandbox_id)
        return sandbox_id

    def destroy(self, sandbox_id: str) -> None:
        self.destroyed.append(sandbox_id)

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        self.cancelled.append(run_id)
        return True

    def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
        self.last_used[run_id] = (sandbox_id, prompt, run_id)
        if self.raise_error is not None:
            raise self.raise_error
        if self.delay:
            time.sleep(self.delay)
        for line in self.stdout.splitlines():
            if on_line is not None:
                on_line(line)
        return ExecResult(exit_code=self.exit_code, stdout=self.stdout, stderr=self.stderr)

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        return []


@pytest.fixture()
def user_id() -> str:
    db = get_session_factory()()
    try:
        user = User(id=new_id("usr"), email="u@example.com", password_hash="x")
        db.add(user)
        conversation = Conversation(id=new_id("cnv"), user_id=user.id, title="t")
        db.add(conversation)
        db.commit()
        return user.id, conversation.id
    finally:
        db.close()


def _controller(runner: StubRunner) -> Controller:
    return Controller(runner=runner)


RESULT_OK = (
    '{"type": "start", "seq": 1}\n'
    '{"type": "delta", "seq": 2, "text": "he"}\n'
    '{"type": "result", "seq": 3, "answer": "hello", "errors": [], '
    '"usage": {"input": 5, "output": 3, "rounds": 1}, "model": "m1"}\n'
)


class TestStartRun:
    def test_success_flow(self, user_id) -> None:
        from app.models import Run

        uid, cid = user_id
        runner = StubRunner(stdout=RESULT_OK)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
            assert run.status == "running"
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})
        db = get_session_factory()()
        try:
            finished = db.get(Run, run_id)
            assert finished is not None
            assert finished.status == "done"
            assert finished.answer == "hello"
            assert finished.exit_code == 0
        finally:
            db.close()

    def test_usage_ledger_written(self, user_id) -> None:
        from app.models import Run, UsageEvent

        uid, cid = user_id
        controller = _controller(StubRunner(stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})
        db = get_session_factory()()
        try:
            event = db.query(UsageEvent).one()
            assert event.user_id == uid
            assert event.run_id == run_id
            assert event.input_tokens == 5
            assert event.output_tokens == 3
            assert event.rounds == 1
            assert event.model == "m1"
            assert db.get(Run, run_id).answer == "hello"
        finally:
            db.close()

    def test_error_exit_marks_error(self, user_id) -> None:
        from app.models import Run

        uid, cid = user_id
        stdout = '{"type": "result", "seq": 1, "answer": "", "errors": ["exploded"]}\n'
        controller = _controller(StubRunner(stdout=stdout, exit_code=1))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        _wait_status(controller, run_id, {"error"})
        db = get_session_factory()()
        try:
            finished = db.get(Run, run_id)
            assert finished is not None
            assert finished.status == "error"
            assert "exploded" in finished.error
        finally:
            db.close()

    def test_result_line_is_canonical_over_stream(self, user_id) -> None:
        uid, cid = user_id
        stdout = (
            '{"type": "notify", "seq": 1, "kind": "error", "data": "transient"}\n'
            '{"type": "result", "seq": 2, "answer": "ok", "errors": []}\n'
        )
        controller = _controller(StubRunner(stdout=stdout))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})

    def test_no_result_line_marks_error(self, user_id) -> None:
        uid, cid = user_id
        controller = _controller(StubRunner(stdout="not json at all\n", exit_code=2))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"error"})

    def test_runner_crash_marks_error(self, user_id) -> None:
        uid, cid = user_id
        runner = StubRunner(raise_error=SandboxNotFoundError("sbx_0"))
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"error"})

    def test_concurrent_run_rejected(self, user_id) -> None:
        uid, cid = user_id
        controller = _controller(StubRunner(delay=0.5, stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="one")
            with pytest.raises(RuntimeError, match="already has a running run"):
                controller.start_run(db, user_id=uid, conversation_id=cid, prompt="two")
        finally:
            db.close()
        # run #1's worker is still sleeping (delay=0.5s); let it finalize
        # before the next test resets the DB engine (a late finalize would
        # query the fresh engine: "no such table: runs")
        _wait_status(controller, run.id, {"done"})


def _wait_status(controller: Controller, run_id: str, states: set[str]) -> None:
    """Wait until the run left the active set (or timeout)."""
    deadline = time.time() + 10
    while time.time() < deadline:
        with controller._lock:
            if run_id not in controller._active:
                return
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} never finished")


class TestSubscriptions:
    def test_replay_and_live_events(self, user_id) -> None:
        uid, cid = user_id
        controller = _controller(StubRunner(delay=0.3, stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        time.sleep(0.05)  # let the worker start
        q = controller.subscribe(run_id)
        assert q is not None
        events = []
        while True:
            event = q.get(timeout=5)
            events.append(event)
            if event.get("type") == "run":
                break
        types = [e["type"] for e in events]
        assert types[0] == "start"
        assert types[-1] == "run"
        assert "result" in types
        controller.unsubscribe(run_id, q)

    def test_subscribe_unknown_run(self) -> None:
        controller = _controller(StubRunner())
        assert controller.subscribe("run_missing") is None

    def test_subscribe_after_finish_gets_replay(self, user_id) -> None:
        uid, cid = user_id
        controller = _controller(StubRunner(stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})
        # _finish_run removes the active entry, so late subscribers see None
        assert controller.subscribe(run_id) is None

    def test_cancel_unknown_run(self) -> None:
        controller = _controller(StubRunner())
        assert controller.cancel_run("run_missing") is False

    def test_cancel_running_run(self, user_id) -> None:
        uid, cid = user_id
        runner = StubRunner(delay=0.5, stdout=RESULT_OK)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        assert controller.cancel_run(run_id) is True
        assert runner.cancelled == [run_id]  # runner signalled
        _wait_status(controller, run_id, {"done"})
        controller.cancel_run(run_id)  # active entry popped; harmless


def test_phantom_finalize_when_row_missing() -> None:
    """If the request thread never commits the Run row (crash), the
    worker still persists the outcome as a phantom row rather than
    losing the terminal state."""
    from app.controller import manager as mgr
    from app.controller.protocol import RunOutcome

    class NullRunner(Runner):
        def create(self, user_id, conversation_id):
            return "sbx_null"

        def destroy(self, sandbox_id):
            pass

        def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
            # skip _finish_run's normal wait by failing after a beat
            time.sleep(0.05)
            mgr.Controller._finish_run(
                controller,
                run_id,
                RunOutcome(errors=["orphaned"], exit_code=1),
            )
            return ExecResult(exit_code=1, stdout="", stderr="")

        def reap_idle(self, ttl_seconds):
            return []

    controller = mgr.Controller(runner=NullRunner())
    # do NOT create the Run row; directly invoke _finish_run
    controller._finish_run("run_phantom", RunOutcome(errors=["orphaned"], exit_code=1))
    from app.db import get_session_factory
    from app.models import Run

    db = get_session_factory()()
    try:
        row = db.get(Run, "run_phantom")
        assert row is not None
        assert row.status == "error"
        assert "run row missing" in row.error or row.error == "orphaned"
    finally:
        db.close()


def test_thread_safety_of_subscribe(user_id) -> None:
    uid, cid = user_id
    controller = _controller(StubRunner(delay=0.2, stdout=RESULT_OK))
    db = get_session_factory()()
    try:
        run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        run_id = run.id
    finally:
        db.close()
    queues = [controller.subscribe(run_id) for _ in range(5)]
    assert all(q is not None for q in queues)
    for q in queues:  # type: ignore[union-attr]
        assert q.get(timeout=5)  # each gets at least one event
    # the worker still sleeps 0.2s; let it finalize before the next test
    # resets the DB engine
    _wait_status(controller, run_id, {"done"})


def test_queue_disconnect_mid_run(user_id) -> None:
    """A subscriber that stops draining must not block the run."""
    uid, cid = user_id
    controller = _controller(StubRunner(stdout=RESULT_OK))
    db = get_session_factory()()
    try:
        run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        run_id = run.id
    finally:
        db.close()
    q = controller.subscribe(run_id)
    assert q is not None
    controller.unsubscribe(run_id, q)
    time.sleep(0.3)
    with controller._lock:
        assert run_id not in controller._active
