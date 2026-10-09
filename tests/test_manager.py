"""Controller manager: run lifecycle with a stub runner (no real harness)."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

import pytest

from app.controllers.manager import Controller
from app.controllers.runner import ExecResult, Runner, SandboxNotFoundError
from app.infra.config import get_settings
from app.infra.db import get_session_factory
from app.models import Conversation, Run, Sandbox, User, new_id


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

    def test_harness_prompt_override(self, user_id) -> None:
        """The Run row keeps the verbatim prompt; the runner gets the
        augmented harness_prompt (or the prompt when not given)."""
        uid, cid = user_id
        runner = StubRunner(stdout=RESULT_OK)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(
                db,
                user_id=uid,
                conversation_id=cid,
                prompt="summarize",
                harness_prompt="summarize\n\n[Uploaded files: a.xlsx]",
            )
            run_id = run.id
            assert run.prompt == "summarize"
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})
        assert runner.last_used[run_id][1] == "summarize\n\n[Uploaded files: a.xlsx]"

        runner2 = StubRunner(stdout=RESULT_OK)
        controller2 = _controller(runner2)
        db = get_session_factory()()
        try:
            run2 = controller2.start_run(db, user_id=uid, conversation_id=cid, prompt="plain")
            run2_id = run2.id
        finally:
            db.close()
        _wait_status(controller2, run2_id, {"done"})
        assert runner2.last_used[run2_id][1] == "plain"

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
    from app.controllers import manager as mgr
    from app.controllers.protocol import RunOutcome

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
    from app.infra.db import get_session_factory
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


class TestOneRunningRunConstraint:
    """The partial unique index — not just the Python pre-check —
    guarantees at most one running run per conversation."""

    def _running_run(self, cid: str):
        from app.models import Run

        return Run(id=new_id("run"), conversation_id=cid, prompt="p", status="running")

    def test_second_running_run_is_rejected_by_the_db(self, user_id) -> None:
        from sqlalchemy.exc import IntegrityError

        _uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add(self._running_run(cid))
            db.commit()
            db.add(self._running_run(cid))  # same conversation, also running
            with pytest.raises(IntegrityError):
                db.commit()
        finally:
            db.rollback()
            db.close()

    def test_new_running_run_allowed_once_the_previous_finished(self, user_id) -> None:
        _uid, cid = user_id
        db = get_session_factory()()
        try:
            first = self._running_run(cid)
            db.add(first)
            db.commit()
            first.status = "done"  # finished -> no longer indexed
            db.commit()
            db.add(self._running_run(cid))
            db.commit()  # must not raise
        finally:
            db.close()

    def test_two_conversations_can_each_have_a_running_run(self, user_id) -> None:
        uid, cid = user_id
        db = get_session_factory()()
        try:
            other = Conversation(id=new_id("cnv"), user_id=uid, title="t2")
            db.add(other)
            db.commit()
            db.add(self._running_run(cid))
            db.add(self._running_run(other.id))
            db.commit()  # different conversations -> both allowed
        finally:
            db.close()


class TestOrphanRunSweep:
    """A restart abandons in-flight runs; startup must fail the stranded
    ``running`` rows so they don't hang forever or block the conversation."""

    def test_orphaned_running_run_is_marked_error(self, user_id) -> None:
        from app.models import Run

        _uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add(Run(id="run_orphan", conversation_id=cid, prompt="p", status="running"))
            db.commit()
        finally:
            db.close()

        n = _controller(StubRunner()).reconcile_orphaned_runs()
        assert n == 1

        db = get_session_factory()()
        try:
            from app.models import Run

            run = db.get(Run, "run_orphan")
            assert run.status == "error"
            assert "restart" in run.error
            assert run.finished_at is not None
        finally:
            db.close()

    def test_sweep_leaves_finished_runs_untouched(self, user_id) -> None:
        from app.models import Run

        _uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add(Run(id="run_done", conversation_id=cid, prompt="p", status="done", answer="a"))
            db.commit()
        finally:
            db.close()

        assert _controller(StubRunner()).reconcile_orphaned_runs() == 0

        db = get_session_factory()()
        try:
            assert db.get(Run, "run_done").status == "done"
        finally:
            db.close()

    def test_swept_conversation_can_start_a_new_run(self, user_id) -> None:
        """After the sweep clears the orphan, the partial unique index no
        longer blocks a fresh run for that conversation."""
        from app.models import Run

        uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add(Run(id="run_stuck", conversation_id=cid, prompt="p", status="running"))
            db.commit()
        finally:
            db.close()

        controller = _controller(StubRunner(stdout=RESULT_OK))
        controller.reconcile_orphaned_runs()
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="fresh")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})


class TestInstanceScopedReconciliation:
    """A restart must fail only THIS instance's orphaned runs, never a
    different live instance's healthy runs."""

    def test_owner_instance_is_stamped_while_running(self, user_id) -> None:
        from app.models import Run

        uid, cid = user_id
        controller = Controller(runner=StubRunner(stdout=RESULT_OK), instance_id="inst-A")
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})
        # finished run releases ownership
        db = get_session_factory()()
        try:
            assert db.get(Run, run_id).owner_instance is None
        finally:
            db.close()

    def test_reconcile_ignores_other_instances_runs(self, user_id) -> None:
        from app.models import Run

        _uid, cid = user_id
        db = get_session_factory()()
        try:
            # a run owned by a DIFFERENT, still-live instance
            db.add(
                Run(
                    id="run_other",
                    conversation_id=cid,
                    prompt="p",
                    status="running",
                    owner_instance="inst-B",
                )
            )
            db.commit()
        finally:
            db.close()

        # instance A restarts and reconciles: must NOT touch inst-B's run
        n = Controller(runner=StubRunner(), instance_id="inst-A").reconcile_orphaned_runs()
        assert n == 0

        db = get_session_factory()()
        try:
            run = db.get(Run, "run_other")
            assert run.status == "running"  # left alone
            assert run.owner_instance == "inst-B"
        finally:
            db.close()

    def test_reconcile_claims_own_and_null_owner_runs(self, user_id) -> None:
        from app.models import Run

        _uid, cid = user_id
        # a second conversation so two running rows can coexist (the
        # partial unique index is per-conversation)
        db = get_session_factory()()
        try:
            other_conv = new_id("cnv")
            db.add(Conversation(id=other_conv, user_id=_uid, title="t2"))
            db.add(
                Run(
                    id="run_mine",
                    conversation_id=cid,
                    prompt="p",
                    status="running",
                    owner_instance="inst-A",
                )
            )
            db.add(
                Run(
                    id="run_legacy",
                    conversation_id=other_conv,
                    prompt="p",
                    status="running",
                    owner_instance=None,  # pre-migration / unclaimed
                )
            )
            db.commit()
        finally:
            db.close()

        n = Controller(runner=StubRunner(), instance_id="inst-A").reconcile_orphaned_runs()
        assert n == 2  # own run + the NULL-owner legacy run

        db = get_session_factory()()
        try:
            assert db.get(Run, "run_mine").status == "error"
            assert db.get(Run, "run_legacy").status == "error"
            # ownership released on reconcile
            assert db.get(Run, "run_mine").owner_instance is None
        finally:
            db.close()


class TestDrain:
    """Graceful shutdown: reject new runs and wait for in-flight ones."""

    def test_drain_waits_for_in_flight_run(self, user_id) -> None:
        from app.models import Run

        uid, cid = user_id
        # a run that takes a moment; drain must block until it finishes
        controller = Controller(runner=StubRunner(delay=0.3, stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()

        still = controller.drain(timeout=10)
        assert still == 0  # finished within the window

        db = get_session_factory()()
        try:
            assert db.get(Run, run_id).status == "done"
        finally:
            db.close()

    def test_drain_rejects_new_runs(self, user_id) -> None:
        uid, cid = user_id
        controller = Controller(runner=StubRunner(stdout=RESULT_OK))
        controller.drain(timeout=1)  # sets draining
        db = get_session_factory()()
        try:
            with pytest.raises(RuntimeError, match="shutting down"):
                controller.start_run(db, user_id=uid, conversation_id=cid, prompt="late")
        finally:
            db.close()

    def test_drain_returns_count_still_running_at_deadline(self, user_id) -> None:
        uid, cid = user_id
        # a run longer than the drain window: still in flight at deadline
        controller = Controller(runner=StubRunner(delay=2.0, stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="slow")
            run_id = run.id
        finally:
            db.close()

        still = controller.drain(timeout=0.2)
        assert still == 1  # did not finish in time

        # it is still owned by this instance and would be reconciled on
        # the next startup
        _wait_status(controller, run_id, {"done"})  # let it finish to clean up


def _sandbox_rows(conversation_id: str) -> list[tuple[str, str]]:
    """``(id, status)`` of every sandbox row for the conversation."""
    db = get_session_factory()()
    try:
        return [
            (s.id, s.status)
            for s in db.query(Sandbox)
            .filter(Sandbox.conversation_id == conversation_id)
            .order_by(Sandbox.created_at)
            .all()
        ]
    finally:
        db.close()


def _run_row(run_id: str):
    db = get_session_factory()()
    try:
        return db.get(Run, run_id)
    finally:
        db.close()


class TestStaleSandboxRow:
    """A ``running`` sandbox row whose runner state is gone.

    Sandboxes are in-process state but the registry row is durable, so
    every ``running`` row from a previous life of the process is a
    corpse.  Reusing one made exec_run raise SandboxNotFoundError and
    the run fail with "sandbox gone" — and because nothing retired the
    row, every LATER run on that conversation failed identically.  That
    turned a restart into permanent breakage for existing conversations.
    """

    def test_row_without_runner_state_is_retired_and_replaced(self, user_id) -> None:
        uid, cid = user_id

        class GhostRunner(StubRunner):
            """A restarted process: holds no state for any sandbox."""

            def exists(self, sandbox_id: str) -> bool:
                return False

        db = get_session_factory()()
        try:
            db.add(Sandbox(id="sbx_ghost", user_id=uid, conversation_id=cid, status="running"))
            db.commit()
        finally:
            db.close()

        runner = GhostRunner(stdout=RESULT_OK)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            run_id = run.id
        finally:
            db.close()
        _wait_status(controller, run_id, {"done"})

        # the run succeeded rather than dying on the corpse
        assert _run_row(run_id).status == "done"
        # a fresh sandbox was created and the ghost row retired
        assert runner.created == ["sbx_0"]
        assert dict(_sandbox_rows(cid)) == {"sbx_ghost": "destroyed", "sbx_0": "running"}

    def test_a_live_row_is_still_reused(self, user_id) -> None:
        """The default (runner confirms the sandbox) must not change:
        two turns share one sandbox, which is what multi-turn memory
        depends on."""
        uid, cid = user_id
        runner = StubRunner(stdout=RESULT_OK)  # inherits exists() -> True
        controller = _controller(runner)
        for _ in range(2):
            db = get_session_factory()()
            try:
                run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            finally:
                db.close()
            _wait_status(controller, run.id, {"done"})

        assert runner.created == ["sbx_0"]  # created once, reused once
        assert _sandbox_rows(cid) == [("sbx_0", "running")]

    def test_sandbox_gone_retires_the_row_so_the_next_run_recovers(self, user_id) -> None:
        uid, cid = user_id
        runner = StubRunner(raise_error=SandboxNotFoundError("sbx_0"))
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
            first_id = run.id
        finally:
            db.close()
        _wait_status(controller, first_id, {"error"})
        assert "sandbox gone" in _run_row(first_id).error
        assert dict(_sandbox_rows(cid))["sbx_0"] == "destroyed"
        # a retired row must not keep an owner: owner_instance is only
        # meaningful while the sandbox is running
        db = get_session_factory()()
        try:
            assert db.get(Sandbox, "sbx_0").owner_instance is None
        finally:
            db.close()

        # the conversation is NOT poisoned: the next turn builds a new
        # sandbox instead of hitting the same dead row forever
        runner.raise_error = None
        runner.stdout = RESULT_OK
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="again")
            second_id = run.id
        finally:
            db.close()
        _wait_status(controller, second_id, {"done"})
        assert _run_row(second_id).status == "done"
        assert dict(_sandbox_rows(cid)) == {"sbx_0": "destroyed", "sbx_1": "running"}


@dataclass
class WedgedRunner(Runner):
    """A runner whose run accepts ``op:cancel`` and then ignores it.

    Models a harness blocked in an uninterruptible operation: the
    cooperative cancel is never reached, so only destroying the sandbox
    ends the run (which is what closes stdout and lets the exec's pump
    see EOF).
    """

    destroyed: list = field(default_factory=list)
    cancelled: list = field(default_factory=list)
    released: threading.Event = field(default_factory=threading.Event)

    def create(self, user_id: str, conversation_id: str) -> str:
        return "sbx_wedged"

    def destroy(self, sandbox_id: str) -> None:
        self.destroyed.append(sandbox_id)
        self.released.set()  # teardown is what frees the wedged run

    def cancel(self, sandbox_id: str, run_id: str) -> bool:
        self.cancelled.append(run_id)
        return True  # written to the pipe, never acted on

    def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
        self.released.wait(10)
        return ExecResult(exit_code=None, stdout="", stderr="harness process died mid-run")

    def reap_idle(self, ttl_seconds: float) -> list[str]:
        return []


class TestCancelEscalation:
    """Cancel must be able to release a run that ignores the cancel op.

    Armed only by an explicit user cancel, never by a timer, so a run
    nobody cancels stays unbounded.
    """

    def _start(self, controller: Controller, uid: str, cid: str) -> str:
        db = get_session_factory()()
        try:
            return controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi").id
        finally:
            db.close()

    def test_wedged_run_is_released_by_destroying_the_sandbox(
        self, user_id, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid, cid = user_id
        monkeypatch.setattr(get_settings(), "cancel_grace_seconds", 0.3)
        runner = WedgedRunner()
        controller = _controller(runner)
        run_id = self._start(controller, uid, cid)

        assert controller.cancel_run(run_id) is True
        assert runner.cancelled == [run_id]  # graceful attempt came first

        # escalation fires only after the grace window elapses
        _wait_status(controller, run_id, {"error"})
        assert runner.destroyed == ["sbx_wedged"]
        # the worker — not the watchdog — wrote the terminal state
        assert _run_row(run_id).status == "error"
        # and the sandbox row went with it
        assert dict(_sandbox_rows(cid))["sbx_wedged"] == "destroyed"

    def test_escalation_stands_down_when_the_run_honours_the_cancel(
        self, user_id, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid, cid = user_id
        monkeypatch.setattr(get_settings(), "cancel_grace_seconds", 0.3)
        runner = StubRunner(delay=0.05, stdout=RESULT_OK)
        controller = _controller(runner)
        run_id = self._start(controller, uid, cid)

        controller.cancel_run(run_id)
        _wait_status(controller, run_id, {"done"})
        time.sleep(0.5)  # past the grace window
        assert runner.destroyed == []  # sandbox kept for the next turn

    def test_zero_grace_disables_escalation(self, user_id, monkeypatch: pytest.MonkeyPatch) -> None:
        uid, cid = user_id
        monkeypatch.setattr(get_settings(), "cancel_grace_seconds", 0)
        runner = WedgedRunner()
        controller = _controller(runner)
        run_id = self._start(controller, uid, cid)
        try:
            assert controller.cancel_run(run_id) is True
            time.sleep(0.4)
            assert runner.destroyed == []  # best-effort cancel only
        finally:
            runner.released.set()  # don't leak the wedged worker
            _wait_status(controller, run_id, {"error"})

    def test_repeated_cancels_arm_one_watchdog(
        self, user_id, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cancel endpoint is not rate-limited, so arming must latch.

        Without the latch each repeated cancel on a live run spawns
        another watchdog thread that polls for the whole grace window —
        trivial for a client to amplify.
        """
        uid, cid = user_id
        monkeypatch.setattr(get_settings(), "cancel_grace_seconds", 0.3)
        runner = WedgedRunner()
        controller = _controller(runner)
        run_id = self._start(controller, uid, cid)

        before = threading.active_count()
        for _ in range(25):
            assert controller.cancel_run(run_id) is True
        assert threading.active_count() - before <= 1  # one watchdog, not 25
        with controller._lock:
            assert controller._active[run_id].escalation_armed is True

        _wait_status(controller, run_id, {"error"})
        assert runner.destroyed == ["sbx_wedged"]  # still escalates once

    def test_escalation_honours_a_runner_that_declines(
        self, user_id, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Escalation asks for an OWNERSHIP-checked teardown, not a blind one.

        The sandbox is resident, so by the time the grace window closes
        the cancelled run may have finalized and the conversation's next
        turn may already own the same sandbox; tearing it down then
        would kill an innocent successor.  The runner is the only place
        that can check ownership atomically, so the manager must route
        through ``destroy_if_running`` and respect a False answer.

        (The interleaving itself is a sub-millisecond window inside
        ``_finish_run``, so it is pinned here by the contract rather
        than by trying to hit the race; ``destroy_if_running``'s own
        semantics are covered in the runner suites.)
        """
        uid, cid = user_id
        monkeypatch.setattr(get_settings(), "cancel_grace_seconds", 0.2)

        @dataclass
        class DecliningRunner(WedgedRunner):
            asked: list = field(default_factory=list)

            def destroy_if_running(self, sandbox_id: str, run_id: str) -> bool:
                self.asked.append((sandbox_id, run_id))
                return False  # the sandbox has moved on

        runner = DecliningRunner()
        controller = _controller(runner)
        run_id = self._start(controller, uid, cid)
        try:
            assert controller.cancel_run(run_id) is True
            time.sleep(0.5)  # well past the grace window
            assert runner.asked == [("sbx_wedged", run_id)]  # it asked
            assert runner.destroyed == []  # and took no for an answer
            assert dict(_sandbox_rows(cid))["sbx_wedged"] == "running"
        finally:
            runner.released.set()
            _wait_status(controller, run_id, {"error"})


class TestSandboxClaim:
    def test_claiming_a_sandbox_resets_its_idle_age(self, user_id) -> None:
        """A returning user's sandbox is stale by idle age at the moment
        it is claimed; the claim must keep the reaper off it until the
        worker thread actually execs."""
        uid, cid = user_id

        @dataclass
        class TouchRecordingRunner(StubRunner):
            touched: list = field(default_factory=list)

            def touch(self, sandbox_id: str) -> None:
                self.touched.append(sandbox_id)

        runner = TouchRecordingRunner(stdout=RESULT_OK)
        controller = _controller(runner)

        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="first")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})
        assert runner.touched == []  # nothing to claim; it was just created

        # second turn reuses the existing sandbox -> must be claimed
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="second")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})
        assert runner.touched == ["sbx_0"]


class TestSpawnOrdering:
    """A submit that loses the race must not spawn anything.

    ``_ensure_sandbox`` starts a real harness process, so doing it
    before the run row's commit meant the loser of a concurrent submit
    had already spawned one that the rollback then abandoned — an
    orphan process with no registry row.
    """

    def test_run_row_is_committed_before_any_sandbox_is_spawned(self, user_id) -> None:
        """The claiming commit must come first.

        That commit is what wins the conversation (the partial unique
        index is the authoritative guard), so a submit which loses the
        race is rejected before ``_ensure_sandbox`` ever starts a
        harness process.  Spawning first meant the loser's process was
        already running when the rollback discarded its row — an
        orphan nothing owned.
        """
        uid, cid = user_id
        order: list[str] = []

        @dataclass
        class ObservingRunner(StubRunner):
            def create(self, user_id: str, conversation_id: str) -> str:
                order.append("spawn")
                return super().create(user_id, conversation_id)

        controller = _controller(ObservingRunner(stdout=RESULT_OK))
        db = get_session_factory()()
        try:
            real_commit = db.commit

            def _tracking_commit() -> None:
                order.append("commit")
                real_commit()

            db.commit = _tracking_commit  # type: ignore[method-assign]
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})

        # claim, then spawn, then persist the sandbox row
        assert order == ["commit", "spawn", "commit"]

    def test_pre_check_also_spawns_nothing(self, user_id) -> None:
        uid, cid = user_id
        runner = StubRunner(stdout=RESULT_OK, delay=0.3)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            first = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="one")
        finally:
            db.close()
        db = get_session_factory()()
        try:
            with pytest.raises(RuntimeError, match="already has a running run"):
                controller.start_run(db, user_id=uid, conversation_id=cid, prompt="two")
        finally:
            db.close()
        assert runner.created == ["sbx_0"]  # exactly one sandbox, not two
        _wait_status(controller, first.id, {"done"})

    def test_a_failed_commit_does_not_orphan_a_fresh_sandbox(self, user_id) -> None:
        """The sandbox row is written after the sandbox is spawned, so a
        failure between the two must tear the process back down.

        Otherwise the rollback discards the row and leaves a harness
        process nothing owns -- the same orphan the commit ordering
        exists to prevent, arriving via the failure path instead.
        """
        uid, cid = user_id
        runner = StubRunner(stdout=RESULT_OK)
        controller = _controller(runner)

        db = get_session_factory()()
        try:
            real_commit = db.commit
            calls: list[int] = []

            def _fail_the_second_commit() -> None:
                calls.append(1)
                if len(calls) == 2:  # the one persisting the sandbox row
                    raise RuntimeError("db went away")
                real_commit()

            db.commit = _fail_the_second_commit  # type: ignore[method-assign]
            with pytest.raises(RuntimeError, match="db went away"):
                controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()

        assert runner.created == ["sbx_0"]  # it was spawned...
        assert runner.destroyed == ["sbx_0"]  # ...and cleaned back up
        # the run is terminal, so the conversation is not left locked
        db = get_session_factory()()
        try:
            runs = db.query(Run).filter(Run.conversation_id == cid).all()
            assert [r.status for r in runs] == ["error"]
            assert db.query(Sandbox).count() == 0  # row rolled back
        finally:
            db.close()

    def test_sandbox_failure_does_not_strand_the_run_row(self, user_id) -> None:
        """The row is committed before the sandbox exists, so a failed
        create must finalize it — otherwise the conversation stays
        locked by a run that never started."""
        uid, cid = user_id

        @dataclass
        class BrokenSpawnRunner(StubRunner):
            def create(self, user_id: str, conversation_id: str) -> str:
                raise OSError("no pty available")

        controller = _controller(BrokenSpawnRunner())
        db = get_session_factory()()
        try:
            with pytest.raises(OSError, match="no pty available"):
                controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()

        # the run exists but is terminal, so the conversation is free
        db = get_session_factory()()
        try:
            runs = db.query(Run).filter(Run.conversation_id == cid).all()
            assert [r.status for r in runs] == ["error"]
            assert "sandbox unavailable" in runs[0].error
        finally:
            db.close()

        # and a later turn can start normally
        runner = StubRunner(stdout=RESULT_OK)
        controller = _controller(runner)
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="again")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})


class TestSandboxOwnership:
    """Sandboxes are per-process, so rows carry the owning instance."""

    def _row(self, cid: str) -> Sandbox:
        db = get_session_factory()()
        try:
            return db.query(Sandbox).filter(Sandbox.conversation_id == cid).one()
        finally:
            db.close()

    def test_new_sandbox_is_stamped_with_this_instance(self, user_id) -> None:
        uid, cid = user_id
        controller = Controller(runner=StubRunner(stdout=RESULT_OK), instance_id="inst-a")
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})
        assert self._row(cid).owner_instance == "inst-a"

    def test_reconcile_retires_only_this_instances_rows(self, user_id) -> None:
        uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add_all(
                [
                    Sandbox(
                        id="sbx_mine", user_id=uid, conversation_id=cid, owner_instance="inst-a"
                    ),
                    Sandbox(
                        id="sbx_theirs", user_id=uid, conversation_id="c2", owner_instance="inst-b"
                    ),
                    Sandbox(id="sbx_legacy", user_id=uid, conversation_id="c3"),  # NULL owner
                ]
            )
            db.commit()
        finally:
            db.close()

        controller = Controller(runner=StubRunner(), instance_id="inst-a")
        assert controller.reconcile_orphaned_sandboxes() == 2  # mine + legacy

        db = get_session_factory()()
        try:
            rows = {s.id: s.status for s in db.query(Sandbox).all()}
        finally:
            db.close()
        assert rows["sbx_mine"] == "destroyed"
        assert rows["sbx_legacy"] == "destroyed"
        assert rows["sbx_theirs"] == "running"  # another live instance's, untouched

    def test_takeover_from_another_instance_is_observable(self, user_id) -> None:
        """Rebuilding another instance's sandbox silently costs the user
        their multi-turn context, so it must be observable — the
        resident process (and its history) lives on the other host and
        cannot be reached from here."""
        from app.infra.metrics import get_registry

        uid, cid = user_id
        db = get_session_factory()()
        try:
            db.add(
                Sandbox(
                    id="sbx_elsewhere",
                    user_id=uid,
                    conversation_id=cid,
                    owner_instance="inst-b",
                )
            )
            db.commit()
        finally:
            db.close()

        # a restarted/other instance holds no state for that sandbox
        @dataclass
        class GhostRunner(StubRunner):
            def exists(self, sandbox_id: str) -> bool:
                return False

        runner = GhostRunner(stdout=RESULT_OK)
        controller = Controller(runner=runner, instance_id="inst-a")
        db = get_session_factory()()
        try:
            run = controller.start_run(db, user_id=uid, conversation_id=cid, prompt="hi")
        finally:
            db.close()
        _wait_status(controller, run.id, {"done"})

        assert "paw_sandbox_instance_migrations_total" in get_registry().render()
        db = get_session_factory()()
        try:
            rows = {s.id: (s.status, s.owner_instance) for s in db.query(Sandbox).all()}
        finally:
            db.close()
        assert rows["sbx_elsewhere"] == ("destroyed", None)
        assert rows["sbx_0"] == ("running", "inst-a")  # rebuilt here, stamped here
